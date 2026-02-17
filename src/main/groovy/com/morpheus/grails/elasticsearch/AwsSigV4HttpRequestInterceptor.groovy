package com.morpheus.grails.elasticsearch

import groovy.util.logging.Slf4j
import org.apache.http.HttpEntity
import org.apache.http.HttpEntityEnclosingRequest
import org.apache.http.HttpException
import org.apache.http.HttpRequest
import org.apache.http.HttpRequestInterceptor
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.protocol.HttpContext
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * HTTP Request Interceptor that signs requests using AWS Signature Version 4
 * for accessing AWS OpenSearch/Elasticsearch with IAM authentication (IRSA).
 *
 * Signing is implemented inline (not via Aws4Signer) so that header values are
 * canonicalized from the exact bytes Apache HttpClient will transmit. Using the
 * AWS SDK Aws4Signer caused 403s because it normalises header values
 * (e.g. trims spaces inside content-type) while AWS OpenSearch verifies the
 * signature against the un-normalised headers it actually receives.
 *
 * @author Morpheus Plugin
 */
@Slf4j
class AwsSigV4HttpRequestInterceptor implements HttpRequestInterceptor {

    // Headers that must never be included in the SigV4 canonical string.
    // - Host is handled separately by the SDK / reconstructed from the request line.
    // - x-elastic-* headers are injected by the ES client after signing.
    // - content-length is rewritten by Apache after the interceptor runs.
    private static final Set<String> EXCLUDED_HEADERS = [
        'host', 'content-length', 'transfer-encoding', 'connection'
    ] as Set<String>

    private static final Set<String> HEADERS_TO_SIGN = [
            'host', 'x-amz-date', 'x-amz-security-token'
    ]
    private static final DateTimeFormatter DATE_TIME_FMT =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
    private static final DateTimeFormatter DATE_FMT =
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC)

    private final AwsCredentialsProvider credentialsProvider
    private final String region
    private final String serviceName

    /**
     * Create an AWS SigV4 interceptor
     * @param credentialsProvider AWS credentials provider (supports IRSA, EC2 instance profiles, etc)
     * @param region AWS region (e.g., 'us-east-1')
     * @param serviceName AWS service name (default: 'es' for Elasticsearch/OpenSearch)
     */
    AwsSigV4HttpRequestInterceptor(AwsCredentialsProvider credentialsProvider, String region, String serviceName = 'es') {
        this.credentialsProvider = credentialsProvider
        this.region = region
        this.serviceName = serviceName
    }

    @Override
    void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        try {
            def requestLine = request.getRequestLine()
            def method      = requestLine.getMethod().toUpperCase()
            def uri         = URI.create(requestLine.getUri())
            def host        = request.getFirstHeader('Host')?.getValue()

            if (!host) {
                log.warn("No Host header found in request, skipping AWS signing")
                return
            }

            // ── 1. Buffer the body (if any) and replace with a repeatable entity ──
            byte[] bodyBytes = new byte[0]
            if (request instanceof HttpEntityEnclosingRequest) {
                HttpEntity entity = request.getEntity()
                if (entity) {
                    def baos = new ByteArrayOutputStream()
                    entity.writeTo(baos)
                    bodyBytes = baos.toByteArray()
                    def replacement = new ByteArrayEntity(bodyBytes)
                    replacement.setContentType(entity.getContentType())
                    request.setEntity(replacement)
                }
            }

            // ── 2. Determine the timestamp ──
            def now       = ZonedDateTime.now(ZoneOffset.UTC)
            def dateTime  = DATE_TIME_FMT.format(now)   // e.g. 20260224T081058Z
            def dateStamp = DATE_FMT.format(now)         // e.g. 20260224

            // Inject x-amz-date (overwrite any stale value)
            request.removeHeaders('x-amz-date')
            request.addHeader('x-amz-date', dateTime)

            // Inject x-amz-security-token for temporary credentials (IRSA / STS)
            def credentials = credentialsProvider.resolveCredentials()
            if (credentials instanceof AwsSessionCredentials) {
                request.removeHeaders('x-amz-security-token')
                request.addHeader('x-amz-security-token', credentials.sessionToken())
            }

            // ── 3. Build the canonical headers map from the ACTUAL request headers ──
            // We use a TreeMap (sorted by key) as required by SigV4.
            // Exclude headers that are not stable at signing time.
            def canonicalHeadersMap = new TreeMap<String, String>()
            // AWS strips default ports when canonicalizing the host header:
            // port 443 is implicit for HTTPS, port 80 for HTTP.
            def canonicalHost = host.replaceAll(':443$', '').replaceAll(':80$', '')
            canonicalHeadersMap['host'] = canonicalHost

            request.getAllHeaders().each { header ->
                def lname = header.name.toLowerCase(Locale.US)
                // Skip 'host' — already set above with port stripped per SigV4 spec
                if (lname == 'host') return
                if (!lname.startsWith('x-elastic-') && HEADERS_TO_SIGN.contains(lname)) {
                    // SigV4 spec: trim leading/trailing whitespace
                    def existing = canonicalHeadersMap.get(lname)
                    def trimmed  = header.value?.trim() ?: ''
                    canonicalHeadersMap.put(lname, existing ? "${existing},${trimmed}" : trimmed)
                }
            }

            def canonicalHeadersStr = canonicalHeadersMap.collect { k, v -> "${k}:${v}" }.join('\n') + '\n'
            def signedHeadersStr    = canonicalHeadersMap.keySet().join(';')

            // ── 4. Hash the body ──
            def bodyHash = sha256Hex(bodyBytes)

            // ── 5. Canonical query string ──
            def canonicalQueryString = buildCanonicalQueryString(uri.rawQuery)

            // ── 6. Canonical request ──
            def canonicalPath    = uri.rawPath ?: '/'
            def canonicalRequest = [
                method,
                canonicalPath,
                canonicalQueryString,
                canonicalHeadersStr,
                signedHeadersStr,
                bodyHash
            ].join('\n')

            // ── 7. String to sign ──
            def credentialScope  = "${dateStamp}/${region}/${serviceName}/aws4_request"
            def stringToSign     = "AWS4-HMAC-SHA256\n${dateTime}\n${credentialScope}\n${sha256Hex(canonicalRequest.getBytes(StandardCharsets.UTF_8))}"

            // ── 8. Signing key ──
            def secretKey    = credentials.secretAccessKey()
            def signingKey   = hmacSha256(
                hmacSha256(
                    hmacSha256(
                        hmacSha256(
                            "AWS4${secretKey}".getBytes(StandardCharsets.UTF_8),
                            dateStamp
                        ),
                        region
                    ),
                    serviceName
                ),
                "aws4_request"
            )

            // ── 9. Signature ──
            def signature = hmacSha256(signingKey, stringToSign).encodeHex().toString()

            // ── 10. Authorization header ──
            def authorization = "AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId()}/${credentialScope}, SignedHeaders=${signedHeadersStr}, Signature=${signature}"
            request.removeHeaders('Authorization')
            request.addHeader('Authorization', authorization)

            if (log.isDebugEnabled()) {
                def headerDump = request.getAllHeaders().collect { h -> "  ${h.name}: ${h.value}" }.join('\n')
                log.debug("=== AWS SigV4 Debug ===")
                log.debug("Canonical Request:\n{}", canonicalRequest)
                log.debug("String to Sign:\n{}", stringToSign)
                log.debug("Signed Headers: {}", signedHeadersStr)
                log.debug("Outgoing Headers for ${method} ${uri.path}:\n${headerDump}")
                log.debug("======================")
            }

        } catch (Exception e) {
            log.error("Failed to sign request with AWS SigV4: ${e.message}", e)
            throw new HttpException("AWS SigV4 signing failed: ${e.message}")
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String buildCanonicalQueryString(String rawQuery) {
        if (!rawQuery) return ''
        def params = rawQuery.split('&').collect { param ->
            def idx   = param.indexOf('=')
            def key   = idx >= 0 ? param.substring(0, idx) : param
            def value = idx >= 0 ? param.substring(idx + 1) : ''
            // URI-encode key and value per SigV4 spec (percent-encode everything except unreserved chars)
            [uriEncode(URLDecoder.decode(key, 'UTF-8')), uriEncode(URLDecoder.decode(value, 'UTF-8'))]
        }.sort { a, b -> a[0] <=> b[0] ?: a[1] <=> b[1] }
        return params.collect { "${it[0]}=${it[1]}" }.join('&')
    }

    private static String uriEncode(String value) {
        // SigV4 unreserved characters: A-Z a-z 0-9 - _ . ~
        // URLEncoder encodes everything except A-Z a-z 0-9 - _ . *  and turns spaces into +
        // We then: replace * with %2A, replace + with %20, and un-encode ~ (%7E)
        URLEncoder.encode(value, 'UTF-8')
            .replace('+', '%20')
            .replace('*', '%2A')
            .replace('%7E', '~')
    }

    private static String sha256Hex(byte[] data) {
        MessageDigest.getInstance('SHA-256').digest(data).encodeHex().toString()
    }

    private static String sha256Hex(String data) {
        sha256Hex(data.getBytes(StandardCharsets.UTF_8))
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        def mac = Mac.getInstance('HmacSHA256')
        mac.init(new SecretKeySpec(key, 'HmacSHA256'))
        mac.doFinal(data.getBytes(StandardCharsets.UTF_8))
    }
}
