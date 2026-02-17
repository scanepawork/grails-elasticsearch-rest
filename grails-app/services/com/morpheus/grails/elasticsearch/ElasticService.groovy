package com.morpheus.grails.elasticsearch

import org.apache.commons.beanutils.PropertyUtils
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.config.Registry
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.ConnectTimeoutException
import org.apache.http.conn.ManagedHttpClientConnection
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContextBuilder
import org.apache.http.conn.ssl.TrustStrategy
import org.apache.http.conn.ssl.X509HostnameVerifier
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.conn.BasicHttpClientConnectionManager
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager
import org.apache.http.io.HttpMessageWriterFactory
import org.apache.http.protocol.HttpContext
import org.elasticsearch.client.Request
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.ResponseListener
import org.elasticsearch.client.Response
import org.apache.http.*
import org.apache.http.entity.*
import org.apache.http.nio.entity.*
import org.apache.http.client.config.*
import com.morpheus.grails.elasticsearch.ElasticQueryBuilder
import com.morpheus.grails.elasticsearch.ElasticQueryBuilder.BulkRequest
import com.morpheus.grails.elasticsearch.ElasticQueryBuilder.MultiSearch
import com.morpheus.grails.elasticsearch.AwsSigV4HttpRequestInterceptor
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider

import org.grails.core.artefact.DomainClassArtefactHandler
import grails.core.GrailsApplication
import grails.core.GrailsClass
import grails.core.GrailsDomainClass

import javax.net.ssl.SNIServerName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLException
import javax.net.ssl.SSLParameters
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import java.lang.reflect.InvocationTargetException
import java.security.cert.X509Certificate
import grails.core.GrailsApplication
import groovy.util.logging.Slf4j
import groovy.json.JsonOutput

/**
* Wrapper Service around the ElasticSearch REST Client for performing common REST Operations
* Against the ElasticSearch endpoints
*
* @author Brian Wheeler
*/
@Slf4j
class ElasticService {

	GrailsApplication grailsApplication

	private internalClient

	static requestTimeout = 1000i * 20i
	static requestSocketTimeout = 1000i * 30i
	static requestRetryTimeout = 1000i * 60i



	void shutdownRestClient() {
		if(internalClient) {

			internalClient = null
		}
	}

	def getRestClient() {
		if(internalClient == null) {
			def configHosts = grailsApplication.config.getProperty('elasticSearch.client.hosts',List,null)
			def protocol = grailsApplication.config.getProperty('elasticSearch.protocol',String,'http')
			def username = grailsApplication.config.getProperty('elasticSearch.user',String,null)
			def password = grailsApplication.config.getProperty('elasticSearch.password', String,null)
			
			// AWS configuration
			def awsEnabled = grailsApplication.config.getProperty('elasticSearch.aws.enabled', Boolean, false)
			def awsRegion = grailsApplication.config.getProperty('elasticSearch.aws.region', String, null)
			def awsServiceName = grailsApplication.config.getProperty('elasticSearch.aws.serviceName', String, 'es')
			
			//host
			if(configHosts == null || (!configHosts instanceof List)) {
				configHosts = [host:'127.0.0.1', port:9200]
			}
			//creds
			CredentialsProvider clientCredentials
			if(username) {
				 clientCredentials = new BasicCredentialsProvider()
				clientCredentials.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials(username, password))
			}
			
			// AWS Credentials Provider (for IRSA, EC2 instance profiles, etc.)
			AwsCredentialsProvider awsCredentialsProvider = null
			if (awsEnabled && awsRegion) {
				log.info("AWS authentication enabled for OpenSearch with region: ${awsRegion}, service: ${awsServiceName}")
				try {
					// Use DefaultCredentialsProvider which supports IRSA, EC2 instance profiles, 
					// environment variables, and more
					awsCredentialsProvider = DefaultCredentialsProvider.create()
					log.debug("AWS credentials provider initialized successfully")
				} catch (Exception e) {
					log.error("Failed to initialize AWS credentials provider: ${e.message}", e)
					throw new RuntimeException("AWS authentication is enabled but credentials provider failed to initialize", e)
				}
			}
			
			//ssl
			SSLContext sslcontext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
				@Override
				public boolean isTrusted(X509Certificate[] chain, String authType) throws java.security.cert.CertificateException {
					return true
				}
			}).build()
			SSLConnectionSocketFactory sslConnectionFactory = new SSLConnectionSocketFactory(sslcontext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER) {
				@Override
				protected void prepareSocket(SSLSocket socket) {
					if(opts.ignoreSSL) {
						PropertyUtils.setProperty(socket, "host", null);
						List<SNIServerName> serverNames  = Collections.<SNIServerName> emptyList();
						SSLParameters sslParams = socket.getSSLParameters();
						sslParams.setServerNames(serverNames);
						socket.setSSLParameters(sslParams);
					}
				}
				@Override
				public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress, InetSocketAddress localAddress, HttpContext context) throws IOException, ConnectTimeoutException {
					if(socket instanceof SSLSocket) {
						try {
							socket.setEnabledProtocols(['SSLv3', 'TLSv1', 'TLSv1.1', 'TLSv1.2'] as String[])
							SSLSocket sslSocket = (SSLSocket)socket

							log.debug "hostname: ${host?.getHostName()}"
							PropertyUtils.setProperty(socket, "host", host.getHostName());
						} catch (NoSuchMethodException ex) {}
						catch (IllegalAccessException ex) {}
						catch (InvocationTargetException ex) {}
						catch (Exception ex) {
							log.error "We have an unhandled exception when attempting to connect to ${host} ignoring SSL errors", ex
						}
					}
					return super.connectSocket(requestTimeout, socket, host, remoteAddress, localAddress, context)
				}
			}
			Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory> create()
				.register("https", sslConnectionFactory)
				.register("http", PlainConnectionSocketFactory.INSTANCE)
				.build();
			//configure
			def clientHosts = []
			configHosts.each { configHost ->
				log.debug('elastic host: ' + configHost.host)
				log.debug('elastic port: ' + configHost.port)
				clientHosts << new HttpHost(configHost.host, configHost.port ?: 9200, protocol)
			}
			//set it up
			def clientBuilder = RestClient.builder(clientHosts as HttpHost[]).setRequestConfigCallback(
				new RestClientBuilder.RequestConfigCallback() {
	      	@Override
	        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {

	        	return requestConfigBuilder.setConnectTimeout(requestTimeout).setSocketTimeout(60000)
	        }

      	}
      ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
				@Override
				public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
					// Basic authentication (legacy support)
					if(clientCredentials) {
						httpClientBuilder.setDefaultCredentialsProvider( clientCredentials as CredentialsProvider)
					}
					
					// AWS SigV4 authentication (IRSA support)
					if (awsEnabled && awsCredentialsProvider && awsRegion) {
						log.debug("Adding AWS SigV4 request interceptor")
						httpClientBuilder.addInterceptorLast(
							new AwsSigV4HttpRequestInterceptor(awsCredentialsProvider, awsRegion, awsServiceName)
						)
					}
					
					httpClientBuilder.setHostnameVerifier(new X509HostnameVerifier() {
						@Override
						void verify(String host, SSLSocket ssl) throws IOException {}

						@Override
						void verify(String host, X509Certificate cert) throws SSLException {}

						@Override
						void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {}

						@Override
						boolean verify(String s, SSLSession sslSession) {
							return true
						}
					})
					return httpClientBuilder.setSSLContext(sslcontext)
				}
			})//removed in 7.0 - .setMaxRetryTimeoutMillis(requestRetryTimeout)
			internalClient = clientBuilder.build()
		}
		return internalClient
	}

	def executeMulti(MultiSearch searchRequest, Map opts) {
		def rtn = [success:false]
		def multiBody
		try {
			def queryUrl = '/_msearch'
			def restClient = getRestClient()
			def queryParams = [:]
			multiBody = searchRequest.getRequestBody()
			HttpEntity queryBody = new NStringEntity(multiBody, ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('GET', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
    	def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.data = data
			rtn.responses = rtn.data.responses
			rtn.success = true
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing multi request: {}", e.message, e)
			log.debug("multi request error body: {}", multiBody)
		}
		return rtn
	}

	def executeBulk(BulkRequest bulkRequest, Map opts = [:]) {
		def rtn = [success:false]
		def bulkBody
		try {
			def queryUrl = '/_bulk'
			def restClient = getRestClient()
			def queryParams = [:]
			bulkBody = bulkRequest.getRequestBody()
			HttpEntity queryBody = new NStringEntity(bulkBody, ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('POST', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			if(opts.async == true) {
				restClient.performRequestAsync(request, new ElasticResponseListener())
				rtn.success = true
				rtn.took = 0
			} else {
				def response = restClient.performRequest(request)
				def content = response.getEntity().getContent().text
				def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
				rtn.errors = data.errors
				rtn.success = data.errors == false
				rtn.took = data.took ?: 0
				rtn.items = data.items
			}
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing bulk request: ${e.message}", e)
			log.debug("Error For Bulk Request Body: {}", bulkBody)
		}
		return rtn
	}

	def createDocument(String index, String path, Map document, Map opts = [:]) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.refresh != null)
				queryParams.put('refresh', opts.refresh)
			HttpEntity queryBody = new NStringEntity(JsonOutput.toJson(document), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('POST', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = data.result == "created"
			rtn.document = document
			rtn.result = data.result
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error creating document: ${e}", e)
		}
		return rtn
	}

	def saveDocument(String index, String path, Map document, String docId = null, Map opts = [:]) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			docId = docId ?: UUID.randomUUID().toString()
			queryUrl = queryUrl + '/' + docId
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.refresh != null)
				queryParams.put('refresh', opts.refresh)
			HttpEntity queryBody = new NStringEntity(JsonOutput.toJson(document), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('PUT', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.id = docId
			rtn.success = data.created == true
			rtn.document = document
			rtn.result = data.result
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error saving document: ${e}", e)
		}
		return rtn
	}

	def updateDocument(String index, String path, Map document, String docId, Map opts = [:]) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			queryUrl = queryUrl + '/' + docId + '/_update'
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.refresh != null)
				queryParams.put('refresh', opts.refresh)
			def body = [doc:document]
			HttpEntity queryBody = new NStringEntity(JsonOutput.toJson(body), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('POST', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.id = docId
			rtn.success = data.created == true
			rtn.document = document
			rtn.result = data.result
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error saving document: ${e}", e)
		}
		return rtn
	}

	def createIndex(String index, Map config) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			def restClient = getRestClient()
			def queryParams = [:]
			HttpEntity queryBody = new NStringEntity(JsonOutput.toJson(config), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('PUT', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error creating index: ${e}", e)
		}
		return rtn
	}

	def deleteIndex(String index) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			def restClient = getRestClient()
			//build the request
			def request = new Request('DELETE', queryUrl)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity()?.getContent()?.text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error deleting index: ${e}", e)
		}
		return rtn
	}

	def executeSearch(Object query, Map opts) {
		def rtn = [success:false]
		try {
			def index = query.index
			def type = query.type
			def queryType = opts.queryType ?: 'query_then_fetch' //'dfs_query_then_fetch'
			def queryUrl = '/' + index
			//set the type - this should always be empty or _doc
			if(type)
				queryUrl = queryUrl + '/' + type
			queryUrl = queryUrl + '/_search'
			//add params
			//get the rest client
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.ignoreUnavailable == true)
				queryParams.put('ignore_unavailable', 'true')
			if(opts.allowNoIndices == true)
				queryParams.put('allow_no_indices', 'true')
			queryParams.put('explain', '')
			//println("executeSearch - index:${index} queryUrl:${queryUrl} query:${query}")
			HttpEntity queryBody = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('GET', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			//println("data: ${data}")
			rtn.success = true
			rtn.took = data.took ?: 0
			rtn.timedOut = data.timedOut ?: false
			rtn.shards = data['_shards']
			rtn.hits = data.hits
		} catch(e) {
			if(!e.message?.contains('no such index') || !opts.allowNoIndices) {
				log.error("error executing search: ${e}", e)
			}
		}
		return rtn
	}

	def executeDeleteQuery(Object query, Map opts) {
		def rtn = [success:false]
		try {
			def index = query.index
			def path = query.type
			def queryUrl = '/' + index
			queryUrl = queryUrl + '/_delete_by_query'
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.ignoreUnavailable)
				queryParams.put('ignore_unavailable', 'true')
			query.prepareDelete()
			HttpEntity queryBody = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('POST', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.data = data
			rtn.success = true
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing delete query: ${e}", e)
		}
		return rtn
	}

	def executeCount(Object query, Map opts) {
		def rtn = [success:false]
		try {
			def index = query.index
			def type = query.type
			def queryType = opts.queryType ?: 'query_then_fetch' //'dfs_query_then_fetch'
			def queryUrl = '/' + index
			if(type)
				queryUrl = queryUrl + '/' + type
			queryUrl = queryUrl + '/_count'
			def restClient = getRestClient()
			def queryParams = [:]
			if(opts.ignoreUnavailable)
				queryParams.put('ignore_unavailable', 'true')
			HttpEntity queryBody = new NStringEntity(query.toString(), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('GET', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.took = data.took ?: 0
			rtn.timedOut = data.timedOut ?: false
			rtn.shards = data['_shards']
			rtn.hits = data.hits
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			if(!e.message.contains('no such index')) {
				log.error("error executing count: ${e}", e)
			}
			
		}
		return rtn
	}

	def executeAggregation(Object aggregation, Map opts) {
		def rtn = [success:false]
		try {
			def index = aggregation.index
			def type = aggregation.type
			def queryType = opts.queryType ?: 'query_then_fetch' //'dfs_query_then_fetch'
			def queryUrl = '/' + index
			if(type)
				queryUrl = queryUrl + '/' + type
			queryUrl = queryUrl + '/_search'
			def restClient = getRestClient()
			def queryParams = [:]
			queryParams.put('size', '0')
			if(opts.ignoreUnavailable)
				queryParams.put('ignore_unavailable', 'true')
			HttpEntity queryBody = new NStringEntity(aggregation.toString(), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('GET', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.took = data.took ?: 0
			rtn.timedOut = data.timedOut ?: false
			rtn.shards = data['_shards']
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing aggregation: ${e}", e)
		}
		return rtn
	}

	def executePut(String index, String path, Map document) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			def restClient = getRestClient()
			def queryParams = [:]
			HttpEntity queryBody = new NStringEntity(JsonOutput.toJson(document), ContentType.APPLICATION_JSON)
			//build the request
			def request = new Request('PUT', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			request.setEntity(queryBody)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing put: ${e}", e)
		}
		return rtn
	}

	def executePost(String index, String path, Map document,Map queryParams=[:]) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			def restClient = getRestClient()
			HttpEntity queryBody = null
			if(document) {
				queryBody = new NStringEntity(JsonOutput.toJson(document), ContentType.APPLICATION_JSON)
			}
			//build the request
			def request = new Request('POST', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			if(queryBody) {
				request.setEntity(queryBody)
			}
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing post: ${e}", e)
		}
		return rtn
	}


	def executeGet(String index, String path, Map document) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			def restClient = getRestClient()
			def queryParams = [:]
			
			//build the request
			def request = new Request('GET', queryUrl)
			if(queryParams)
				request.addParameters(queryParams as Map<String, String>)
			if(document)
				request.setEntity(NStringEntity(JsonOutput.toJson(document), ContentType.APPLICATION_JSON))
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing get: ${e}", e)
		}
		return rtn
	}

	/**
	* Performs a Delete operation on an index based on path and id
	* @param String index - The name of the Index you wish to perform the operation on
	* @param String path - Document Path
	* @param String id - Document id you wish to delete
	*/
	def executeDelete(String index, String path, String id) {
		def rtn = [success:false]
		try {
			def queryUrl = '/' + index
			if(path)
				queryUrl = queryUrl + '/' + path
			if(id)
				queryUrl = queryUrl + '/' + id
			def restClient = getRestClient()
			//build the request
			def request = new Request('DELETE', queryUrl)
			//execute
			def response = restClient.performRequest(request)
			def content = response.getEntity().getContent().text
			def data = content ? new groovy.json.JsonSlurper().parseText(content) : [:]
			rtn.success = true
			rtn.data = data
		} catch(RuntimeException re) {
			log.warn("ElasticSearch RunTimeException Occurred. Throwing away Client and Creating new... {}",re.message,re)
			shutdownRestClient()
		} catch(e) {
			log.error("error executing delete: ${e}", e)
		}
		return rtn
	}

	/**
	* Executes a Merge operation on the specified Index
	* @param String index - The name of the Index you wish to merge
	* @param Boolean flush - Whether or not you want to also flush this merge request at the same time
	* @param Integer maxSegments - The maximum number of segments you wish to merge into (default 1)
	*/
	def executeIndexMerge(String index, Boolean flush = false, Integer maxSegments = 1, Map opts = [:]) {
		def rtn
		def indexConfig = [
			max_num_segments:maxSegments?.toString(),
			flush:flush?.toString()
		]
		rtn = executePost(index, '_forcemerge',null, indexConfig)
		return rtn
	}

	static class ElasticResponseListener implements ResponseListener {

		@Override
		public void onSuccess(Response response) {
			//nothing?
		}

		@Override
		public void onFailure(Exception e) {
			log.warn("error on async elastic request: ${e}", e)
		}

	}

}
