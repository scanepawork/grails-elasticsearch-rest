package com.morpheus.grails.elasticsearch

import groovy.json.JsonOutput


/**
* Simplified ElasticSearch Query Builder for performing Query Operations
*
* @author Brian Wheeler
*/
public class ElasticQueryBuilder {

	//query calls
	static rootQuery() {
		return new RootQuery()
	}

	static rootQuery(String index, String type) {
		return new RootQuery(index, type)
	}

	static countQuery() {
		return new CountQuery()
	}

	static countQuery(String index, String type) {
		return new CountQuery(index, type)
	}

	static boolQuery() {
		return new BoolQuery()
	}

	static termQuery(String field, Object value) {
		return new TermQuery(field, value)
	}

	static termsQuery(String field, List values) {
		return new TermsQuery(field, values)
	}

	static termsQuery(String field, Object[] values) {
		return new TermsQuery(field, Arrays.asList(values))
	}

	static rangeQuery(String field) {
		return new RangeQuery(field)
	}

	static typeQuery(String type) {
		return new TypeQuery(type)
	}

	static queryStringQuery(String queryString) {
		return new QueryStringQuery(queryString)
	}

	static simpleQueryStringQuery(String queryString) {
		return new SimpleQueryStringQuery(queryString)
	}

	static simpleQueryStringQuery(String queryString, String defaultField) {
		return new SimpleQueryStringQuery(queryString, defaultField)
	}

	static queryStringQuery(String queryString, String defaultField) {
		return new QueryStringQuery(queryString, defaultField)
	}

	static idsQuery() {
		return new IdsQuery()
	}

	static nestedQuery(String path, Object query, String scoreMode) {
		return new NestedQuery(path, query, scoreMode)
	}

	static prefixQuery(String field, Object value) {
		return new PrefixQuery(field, value)
	}

	static matchQuery(String field, Object value) {
		return new MatchQuery(field, value)
	}

	static matchAllQuery() {
		return new MatchAllQuery()
	}

	static matchPhrasePrefixQuery(String field, Object value) {
		return new MatchPhrasePrefixQuery(field, value)
	}

	static existsQuery(String field) {
		return new ExistsQuery(field)
	}

	static prepareMultiSearch() {
		return new MultiSearch()
	}

	static prepareBulk() {
		return new BulkRequest()
	}

	//

	//internal classes
	static class BaseQuery {

		Map body = [:]
		Map queryTarget

		String toString() {
			return JsonOutput.toJson(body)
		}

		def setBaseKeyValue(String key, Object value) {
			body[key] = value
			return this
		}

		def setKeyValue(String key, Object value) {
			queryTarget[key] = value
			return this	
		}

		def getQuery() {
			//println("getQuery: ${queryTarget}")
			return queryTarget
		}

		def getMultiQuery() {
			return body
		}

	}

	//root query - use it like the elastic client prepare search
	static class RootQuery extends BaseQuery {

		String index
		String type

		public RootQuery() {
			body = [from:0, size:10, query:[:]]
			queryTarget = body.query
		}

		public RootQuery(String index, String type) {
			this.index = index
			this.type = type
			body = [from:0, size:10, query:[:]]
			queryTarget = body.query
		}

		def setSize(Integer size) {
			body.size = size
			return this
		}

		def setFrom(Integer from) {
			body.from = from
			return this
		}
		
		def prepareDelete() {
			body.remove('from')
			body.remove('size')
			return this
		}

		def addSort(String field, String order) {
			body.sort = body.sort ?: []
			def newSort = [:]
			newSort[field] = [order:order?.toLowerCase()]
			body.sort << newSort
			return this
		}

		def addSort(String field, String order, String unmappedType, String missing) {
			body.sort = body.sort ?: []
			def newSort = [:]
			newSort[field] = [order:order?.toLowerCase()]
			if(unmappedType)
				newSort[field].unmapped_type = unmappedType
			if(missing)
				newSort[field].missing = missing
			body.sort << newSort
			return this
		}

		def addIndexBoost(String index, Float boost) {
			body.indices_boost = body.indices_boost ?: []
			def newBoost = [:]
			newBoost[index] = boost
			body.indices_boost << newBoost
			return this  
		}

		def setQuery(query) {
			body.query = query.body
			queryTarget = body.query
			return this
		}

		def addAggregation(Object aggregation) {
			body.aggs = body.aggs ?: [:]
			body.aggs << aggregation.body
			return this
		}

		def addAggregation(String name, Object aggregation) {
			body.aggs = body.aggs ?: [:]
			body.aggs[name] = aggregation.body
			return this
		}

		def getMultiQuery() {
			return body
		}

	}

	static class CountQuery extends BaseQuery {

		String index
		String type

		public CountQuery() {
			body = [from:0, size:0, query:[:]]
			queryTarget = body.query
		}

		public CountQuery(String index, String type) {
			this.index = index
			this.type = type
			body = [from:0, size:0, query:[:]]
			queryTarget = body.query
		}

		def setSize(Integer size) {
			body.size = size
			return this
		}

		def setFrom(Integer from) {
			body.from = from
			return this
		}

		def addSort(String field, String order) {
			body.sort = body.sort ?: []
			def newSort = [:]
			newSort[field] = [order:order?.toLowerCase()]
			body.sort << newSort
			return this
		}

		def setQuery(query) {
			body.query = query.body
			queryTarget = body.query
			return this
		}

		def getMultiQuery() {
			return body
		}

	}

	static class TypeQuery extends BaseQuery {
		public TypeQuery(String type) {
			body.type = [:]
			body.type.value = type
			queryTarget = body.type
		}
	}

	static class ExistsQuery extends BaseQuery {
		public ExistsQuery(String field) {
			body.exists = [field:field]
			queryTarget = body.exists
		}
	}

	static class TermQuery extends BaseQuery {
		public TermQuery(String field, Object value) {
			body.term = [:]
			body.term[field] = value
			queryTarget = body.term
		}
	}

	static class TermsQuery extends BaseQuery {
		public TermsQuery(String field, List values) {
			body.terms = [:]
			body.terms[field] = values
			queryTarget = body.terms
		}
	}

	static class PrefixQuery extends BaseQuery {
		public PrefixQuery(String field, Object value) {
			body.prefix = [:]
			body.prefix[field] = value
			queryTarget = body.prefix
		}
	}

	static class MatchQuery extends BaseQuery {
		public MatchQuery(String field, Object value) {
			body.match = [:]
			body.match[field] = value
			queryTarget = body.match
		}
	}

	static class MatchAllQuery extends BaseQuery {
		public MatchAllQuery() {
			body.match_all = [:]
			queryTarget = body.match_all
		}
	}

	static class MatchPhrasePrefixQuery extends BaseQuery {
		public MatchPhrasePrefixQuery(String field, Object value) {
			body.match_phrase_prefix = [:]
			body.match_phrase_prefix[field] = value
			queryTarget = body.match_phrase_prefix
		}
	}

	static class IdsQuery extends BaseQuery {
		public IdsQuery() {
			body.ids = [values:[]]
			queryTarget = body.ids
		}

		def setTypes(String type) {
			queryTarget.type = type
			return this
		}

		def addIds(List idList) {
			queryTarget.values += idList
			return this
		}

		def addIds(Object object) {
			queryTarget.values << object
			return this
		}
	}

	static class NestedQuery extends BaseQuery {
		public NestedQuery(String path, Object query, String scoreMode) {
			body.nested = [
				path:path,
				score_mode:scoreMode,
				query:query.body
			]
			queryTarget = body.nested
		}
	}

	static class QueryStringQuery extends BaseQuery {

		public QueryStringQuery(String queryString) {
			body.query_string = [:]
			body.query_string.query = queryString
			queryTarget = body.query_string
		}

		public QueryStringQuery(String queryString, String defaultField) {
			body.query_string = [:]
			body.query_string.query = queryString
			body.query_string.default_field = defaultField
			queryTarget = body.query_string
		}
		
		def defaultOperator(String value) {
			body.query_string.default_operator = value
			return this
		}
		
		def lenient(Boolean value) {
			body.query_string.lenient = value
			return this
		}
		
		def analyzer(String value) {
			body.query_string.analyzer = value
			return this
		}
		
		def autoGeneratePhraseQueries(Boolean value) {
			//body.query_string.auto_generate_phrase_queries = value
			return this
		}

		def fields(Collection values) {
			body.query_string.fields = values
			return this
		}
		
		def type(String value) {
			body.query_string.type = value
			return this
		}

		def rewrite(String value) {
			body.query_string.rewrite = value
			return this
		}

		def phraseSlop(Integer value) {
			body.query_string.phrase_slop = value
			return this
		}
		
		def analyzeWildcard(Boolean value) {
			body.query_string.analyze_wildcard = value
			return this
		}
	}

	static class SimpleQueryStringQuery extends BaseQuery {

		public SimpleQueryStringQuery(String queryString) {
			body.simple_query_string = [:]
			body.simple_query_string.query = queryString
			queryTarget = body.simple_query_string
		}

		public SimpleQueryStringQuery(String queryString, String defaultField) {
			body.simple_query_string = [:]
			body.simple_query_string.query = queryString
			body.simple_query_string.default_field = defaultField
			queryTarget = body.simple_query_string
		}
		
		def defaultOperator(String value) {
			body.simple_query_string.default_operator = value
			return this
		}
		
		def lenient(Boolean value) {
			body.simple_query_string.lenient = value
			return this
		}
		
		def analyzer(String value) {
			body.simple_query_string.analyzer = value
			return this
		}
		
		def autoGenerateSynonymsPhraseQueries(Boolean value) {
			body.simple_query_string.auto_generate_synonyms_phrase_query = value
			return this
		}

		def fields(Collection values) {
			body.simple_query_string.fields = values
			return this
		}
		
		
		
		
		
	}

	static class BoolQuery extends BaseQuery {

		public BoolQuery() {
			body.bool = [:]
			queryTarget = body.bool
		}

		def must(query) {
			queryTarget.must = addFlexArrayMapValue(queryTarget.must, query.body)
			return this
		}

		def should(query) {
			queryTarget.should = addFlexArrayMapValue(queryTarget.should, query.body)
			return this
		}

		def filter(query) {
			queryTarget.filter = addFlexArrayMapValue(queryTarget.filter, query.body)
			return this	
		}

		def mustNot(query) {
			queryTarget.must_not = addFlexArrayMapValue(queryTarget.must_not, query.body)
			return this
		}

	}

	static class RangeQuery extends BaseQuery {
		
		String field
		
		public RangeQuery(String field) {
			this.field = field
			body.range = [:]
			body.range[field] = [:]
			queryTarget = body.range[field]
		}

		def from(Object value) {
			queryTarget.gte = value
			return this
		}

		def to(Object value) {
			queryTarget.lte = value
			return this
		}

		def gte(Object value) {
			queryTarget.gte = value
			return this
		}

		def lte(Object value) {
			queryTarget.lte = value
			return this
		}

		def gt(Object value) {
			queryTarget.gt = value
			return this
		}

		def lt(Object value) {
			queryTarget.lt = value
			return this
		}

		def boost(Double value) {
			queryTarget.boost = value
			return this
		}

	}

	static class MultiSearch {
		
		List searchItems

		public MultiSearch() {
			searchItems = []
		}

		def add(String index, String type, String id, Object item) {
			def headerRow = ['index':index ?: '_all']
			if(type)
				headerRow.type = type
			if(id)
				headerRow.id = id
			searchItems << headerRow
			searchItems << item.getMultiQuery()
		}

		def hasContent() {
			return searchItems.size() > 0
		}

		def getRequestBody() {
			def rtn = searchItems.collect{ multiItem ->
				return JsonOutput.toJson(multiItem)
			}?.join('\n')
			return rtn
		}

	}

	static class BulkRequest {

		List bulkItems

		public BulkRequest() {
			bulkItems = []
		}

		def add(String action, String index, String type, Object id, Map document) {
			def actionRow = [:]
			actionRow[action] = ['_index':index, '_id':id]
			bulkItems << actionRow
			if(document)
				bulkItems << document
		}

		def hasContent() {
			return bulkItems?.size() > 0
		}

		def getRequestBody() {
			def rtn = bulkItems.collect{ bulkItem ->
				return JsonOutput.toJson(bulkItem)
			}?.join('\n') + '\n'
			return rtn
		}

	}

	//internal stuff
	static addFlexArrayMapValue(Object target, Map addon) {
		def rtn
		if(target) {
			if(target instanceof List) {
				target << addon
				rtn = target
			} else if(target instanceof Map) {
				rtn = []
				rtn << target
				rtn << addon
			}
		} else {
			rtn = addon
		}
		return rtn
	}

}
