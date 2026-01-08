package com.morpheus.grails.elasticsearch

import com.morpheus.grails.elasticsearch.ElasticQueryBuilder
import grails.core.GrailsApplication
import grails.core.GrailsClass
import grails.core.GrailsDomainClass
import grails.gorm.transactions.Transactional
import grails.util.Holders
import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.grails.core.artefact.DomainClassArtefactHandler
import org.grails.datastore.mapping.core.Datastore
import org.grails.datastore.mapping.engine.event.*
import org.grails.datastore.mapping.model.PersistentEntity
import org.grails.orm.hibernate.HibernateDatastore
import org.springframework.context.ApplicationEvent
import org.springframework.transaction.annotation.Propagation
import org.springframework.util.Assert
import groovy.util.logging.Slf4j
import java.util.regex.Matcher

/**
* Grails Admin Service performs functions related to indexing Hibernate based Domain Classes
*
* @author Brian Wheeler
*/
@Slf4j
public class ElasticAdminService {

	static searchablePropertyName = 'searchable'

	GrailsApplication grailsApplication

	ElasticService elasticService

	static maxRetries = 10
	static sleepInterval = 100
	static elasticConfig
	static defaultLogRetainment = 7
	static defaultCheckRetainment = 30
	static defaultStatsRetainment = 30

	static List<DomainMap> domainList //list of the grails domain classes
	static Map<String, Map> indexMap = [:]
	static Map<String, String> indexAliases = [:]
	static Map<String, String> indexVersions = [:]

	//queueing structures
	def indexRequests = [:]
	def deleteRequests = [:]
	def inProgressRequests = []

	def indexMutex = new Object()
	def startupMutex = new Object()

	static {
		elasticConfig = Holders.getConfig().getProperty('elasticSearch', Map, [:])
		//println("elasticConfig: ${elasticConfig}")
	}

	static indexToVersion(String indexName) {
		return indexAliases[indexName] ?: indexName
	}

	static versionToIndex(String indexVersion) {
		return indexVersions[indexVersion] ?: indexVersion
	}

	def applicationStartup(doIndex=false) {
		//auto wire up stuff
		installDomainMappings()
		installDomainMethods()
		//bulk indexing check
		def doIndexing = doIndex ?: elasticConfig.bulkIndexOnStartup == true
		
		//bulkIndex
		if(doIndexing == true) {
			log.info("elastic - indexing domains")
			indexDomains()
			log.info("elastic - finished indexing domains")
		}
		//add listeners
		def datastore = grailsApplication.mainContext.getBean(HibernateDatastore)
		grailsApplication.mainContext.addApplicationListener(new DomainSearchListener(this, datastore))
		//done
	}

	Map getIndexMapping() {
		if(indexMap?.size() == 0) {
			synchronized(startupMutex) {
				//reload it
				installDomainMappings()
			}
		}
		return indexMap
	}

	Map getIndexMap(String mappingName) {
		return ((Map)(getIndexMapping()[mappingName ?: 'poo']))
	}

	DomainMap getIndexMapDomainMap(String mappingName) {
		return ((DomainMap)(getIndexMapping()[mappingName ?: 'poo']?.domainMap))
	}

	public void installDomainMethods() {
		//nothing for now
	}

	//class mapping stuff
	public void installDomainMappings() {
		log.info("elastic - installing domain mappings")
		domainList = []
		indexMap = [:]
		for(GrailsClass clazz:grailsApplication.getArtefacts(DomainClassArtefactHandler.TYPE)) {
			GrailsDomainClass domainClass = (GrailsDomainClass)clazz
			try {
				def mapResults = mapDomainClass(domainClass)
				if(mapResults)
					domainList << mapResults
			} catch(ex) {
				log.error("error mapping domain class ${domainClass.getName()}: ${ex}", ex)
			}
		}
		//create the templates
		domainList?.each { DomainMap domainMap ->
			def domainMapping = domainMap.buildElasticMapping()
			if(domainMapping) {
				def indexName = domainMap.indexName
				def type = domainMap.typeName ?: domainMap.indexName
				def domainName = domainMap.getDomainName()
				//get latest index
				def currentVersion = getLatestVersion(indexName)
				if(currentVersion > -1) {
					log.debug("existing version found: ${indexName} - ${currentVersion}")
					//exists - update mappings
					def indexVersion = getIndexVersion(indexName, currentVersion)
					//update mapping and see if it fails
					def mappingConfig = domainMapping.mappings
					def mappingResults = installMapping(indexVersion + '/_mapping', mappingConfig)
					if(mappingResults.success != true) {
						def domainConfig = [domain:domainName, index:indexName, type:type, version:currentVersion, domainMap:domainMap,
											readVersion:currentVersion, writeVersion:currentVersion, oldIndex:true, indexVersion:indexVersion]
						indexMap[domainName] = domainConfig
						indexAliases[indexName] = indexVersion
						indexVersions[indexVersion] = indexName
						//migrate to a new index
						migrateIndex(domainConfig)
					} else {
						//repoint aliases?
						installAliases(indexVersion, currentVersion, indexName)
						def domainConfig = [domain:domainName, index:indexName, type:type, version:currentVersion, readVersion:currentVersion,
											writeVersion:currentVersion, domainMap:domainMap, indexVersion:indexVersion]
						indexMap[domainName] = domainConfig
						indexAliases[indexName] = indexVersion
						indexVersions[indexVersion] = indexName
					}
				} else {
					currentVersion = 0
					def indexVersion = getIndexVersion(indexName, currentVersion)
					//create index with mappings
					def indexConfig = [
							settings:[
									'number_of_shards':(elasticConfig?.numberOfShards ?: elasticConfig?.index?.numberOfShards ?: 5),
									'number_of_replicas':(elasticConfig?.numberOfReplicas ?: elasticConfig?.index?.numberOfReplicas ?: 1)
									// analysis: [analyzer: [ngram_analyzer: [tokenizer: "ngram_tokenizer"]], tokenizer: [ngram_tokenizer: [type:'nGram', min_gram: '2', max_gram:'50']]]
							]
					]
					def createResults = createIndex(indexVersion, indexConfig)
					if(createResults.success == true) {
						def mappingConfig = domainMapping.mappings
						def mappingResults = installMapping(indexVersion + '/_mapping', mappingConfig)
						if(mappingResults.success == true) {
							//alias it
							installAliases(indexVersion, currentVersion, indexName)
							//add it
							def domainConfig = [domain:domainName, index:indexName, type:type, version:currentVersion,
												readVersion:currentVersion, writeVersion:currentVersion, domainMap:domainMap, newIndex:true,
												indexVersion:indexVersion]
							indexMap[domainName] = domainConfig
							indexAliases[indexName] = indexVersion
							indexVersions[indexVersion] = indexName
						}
					}
				}
			}
		}
		log.info("elastic - finished installing domain mappings")
	}

	def indexDomains() {
		try {
			indexMap?.each { key, value ->
				indexDomain(value)
			}
		} catch(e) {
			log.error("error indexing domains: ${e}", e)
		}
	}

	//migrate
	def migrateIndex(Map domainConfig) {
		//create a new version
		def indexName = domainConfig.index
		def type = domainConfig.type
		def domainMapping = domainConfig.domainMap.buildElasticMapping()
		def nextVersion = getNextVersion(indexName)
		def indexVersion = getIndexVersion(indexName, nextVersion)
		//create index with mappings
		def indexConfig = [
				settings:[
						'number_of_shards':(elasticConfig?.numberOfShards ?: 5),
						'number_of_replicas':(elasticConfig?.numberOfReplicas ?: 0)
						// analysis: [analyzer: [ngram_analyzer: [tokenizer: "ngram_tokenizer"]], tokenizer: [ngram_tokenizer: [type:'nGram', min_gram: '2', max_gram:'50']]]
				]
		]
		def createResults = createIndex(indexVersion, indexConfig)
		if(createResults.success == true) {
			def mappingConfig = domainMapping.mappings
			def mappingResults = installMapping(indexVersion + '/_mapping', mappingConfig)
			if(mappingResults.success == true) {
				//alias writes
				installWriteAlias(indexVersion, nextVersion, indexName)
				domainConfig.writeVersion = nextVersion
				//reindex
				if(elasticConfig.bulkIndexOnStartup == true) {
					indexDomain(domainConfig)
				}
				//alias reads
				installReadAlias(indexVersion, nextVersion, indexName)
				domainConfig.version = nextVersion
				domainConfig.readVersion = nextVersion
				domainConfig.oldIndex = false
				indexMap[domainConfig.domain] = domainConfig
				indexAliases[indexName] = indexVersion
				indexVersions[indexVersion] = indexName
			} else {
				//map failed
			}
		} else {
			//create index failed
		}
	}

	def indexDomain(String domainName) {
		def artefactName = grailsApplication.getArtefact(DomainClassArtefactHandler.TYPE, domainName)?.getName()
		def domainMap = indexMap[artefactName]
		if(domainMap) {
			indexDomain(domainMap)
		} else {
			throw new Exception("Failed to index domain \"${domainName}\": Configuration not found in index map")
		}
	}

	//indexing
	def indexDomain(Map indexConfig) {
		try {
			def internalDomain = indexConfig.domainMap.getDomainClass().getClazz()
			log.info("Building index for ${indexConfig.domainMap.getDomainName()}")
			internalDomain.withNewSession { session ->
				def perPage = elasticConfig.maxBulkRequest ?: 100
				def count = internalDomain.count() ?: 0
				def runCount = Math.ceil(count.div(perPage))
				for(int i = 0; i < runCount; i++) {
					def offset = i * perPage
					chunkedIndexDomain(internalDomain,offset,perPage,indexConfig)
					flushQueue()
				}
			}
		} catch(e) {
			log.error("error indexing: ${e}", e)
		}
	}

	@Transactional(propagation=Propagation.REQUIRES_NEW,readOnly=true)
	def chunkedIndexDomain(internalDomain,offset,perPage,indexConfig) {
		def objList = internalDomain.withCriteria {
			firstResult(offset)
			maxResults(perPage)
			order('id', 'asc')
		}
		//index them
		objList.each { obj ->
			indexObject(obj, indexConfig.domainMap)
		}
	}

	def indexObject(Object obj, DomainMap domainMap) {
		def encodedObject = domainMap?.encodeDomain(obj)
		if(encodedObject) {
			def indexId = domainMap.getIndexId(obj)
			if(indexId) {
				def indexKey = domainMap.indexName + '_' + indexId
				synchronized(indexMutex) {
					indexRequests[indexKey] = [id:indexId, index:domainMap.indexName, domain:domainMap.getDomainName(),
											   doc:encodedObject]
				}
				//saveDocument(String index, String type, Map document, String docId = null, Map opts = [:])
			}
		}
	}

	def deleteObject(Object obj, DomainMap domainMap) {
		def indexId = domainMap?.getIndexId(obj)
		if(indexId) {
			def indexKey = domainMap.indexName + '_' + indexId
			synchronized(indexMutex) {
				deleteRequests[indexKey] = [id:indexId, index:domainMap.indexName, domain:domainMap.getDomainName()]
			}
		}
	}

	def flushQueue() {
		def toIndex = [:]
		def toDelete = [:]
		//clean in progress
		cleanInProgressRequests()
		//syncronize
		synchronized(indexMutex) {
			toIndex.putAll(indexRequests)
			toDelete.putAll(deleteRequests)
			indexRequests.clear()
			deleteRequests.clear()
		}
		//check if anything to do
		if(toIndex.isEmpty() == false || toDelete.isEmpty() == false) {
			//remove any deletes from the index request
			def deleteKeys = toDelete.keySet()
			toIndex.keySet().removeAll(deleteKeys)
			//setup a bulk operation
			def bulkRequest = ElasticQueryBuilder.prepareBulk()
			//add index requests
			toIndex.each { key, value ->
				def indexConfig = indexMap[value.domain]
				if(indexConfig) {
					//add it
					def indexName = getWriteIndex(value.index)
					bulkRequest.add('index', indexName, indexConfig.type, value.id, value.doc)
				}
			}
			//add delete requests
			toDelete?.each { key, value ->
				def indexConfig = indexMap[value.domain]
				if(indexConfig) {
					def indexName = getWriteIndex(value.index)
					bulkRequest.add('delete', indexName, indexConfig.type, value.id, null)
				}
			}
			//save them
			if(bulkRequest.hasContent()) {
				def results = elasticService.executeBulk(bulkRequest, [async:true])
				log.debug("bulk results: ${results}")
				if(results.success == false) {
					log.error("bulk errors: ${results.items}}")
				}
			}
		}
	}

	def cleanInProgressRequests() {

	}

	//helpers
	def getReadIndex(String index) {
		return index + '_read'
	}

	def getWriteIndex(String index) {
		return index + '_write'
	}

	def installAliases(String indexVersion, Integer version, String alias) {
		installReadAlias(indexVersion, version, alias)
		installWriteAlias(indexVersion, version, alias)
	}

	def installReadAlias(String indexVersion, Integer version, String alias) {
		def readAlias = getReadIndex(alias)
		def aliasResults = repointAlias(readAlias, indexVersion)
		//add straight up to
		aliasResults = repointAlias(alias, indexVersion)
		return aliasResults
	}

	def installWriteAlias(String indexVersion, Integer version, String alias) {
		def writeAlias = getWriteIndex(alias)
		def aliasResults = repointAlias(writeAlias, indexVersion)
		return aliasResults
	}

	// create an index template (this endpoint is used for datastreams)
	def installIndexTemplate(String name, Map config) {
		def templateResults = elasticService.executePut('_index_template', name, config)
		log.debug("install template: {}", templateResults)
		return templateResults
	}

	//templates
	def installTemplate(String name, Map config) {
		def templateResults = elasticService.executePut('_template', name, config)
		log.debug("install template: {}", templateResults)
		return templateResults
	}

	//mappings
	def getMappings() {
		def rtn = [success:false, mappingList:[]]
		def mappingResults = elasticService.executeGet('_mapping', null, null)
		if(mappingResults.success == true) {
			//println("mappingResults: ${mappingResults}")
			mappingResults.data?.each { key, value ->
				//key is the index name - value is a map { mappings: {}}
				rtn.mappingList << [index:key, name:key, config:value.mappings]
			}
			rtn.success = true
		}
		return rtn
	}

	def installMapping(String index, Map config) {
		def mappingResults = elasticService.executePut(index, null, config)
		log.debug("install mapping: {}", mappingResults)
		return mappingResults
	}

	def mappingExists(String index, String type) {

	}

	//indices
	def deleteIndex(String indexName) {
		def deleteResults = elasticService.deleteIndex(indexName)
		log.debug("delete index: {}", deleteResults)
	}

	def flatMerge(String indexName) {
		def mergeResults = elasticService.executeIndexMerge(indexName,true)
		log.debug("merge index: {}", mergeResults)
	}

	def createIndex(String indexName, Map config) {
		def createResults = elasticService.createIndex(indexName, config)
		log.debug("create index: {}", createResults)
		return createResults
	}

	def indexExists(String index) {
		def indexResults = findIndices(index)
		def indexMatch = indexResults.indexList.find{ it.name == index }
		return indexMatch ? true : false
	}

	def waitForIndex(String index, int version) {
		def rtn = false
		def retries = maxRetries
		while(rtn != true && retries > 0) {
			def latestVersion = getLatestVersion(index)
			if(latestVersion == null || latestVersion < version) {
				//sleep
				Thread.sleep(sleepInterval)
			} else {
				rtn = true
			}
		}
		return rtn
	}

	def getIndexVersion(String index, Integer version = null) {
		return version == null ? index : (index + '_v' + version)
	}

	def getIndices() {
		def rtn = [success:false, indexList:[]]
		def indexStats = elasticService.executeGet('_stats', null, null)
		if(indexStats.success == true) {
			indexStats.data.indices?.each { key, value ->
				rtn.indexList << [name:key, stats:value]
			}
			rtn.success = true
		}
		return rtn
	}

	def findIndices(String prefix) {
		def rtn = [success:false, indexList:[]]
		def indices = getIndices()
		if(prefix) {
			rtn.indexList = indices.indexList?.findAll {
				it.name =~ /^${prefix}/
			}
			rtn.success = true
		}
		return rtn
	}

	def getLatestVersion(String index) {
		def versions = findIndices(index).indexList?.collect {
			Matcher m = (it.name =~ /^${index}_v(\d+)$/)
			m ? m[0][1] as Integer : -1
		}.sort()
		versions ? versions.last() : -1
	}

	def getNextVersion(String index) {
		getLatestVersion(index) + 1
	}

	//alias
	def repointAlias(String alias, String index) {
		def existingAlias = getAliasIndex(alias)
		if(existingAlias == null) {
			createAlias(alias, index)
		} else if(existingAlias != index) {
			deleteAlias(alias, existingAlias)
			createAlias(alias, index)
		}
	}

	def createAlias(String alias, String index) {
		def existingAlias = getAliasIndex(alias)
		if(existingAlias == null || existingAlias != index) {
			def aliasConfig = [actions:[
					[add:[index:index, alias:alias]]
			]]
			def aliasResults = elasticService.executePost('_aliases', null, aliasConfig)
			log.debug("createAlias: {}", aliasConfig)
			return aliasResults
		}
	}

	def getAliases() {
		def rtn = [success:false, aliasList:[]]
		def aliasList = elasticService.executeGet('_aliases', null, null)
		if(aliasList.success == true) {
			//println("aliasList: ${aliasList}")
			aliasList.data?.each { key, value ->
				//key is the index name
				value.aliases?.each { aliasKey, aliasValue ->
					rtn.aliasList << [index:key, name:aliasKey, config:aliasValue]
				}
			}
			rtn.success = true
		}
		return rtn
	}

	def findAliases(String prefix) {
		def rtn = [success:false, aliasList:[]]
		def aliases = getAliases()
		if(prefix) {
			rtn.aliasList = aliases.aliasList?.findAll {
				it.name =~ /^${prefix}/
			}
			rtn.success = true
		}
		return rtn
	}

	def aliasExists(String alias) {
		def aliasResults = findAliases(alias)
		def aliasMatch = aliasResults.aliasList.find{ it.name == alias }
		return aliasMatch ? true : false
	}

	def getAliasIndex(String alias) {
		def aliasResults = findAliases(alias)
		def aliasMatch = aliasResults.aliasList.find{ it.name == alias }
		return aliasMatch ? aliasMatch.index : null
	}

	def deleteAlias(String alias, String index) {
		def aliasConfig = [actions:[
				[remove:[index:index, alias:alias]]
		]]
		def aliasResults = elasticService.executePost('_aliases', null, aliasConfig)
		log.debug("deleteAlias: {}", aliasResults)
		return aliasResults
	}

	//gorm listeners
	@CompileStatic
	static class DomainSearchListener extends AbstractPersistenceEventListener {

		ElasticAdminService adminService

		public DomainSearchListener(ElasticAdminService adminService, Datastore datastore) {
			super(datastore)
			this.adminService = adminService
		}

		@Override
		protected void onPersistenceEvent(AbstractPersistenceEvent event) {
			try {
				if(adminService.getIndexMap(getEventEntity(event)?.getClass().getSimpleName())) {
					if(event instanceof PostInsertEvent) {
						onPostInsert(event)
					} else if (event instanceof PostUpdateEvent) {
						onPostUpdate(event)
					} else if (event instanceof PostDeleteEvent) {
						onPostDelete(event)
					}
				}
			} catch(e) {
				log.error("error on elastic persistence event: ${e}", e)
			}
		}

		@Override
		boolean supportsEventType(Class<? extends ApplicationEvent> aClass) {
			return [PostInsertEvent, PostUpdateEvent, PostDeleteEvent].any() { it.isAssignableFrom(aClass) }
		}

		protected void onPostInsert(PostInsertEvent event) {
			Object entity = getEventEntity(event)
			if(entity) {
				String entityClass = entity.getClass().getSimpleName()
				adminService.indexObject(entity, adminService.getIndexMapDomainMap(entityClass))
				adminService.flushQueue()
			}
		}

		protected void onPostUpdate(PostUpdateEvent event) {
			Object entity = getEventEntity(event)
			if(entity) {
				String entityClass = entity.getClass().getSimpleName()
				adminService.indexObject(entity, adminService.getIndexMapDomainMap(entityClass))
				adminService.flushQueue()
			}
		}

		protected void onPostDelete(PostDeleteEvent event) {
			//queue a delete
			Object entity = getEventEntity(event)
			if(entity) {
				String entityClass = entity.getClass().getSimpleName()
				adminService.deleteObject(entity, adminService.getIndexMapDomainMap(entityClass))
				adminService.flushQueue()
			}
		}

		Object getEventEntity(AbstractPersistenceEvent event) {
			def rtn
			if(event.entityAccess)
				rtn = event.entityAccess.entity
			else
				rtn = event.entityObject
			return rtn
		}

	}

	//mapping domain
	public DomainMap mapDomainClass(GrailsDomainClass domainClass) {
		def rtn
		if(domainClass.hasProperty(getSearchablePropertyName())) {
			Object searchable = domainClass.getPropertyValue(getSearchablePropertyName())
			if(searchable instanceof Closure) {
				DomainMap domainMap = new DomainMap(grailsApplication, domainClass)
				Closure closure = (Closure)searchable.clone()
				closure.setDelegate(domainMap)
				closure.call()
				rtn = domainMap
			} else if(searchable instanceof Map) {
				DomainMap domainMap = new DomainMap(grailsApplication, domainClass)
				Closure closure = {
					for(key in searchable.keySet()) {
						"${key}"(searchable[key])
					}
				}
				closure.setDelegate(domainMap)
				closure.call()
				rtn = domainMap
			} else if(searchable instanceof Boolean && searchable == true) {
				DomainMap domainMap = new DomainMap(grailsApplication, domainClass)
				rtn = domainMap
			}
		}
		return rtn
	}

	@Commons
	static class DomainMap {

		private String domainName
		private Boolean all = true
		private Boolean root = true
		private GrailsDomainClass grailsDomainClass
		private PersistentEntity persistentEntity
		private GrailsApplication grailsApplication
		private List only
		private List except
		private String indexName
		private String typeName = '_doc'
		private Map searchProperties = [:]
		private Map searchConfig = [:]

		@Deprecated
		public DomainMap(GrailsApplication grailsApplication, GrailsDomainClass domainClass) {
			this.grailsDomainClass = domainClass
			this.grailsApplication = grailsApplication
		}

		public DomainMap(GrailsApplication grailsApplication, PersistentEntity persistentEntity) {
			this.persistentEntity = persistentEntity
			this.grailsApplication = grailsApplication
		}

		String getIndexQueueName(Object obj) {
			def rtn
			def idValue = getMethodValue(obj, 'getId', null, null)
			if(idValue) {
				rtn = indexName + '_' + idValue
			}
			return rtn
		}

		String getIndexId(Object obj) {
			def rtn
			def idValue = getMethodValue(obj, 'getId', null, null)
			if(idValue) {
				rtn = idValue.toString()
			}
			return rtn
		}

		String getDomainName() {
			return grailsDomainClass.getName()
		}

		GrailsDomainClass getDomainClass() {
			return grailsDomainClass
		}

		PersistentEntity domainClassToPersistentEntity() {
			return grailsApplication.getMappingContext().getPersistentEntity(grailsDomainClass.fullName)
		}

		void setAll(Boolean all) {
			this.all = all
		}

		void setRoot(Boolean root) {
			this.root = root
		}

		void setOnly(List only) {
			this.only = only
		}

		void setExcept(List except) {
			this.except = except
		}

		void root(Boolean rootFlag) {
			this.root = rootFlag
		}

		void setIndexName(String indexName) {
			this.indexName = indexName
		}

		void indexName(String indexName) {
			this.indexName = indexName
		}

		void setTypeName(String typeName) {
			//this.typeName = typeName
		}

		void typeName(String typeName) {
			//this.typeName = typeName
		}

		def invokeMethod(String name, args) {
			// Custom properties mapping options
			def property = grailsDomainClass.hasProperty(name)
			Assert.notNull(property, "unable to find property [$name] used in [$grailsDomainClass.propertyName]#${getSearchablePropertyName()}]")
			// Check if we already has mapping for this property.
			def propertyMap = searchProperties[name]
			if(propertyMap == null) {
				propertyMap = [:]
				searchProperties[name] = propertyMap
			}
			//
			//noinspection unchecked
			def attributes = (Map<String, Object>) ((Object[]) args)[0]
			attributes.each { key, value ->
				if(key == 'copy_to') {
					//add copy too field
					def copyMap = searchProperties[value]
					if(copyMap == null) {
						copyMap = [type:'text', copy:true]
						searchProperties[value] = copyMap
					}
				}
				propertyMap[key] = value
			}
			//set the type
			return null
		}

		public Map buildElasticTemplate() {
			def rtn
			def mappingConfig = buildElasticMapping()
			//return a template mapping
			def templateMap = [
					template:indexName + '_*',
					//settings: [
					//	'number_of_shards':5
					//],
					mappings:mappingConfig.mappings
			]
			rtn = [
					templateName:indexName + '_template',
					templateConfig: templateMap
			]
			return rtn
		}

		public Map buildElasticMapping() {
			def rtn
			//config
			def doOnly = only?.size() > 0
			def doExcept = except?.size() > 0
			def defaultExcept = elasticConfig.defaultExcept ?: null
			def doDefaultExcept = defaultExcept?.size() > 0
			def domainProperties = domainClassToPersistentEntity().persistentProperties
			//validate
			if(doOnly == true && doExcept == true) {
				//found both only and except
				throw new IllegalArgumentException("Both 'only' and 'except' were used in " +
						"'${grailsDomainClass.getPropertyName()}#${getSearchablePropertyName()}': provide one or neither but not both")
			}
			//build mappings
			def fieldMap = [:]
			searchConfig = [:]
			//iterate properties
			searchProperties?.each { String key, Map value ->
				def addKey = true
				if(doDefaultExcept) {
					def keyMatch = defaultExcept?.find{ it == key }
					if(keyMatch)
						addKey = false
					else
						addKey = true
				}
				if(addKey == true) {
					if(doOnly) {
						def keyMatch = only?.find{ it == key }
						if(keyMatch)
							addKey = true
						else
							addKey = false
					} else if(doExcept) {
						def keyMatch = except?.find{ it == key }
						if(keyMatch)
							addKey = false
						else
							addKey = true
					} else {
						addKey = true
					}
					if(addKey) {
						//find matching property
						def domainMatch = domainProperties?.find{ it.getName() == key }
						if(domainMatch || value?.copy == true) {
							if(value.reference == true) {
								def idProperty = domainClassToPersistentEntity().getPropertyByName('id')
								def idType = idProperty.type.name
								Map objectConfig = [:]
								//configure ref
								if(idType == 'text') {
									idType = 'keyword'
								} else if(idType == 'uuid') {
									idType = 'keyword'
								} else {
									idType = 'long'
								}
								objectConfig.id = [type:idType]
								//objectConfig.class = [type:'keyword', index:false]
								//objectConfig.ref = [type:'keyword', index:false]
								fieldMap[key] = [properties:objectConfig]
								searchConfig[key] = [key:key, type:'reference', mapping:fieldMap[key], domainProperty:domainMatch]
							} else {
								String type = value.type ?: getDomainFieldType(domainMatch) ?: 'text'
								Map fieldConfig = [type:type]
								//canned config
								if(type == 'date')
									fieldConfig.format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd'T'HH:mm:ss'Z'||strict_date_time||strict_date_time_no_millis||strict_date||epoch_millis"
								//think strict date time might have a bug missing millisenconds "strict_date_time||strict_date_time_no_millis||strict_date||epoch_millis"
								//add extra stuff
								value.each { propKey, propValue ->
									if(propKey == 'type' || propKey == 'copy') {
										//nodda
									} else if(propKey == 'excludeFromAll') {
										//if(propValue == true)
										//	fieldConfig['include_in_all'] = false
									} else {
										fieldConfig[propKey] = propValue
									}
								}
								fieldMap[key] = fieldConfig
								searchConfig[key] = [key:key, type:type, mapping:fieldConfig, domainProperty:domainMatch]
							}
						} else {
							log.warn("searchable mapping has invalid field for: ${grailsDomainClass.getName()} - ${key}")
						}
					}
				}
			}
			//handle rest of the domain properties
			domainProperties.each { domainProperty ->
				def key = domainProperty.getName()
				//println("key: ${key} class: ${domainProperty.getType().getName()} enum: ${domainProperty.isEnum()}")
				if(domainProperty.type.isEnum()) {
					//skip enum
					log.debug("have enum: ${key}")
				} else {
					if(fieldMap[key] == null) {
						//missing - add it
						def addKey = true
						if(doDefaultExcept) {
							def keyMatch = defaultExcept?.find{ it == key }
							if(keyMatch)
								addKey = false
							else
								addKey = true
						}
						if(addKey == true) {
							if(doOnly) {
								def keyMatch = only?.find{ it == key }
								if(keyMatch)
									addKey = true
								else
									addKey = false
							} else if(doExcept) {
								def keyMatch = except?.find{ it == key }
								if(keyMatch)
									addKey = false
								else
									addKey = true
							} else {
								addKey = true
							}
							if(addKey) {
								//find matching property
								String type = getDomainFieldType(domainProperty) ?: 'text'
								Map fieldConfig = [type:type]
								//canned config
								if(type == 'date')
									fieldConfig.format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||yyyy-MM-dd'T'HH:mm:ss'Z'||strict_date_time||strict_date_time_no_millis||strict_date||epoch_millis"
								//other stuff
								fieldMap[key] = fieldConfig
								searchConfig[key] = [key:key, type:type, mapping:fieldConfig, domainProperty:domainProperty]
							}
						}
					}
				}
			}
			//final mappings
			def type = typeName ?: indexName
			if(fieldMap.keySet()?.size() > 0) {
				//return a template mapping
				def mappingConfig = [:]
				mappingConfig.mappings = [properties:fieldMap]
				//build return
				rtn = mappingConfig
			}
			return rtn
		}

		public Map encodeDomain(Object obj) {
			def rtn = [:]
			searchConfig?.each { key, value ->
				if(value.type == 'reference') {
					def refObj = getFieldValue(obj, key, null)
					if(refObj) {
						//get id
						def idValue = getMethodValue(refObj, 'getId', null, null)
						if(idValue) {
							rtn[key] = [id:idValue]
						}
					}
				} else {
					def fieldValue = getFieldValue(obj, key, null)
					if(fieldValue instanceof Map) {
						//handle map
					} else if(fieldValue instanceof List) {
						//handle list
					} else {
						if((value.type == 'text' || value.type == 'keyword') && !(fieldValue instanceof CharSequence))
							rtn[key] = fieldValue ? String.valueOf(fieldValue) : null
						else
							rtn[key] = fieldValue
					}
				}
			}
			return rtn
		}

		public static String getDomainFieldType(property) {
			def rtn = 'text'
			def propertyType = property.getType()?.getName()
			if(propertyType) {
				if(propertyType == 'java.lang.String')
					rtn = 'text'
				else if(propertyType == 'java.lang.Long' || propertyType == 'long')
					rtn = 'long'
				else if(propertyType == 'java.lang.Integer' || propertyType == 'integer')
					rtn = 'integer'
				else if(propertyType == 'java.lang.Double' || propertyType == 'double')
					rtn = 'double'
				else if(propertyType == 'java.lang.Float' || propertyType == 'float')
					rtn = 'float'
				else if(propertyType == 'java.lang.Short' || propertyType == 'short')
					rtn = 'short'
				else if(propertyType == 'java.lang.Byte' || propertyType == 'byte')
					rtn = 'byte'
				else if(propertyType == 'java.lang.Boolean' || propertyType == 'boolean')
					rtn = 'boolean'
				else if(propertyType == 'java.util.Date' || propertyType == 'date')
					rtn = 'date'
				else
					log.warn("unmapped property type - using text: ${property?.getName()} ${propertyType}")
			} else {
				log.warn("no matching type for: ${property.getName()} ${property}")
			}
			return rtn
		}

		public static getFieldValue(Object obj, String field, Object defaultValue) {
			def rtn = defaultValue
			try { rtn = obj[field] } catch(e) {}
			return rtn
		}

		public static getMethodValue(Object obj, String method, Object defaultValue, args) {
			def rtn = defaultValue
			try { rtn = obj.invokeMethod(method, args) } catch(e) {}
			return rtn
		}

	}

	public static String getSearchablePropertyName() {
		return searchablePropertyName
	}

}
