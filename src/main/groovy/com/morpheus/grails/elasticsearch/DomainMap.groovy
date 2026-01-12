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

@Commons
public class DomainMap {

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

	def methodMissing(String name, args) {
		// Custom properties mapping options
		def property = grailsDomainClass.hasProperty(name)
		Assert.notNull(property, "unable to find property [$name] used in [$grailsDomainClass.propertyName]#${ElasticAdminService.getSearchablePropertyName()}]")
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
		def defaultExcept = ElasticAdminService.elasticConfig.defaultExcept ?: null
		def doDefaultExcept = defaultExcept?.size() > 0
		def domainProperties = domainClassToPersistentEntity().persistentProperties
		//validate
		if(doOnly == true && doExcept == true) {
			//found both only and except
			throw new IllegalArgumentException("Both 'only' and 'except' were used in " +
					"'${grailsDomainClass.getPropertyName()}#${ElasticAdminService.getSearchablePropertyName()}': provide one or neither but not both")
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