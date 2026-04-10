package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import groovy.json.JsonSlurper
import org.geotools.data.DataAccess
import org.geotools.data.DataAccessFactory
import org.geotools.data.Parameter
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType

import java.awt.*
import java.util.logging.Logger

public class MongoDBDataAccessFactory implements DataAccessFactory {

    private static final Logger log = Logging.getLogger(MongoDBDataAccessFactory.class.getPackage().getName())

    final DataAccessFactory.Param HOST = new DataAccessFactory.Param("host", String.class, "MongoDB host", false)
    final DataAccessFactory.Param PORT = new DataAccessFactory.Param("port", String.class, "MongoDB port", false)
    final DataAccessFactory.Param DATABASE_NAME = new DataAccessFactory.Param("databaseName", String.class, "MongoDB database name", true)
    final DataAccessFactory.Param USERNAME = new DataAccessFactory.Param("username", String.class, "MongoDB username", false)
    final DataAccessFactory.Param PASSWORD = new DataAccessFactory.Param("password", String.class, "MongoDB passsword", false, '', [(Parameter.IS_PASSWORD): true])
    final DataAccessFactory.Param NAMESPACE = new DataAccessFactory.Param("namespace", String.class, "Namespace", true)
    final DataAccessFactory.Param URI = new DataAccessFactory.Param("uri", String.class, "MongoDB URI", false)
    final DataAccessFactory.Param FEATURE_TYPE_MAPPING_FILE = new DataAccessFactory.Param("featureTypeMappingFile", String.class, "FEATURE_TYPE_MAPPING_FILE", true)

    @Override
    DataAccess<? extends FeatureType, ? extends Feature> createDataStore(Map<String, Serializable> params) throws IOException {
        BasicDBList jsonMapping = parseJsonFile(new File((String) FEATURE_TYPE_MAPPING_FILE.lookUp(params)).text)
        return new MongoDBDataAccess((String) NAMESPACE.lookUp(params),
                                     (String) HOST.lookUp(params),
                                     (String) PORT.lookUp(params),
                                     (String) DATABASE_NAME.lookUp(params),
                                     (String) USERNAME.lookUp(params),
                                     (String) PASSWORD.lookUp(params),
                                     (String) URI.lookUp(params),
                                     jsonMapping)
    }

    private static BasicDBList parseJsonFile(String jsonText) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parseText(jsonText)
        return convertListToBasicDBList(parsed)
    }

    private static BasicDBList convertListToBasicDBList(java.util.List list) {
        BasicDBList dbList = new BasicDBList()
        list.each { item ->
            if (item instanceof java.util.Map) {
                dbList.add(convertMapToBasicDBObject((java.util.Map) item))
            } else if (item instanceof java.util.List) {
                dbList.add(convertListToBasicDBList((java.util.List) item))
            } else {
                dbList.add(item)
            }
        }
        return dbList
    }

    private static BasicDBObject convertMapToBasicDBObject(java.util.Map map) {
        BasicDBObject dbObject = new BasicDBObject()
        map.each { key, value ->
            if (value instanceof java.util.Map) {
                dbObject.put(key, convertMapToBasicDBObject((java.util.Map) value))
            } else if (value instanceof java.util.List) {
                dbObject.put(key, convertListToBasicDBList((java.util.List) value))
            } else {
                dbObject.put(key, value)
            }
        }
        return dbObject
    }

    @Override
    String getDisplayName() {
        return "Mongo DB"
    }

    @Override
    String getDescription() {
        return "${getDisplayName()} WFS plugin"
    }

    @Override
    DataAccessFactory.Param[] getParametersInfo() {
        return [HOST, PORT, DATABASE_NAME, USERNAME, PASSWORD, URI, FEATURE_TYPE_MAPPING_FILE, NAMESPACE]
    }

    @Override
    boolean canProcess(Map<String, Serializable> params) {
        try {
            String featureTypeMappingFile = FEATURE_TYPE_MAPPING_FILE.lookUp(params)
            def mappingFile = new File(featureTypeMappingFile)
            BasicDBList jsonMapping = parseJsonFile(mappingFile.text)
            assert jsonMapping.size() > 0
            jsonMapping.each { mapping ->
                assert mapping.typeName != null
                assert mapping.collection != null
                assert mapping.idAsAttribute != null
                validateMappingAttributes(mapping)
            }

            [NAMESPACE, DATABASE_NAME].each {
                assert it.lookUp(params) != null
            }
            // if URI not set, HOST & PORT must be
            if (URI.lookUp(params) == null) {
                [HOST, PORT].each {
                    assert it.lookUp(params) != null
                }
            }
        } catch (Exception | AssertionError e) {
            log.info("Error, can't process: ${e.toString()}")
            return false
        }
        return true
    }

    void validateMappingAttributes(BasicDBObject mapping) {
        assert mapping.attributes.findAll { it.useObjectKey }.size() <= 1
        mapping.attributes.each { attr ->
            assert attr.name != null
            assert attr.path != null ||
                   attr.value != null ||
                   attr.subCollectionPath != null ||
                   attr.concatenate != null ||
                   attr.useKey != null ||
                   attr.useValue != null ||
                   attr.useObjectKey != null ||
                   attr.stringValue != null
        }
        mapping.subCollections?.each { subCollection ->
            assert subCollection.subCollectionPath != null
            assert subCollection.includeInDefaultQuery != null
            validateMappingAttributes(subCollection)
        }
    }

    @Override
    boolean isAvailable() {
        try {
            Class.forName("com.mongodb.client.MongoClient")
            return  true
        } catch(ClassNotFoundException e) {
            return false
        }
    }

    @Override
    Map<RenderingHints.Key, ?> getImplementationHints() {
        return Collections.EMPTY_MAP
    }
}
