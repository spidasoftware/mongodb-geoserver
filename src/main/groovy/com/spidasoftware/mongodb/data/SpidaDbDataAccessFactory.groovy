package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.geotools.data.DataAccess
import org.geotools.data.DataAccessFactory
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType

import java.awt.*
import java.util.logging.Logger

public class SpidaDbDataAccessFactory implements DataAccessFactory {

    private static final Logger log = Logging.getLogger(SpidaDbDataAccessFactory.class.getPackage().getName())

    final DataAccessFactory.Param HOST = new DataAccessFactory.Param("host", String.class, "MongoDB host", true)
    final DataAccessFactory.Param PORT = new DataAccessFactory.Param("port", String.class, "MongoDB port", true)
    final DataAccessFactory.Param DATABASE_NAME = new DataAccessFactory.Param("databaseName", String.class, "MongoDB database name", true)
    final DataAccessFactory.Param USERNAME = new DataAccessFactory.Param("username", String.class, "MongoDB username", false)
    final DataAccessFactory.Param PASSWORD = new DataAccessFactory.Param("password", String.class, "MongoDB passsword", false)
    final DataAccessFactory.Param NAMESPACE = new DataAccessFactory.Param("namespace", String.class, "Namespace", true)
    final DataAccessFactory.Param FEATURE_TYPE_MAPPING_FILE = new DataAccessFactory.Param("featureTypeMappingFile", String.class, "FEATURE_TYPE_MAPPING_FILE", true)

    @Override
    DataAccess<? extends FeatureType, ? extends Feature> createDataStore(Map<String, Serializable> params) throws IOException {
        BasicDBList jsonMapping = JSON.parse( new File((String) FEATURE_TYPE_MAPPING_FILE.lookUp(params)).text)
        return new SpidaDbDataAccess((String) NAMESPACE.lookUp(params),
                                     (String) HOST.lookUp(params),
                                     (String) PORT.lookUp(params),
                                     (String) DATABASE_NAME.lookUp(params),
                                     (String) USERNAME.lookUp(params),
                                     (String) PASSWORD.lookUp(params),
                                     jsonMapping)
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
        return [HOST, PORT, DATABASE_NAME, USERNAME, PASSWORD, FEATURE_TYPE_MAPPING_FILE, NAMESPACE]
    }

    @Override
    boolean canProcess(Map<String, Serializable> params) {
        try {
            String featureTypeMappingFile = FEATURE_TYPE_MAPPING_FILE.lookUp(params)
            def mappingFile = new File(featureTypeMappingFile)
            BasicDBList jsonMapping = JSON.parse(mappingFile.text)
            assert jsonMapping.size() > 0
            jsonMapping.each { mapping ->
                assert mapping.typeName != null
                assert mapping.collection != null
                assert mapping.idAsAttribute != null
                validateMappingAttributes(mapping)
            }

            [NAMESPACE, HOST, PORT, DATABASE_NAME].each {
                assert it.lookUp(params) != null
            }
        } catch(Exception | AssertionError e) {
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
        mapping.subCollections.each { subCollection ->
            assert subCollection.subCollectionPath != null
            assert subCollection.includeInDefaultQuery != null
            validateMappingAttributes(subCollection)
        }
    }

    @Override
    boolean isAvailable() {
        try {
            Class.forName("com.mongodb.DB")
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
