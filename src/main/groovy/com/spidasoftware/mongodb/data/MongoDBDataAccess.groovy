package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBObject
import com.mongodb.MongoClient
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.vividsolutions.jts.geom.Point
import org.geotools.data.DataAccess
import org.geotools.data.FeatureSource
import org.geotools.data.ServiceInfo
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.CRS
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType
import org.opengis.feature.type.Name

import java.util.logging.Logger



public class MongoDBDataAccess implements DataAccess<FeatureType, Feature> {

    private static final Logger log = Logging.getLogger(MongoDBDataAccess.class.getPackage().getName())

    String namespace
    String host
    String port
    String databaseName
    String username
    String password
    BasicDBList jsonMapping

    Map<Name, FeatureType> schemaCache = [:]
    MongoClient mongoClient
    DB database

    MongoDBDataAccess(String namespace, String host, String port, String databaseName, String username, String password, BasicDBList jsonMapping) {
        this.namespace  = namespace
        this.host = host
        this.port = port
        this.databaseName = databaseName
        this.username = username
        this.password = password
        this.jsonMapping = jsonMapping
        this.initDB()
    }
    @Override
    ServiceInfo getInfo() {
        return null
    }

    @Override
    void createSchema(FeatureType featureType) throws IOException {
        this.schemaCache.put(featureType.getName(), featureType)
    }

    @Override
    void updateSchema(Name typeName, FeatureType featureType) throws IOException {
        this.schemaCache.put(typeName, featureType)
    }

    @Override
    void removeSchema(Name typeName) throws IOException {
        this.schemaCache.remove(typeName)
    }

    @Override
    List<Name> getNames() throws IOException {
        List<Name> names = []
        this.jsonMapping.each { mapping ->
            log.info("mapping = ${mapping}")
            String collection = mapping.collection
            if (this.database.getCollectionNames().contains(collection)) {
                names << new NameImpl(this.namespace, mapping.typeName)
            } else {
                log.info("${collection} collection doesn't exist for typeName: ${mapping.typeName}")
            }
        }
        log.info("names = ${names}")
        return names
    }

    @Override
    FeatureType getSchema(Name typeName) throws IOException {
        if(this.schemaCache.get(typeName) == null) {
            BasicDBObject mapping = getMappingForName(typeName)
            SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder()
            simpleFeatureTypeBuilder.setName(typeName)

            if(mapping.geometry && mapping.displayGeometry) {
                simpleFeatureTypeBuilder.setCRS(CRS.decode(mapping.geometry.crs))
                simpleFeatureTypeBuilder.add(mapping.geometry.name, Point.class)
            }

            addAttributes(simpleFeatureTypeBuilder, mapping, mapping)

            this.schemaCache.put(typeName, simpleFeatureTypeBuilder.buildFeatureType())
        }
        return this.schemaCache.get(typeName)
    }

    private BasicDBObject getMappingForName(Name typeName) {
        return this.jsonMapping.find { it.typeName == typeName.getLocalPart() }
    }

    private void addAttributes(SimpleFeatureTypeBuilder simpleFeatureTypeBuilder, BasicDBObject mapping, BasicDBObject subCollectionMapping) {
        subCollectionMapping.attributes.each { BasicDBObject attribute ->
            if(attribute.name != "id" || (attribute.name == "id" && mapping.idAsAttribute)) {
                Class clazz = String
                if (attribute.class == "Double") {
                    clazz = Double
                } else if (attribute.class == "Long") {
                    clazz = Long
                } else if (attribute.class == "Boolean") {
                    clazz = Boolean
                }
                if (simpleFeatureTypeBuilder.attributes.find { it.localName == attribute.name } == null) {
                    simpleFeatureTypeBuilder.add(attribute.name, clazz)
                }
            }
        }
        subCollectionMapping.subCollections.each { BasicDBObject subCollection ->
            addAttributes(simpleFeatureTypeBuilder, mapping, subCollection)
        }
    }

    @Override
    FeatureSource<FeatureType, Feature> getFeatureSource(Name typeName) throws IOException {
        FeatureType featureType = getSchema(typeName)
        return new MongoDBFeatureSource(this, this.database, featureType, getMappingForName(typeName))
    }

    @Override
    void dispose() {
        mongoClient?.close()
    }

    protected void initDB() {
        try {
            def serverAddress = new ServerAddress(host, Integer.valueOf(port))
            if(username && password) {
                MongoCredential credential = MongoCredential.createCredential(username, databaseName, password.toCharArray());
                mongoClient = new MongoClient(serverAddress, Arrays.asList(credential));
            } else {
                mongoClient = new MongoClient(serverAddress)
            }
            database = mongoClient.getDB(databaseName)

        } catch(UnknownHostException e) {
            throw new IllegalArgumentException("Unknown mongodb host")
        }


        if(database == null) {
            throw new IllegalArgumentException("database does not exist")
        }

        this.jsonMapping.each { BasicDBObject mapping ->
            if(mapping.geometry) {
                def collectionName = mapping.collection
                def geometryPath = mapping.geometry.path

                def collection = database.getCollection(collectionName)

                def alreadyIndexed = collection.getIndexInfo().any { DBObject indexInfo ->
                    return indexInfo.key.get(geometryPath.toString()) != null
                }

                if (!alreadyIndexed) {
                    collection.createIndex(new BasicDBObject((geometryPath): "2dsphere"), new BasicDBObject('sparse', true))
                }
            }
        }
    }
}
