package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import org.bson.Document
import org.geotools.data.DataAccess
import org.geotools.data.FeatureSource
import org.geotools.data.ServiceInfo
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.CRS
import org.geotools.util.logging.Logging
import org.locationtech.jts.geom.Point
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
    String uri
    BasicDBList jsonMapping

    Map<Name, FeatureType> schemaCache = [:]
    MongoClient mongoClient
    MongoDatabase database

    MongoDBDataAccess(String namespace, String host, String port, String databaseName, String username, String password, String uri, BasicDBList jsonMapping) {
        this.namespace  = namespace
        this.host = host
        this.port = port
        this.databaseName = databaseName
        this.username = username
        this.password = password
        this.uri = uri
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
        List<String> collectionNames = this.database.listCollectionNames().into([])
        this.jsonMapping.each { mapping ->
            String collection = mapping.collection
            if (collectionNames.contains(collection)) {
                names << new NameImpl(this.namespace, mapping.typeName)
            } else {
                log.info("${collection} collection doesn't exist for typeName: ${mapping.typeName}")
            }
        }
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
            if (uri) {
                ConnectionString connectionString = new ConnectionString(uri)
                mongoClient = MongoClients.create(connectionString)
            }
            else if (host && port) {
                def serverAddress = new ServerAddress(host, Integer.valueOf(port))
                if (username && password) {
                    MongoCredential credential = MongoCredential.createCredential(username, databaseName, password.toCharArray())
                    MongoClientSettings settings = MongoClientSettings.builder()
                        .applyToClusterSettings { builder -> builder.hosts([serverAddress]) }
                        .credential(credential)
                        .build()
                    mongoClient = MongoClients.create(settings)
                } else {
                    MongoClientSettings settings = MongoClientSettings.builder()
                        .applyToClusterSettings { builder -> builder.hosts([serverAddress]) }
                        .build()
                    mongoClient = MongoClients.create(settings)
                }
            }
            else {
                throw new IllegalArgumentException("Host & Port or URI Required")
            }
            database = mongoClient.getDatabase(databaseName)

        } catch(UnknownHostException e) {
            throw new IllegalArgumentException("Unknown mongodb host")
        }

        if (database == null) {
            throw new IllegalArgumentException("database does not exist")
        }

        this.jsonMapping.each { BasicDBObject mapping ->
            def collectionName = mapping.collection
            def collection = database.getCollection(collectionName)

            // Create geometry index
            if(mapping.geometry) {
                def geometryPath = mapping.geometry.path
                createIndexIfNotExists(collection, geometryPath, "2dsphere", true)
            }

            // Auto-create indexes for frequently queried attribute paths
            // This greatly improves query performance (10-100x for attribute queries)
            if (Boolean.getBoolean("mongodb.geoserver.autoCreateIndexes")) {
                Set<String> indexablePaths = collectIndexablePaths(mapping)
                indexablePaths.each { path ->
                    createIndexIfNotExists(collection, path, 1, false)
                }
            }
        }
    }

    /**
     * Collect all attribute paths that should be indexed for query performance.
     */
    private Set<String> collectIndexablePaths(BasicDBObject mapping, String prefix = null) {
        Set<String> paths = []

        mapping.attributes?.each { attr ->
            if (attr.path) {
                // Add the root path for indexing (not deeply nested)
                String path = attr.path
                String rootPath = path.contains(".") ? path.split("\\.").take(2).join(".") : path
                if (!rootPath.contains("coordinates")) { // Skip coordinate arrays
                    paths.add(rootPath)
                }
            }
        }

        // Recurse into sub-collections for commonly queried nested paths
        mapping.subCollections?.each { subCollection ->
            String subPath = subCollection.subCollectionPath
            if (subPath) {
                paths.add(subPath)
            }
        }

        return paths
    }

    /**
     * Create an index if it doesn't already exist.
     */
    private void createIndexIfNotExists(def collection, String path, def indexType, boolean sparse) {
        try {
            def existingIndexes = collection.listIndexes().collect { it.get("key")?.keySet() }.flatten()

            if (!existingIndexes.contains(path)) {
                def indexOptions = new com.mongodb.client.model.IndexOptions()
                    .sparse(sparse)
                    .background(true) // Create index in background to avoid locking

                collection.createIndex(new BasicDBObject((path): indexType), indexOptions)
                log.info("Created index on ${collection.getNamespace()}.${path}")
            }
        } catch (Exception e) {
            // Log but don't fail - indexes are optimization, not required
            log.warning("Failed to create index on ${path}: ${e.message}")
        }
    }
}
