package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.MongoClientSettings
import com.mongodb.ServerAddress
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import groovy.json.JsonSlurper
import org.bson.Document
import com.spidasoftware.mongodb.feature.collection.MongoDBFeatureCollection
import org.geotools.data.Query
import org.geotools.feature.FeatureCollection
import org.geotools.feature.NameImpl
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import spock.lang.Shared
import spock.lang.Specification

import java.util.logging.Logger

class MongoDBFeatureSourceSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBFeatureSourceSpec.class.getPackage().getName())

    @Shared FeatureType featureType
    @Shared MongoDatabase database
    @Shared Document locationJSON
    @Shared MongoDBDataAccess MongoDBDataAccess
    @Shared MongoDBFeatureSource mongoDBFeatureSource
    @Shared BasicDBList jsonMapping
    @Shared String namespace = "http://spida/db"

    private static BasicDBList parseJsonResource(String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(MongoDBFeatureSourceSpec.class.getResourceAsStream(resourcePath))
        return convertToBasicDBList(parsed)
    }

    private static Document parseJsonResourceAsDocument(String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(MongoDBFeatureSourceSpec.class.getResourceAsStream(resourcePath))
        return new Document(convertToBasicDBObject(parsed))
    }

    private static Object convertValue(Object value) {
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue()
        } else if (value instanceof Map) {
            return convertToBasicDBObject(value)
        } else if (value instanceof List) {
            return convertToBasicDBList(value)
        }
        return value
    }

    private static BasicDBList convertToBasicDBList(List list) {
        BasicDBList dbList = new BasicDBList()
        list.each { item ->
            dbList.add(convertValue(item))
        }
        return dbList
    }

    private static BasicDBObject convertToBasicDBObject(Map map) {
        BasicDBObject dbObject = new BasicDBObject()
        map.each { key, value ->
            dbObject.put(key, convertValue(value))
        }
        return dbObject
    }

    void setupSpec() {
        locationJSON = parseJsonResourceAsDocument('/location.json')
        String host = System.getProperty("mongoHost")
        String port = System.getProperty("mongoPort")
        String databaseName = System.getProperty("mongoDatabase")
        def serverAddress = new ServerAddress(host, Integer.valueOf(port))
        MongoClientSettings settings = MongoClientSettings.builder()
            .applyToClusterSettings { builder -> builder.hosts([serverAddress]) }
            .build()
        MongoClient mongoClient = MongoClients.create(settings)
        jsonMapping = parseJsonResource('/mapping.json')
        mongoDBDataAccess = new MongoDBDataAccess(namespace, host, port, databaseName, null, null, null, jsonMapping)
        database = mongoClient.getDatabase(databaseName)
        database.getCollection("locations").deleteOne(new Document("id", locationJSON.get("id")))
        database.getCollection("locations").insertOne(locationJSON)

        featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "location"))

        mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType, jsonMapping.find { it.typeName == "location" })
    }

    void cleanupSpec () {
        database.getCollection("locations").deleteOne(new Document("id", locationJSON.get("id")))
    }

    void "get getFeatures no filter or query"() {
        when:
            FeatureCollection featureCollection = mongoDBFeatureSource.getFeatures()
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == database.getCollection("locations").countDocuments()
    }

    void "get getFeatures with filter"() {
        when:
            FeatureCollection featureCollection = mongoDBFeatureSource.getFeatures(CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'"))
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == 1
    }

    void "get getFeatures with query"() {
        when:
            FeatureCollection featureCollection = mongoDBFeatureSource.getFeatures(new Query("location", CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'")))
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == 1
    }
}
