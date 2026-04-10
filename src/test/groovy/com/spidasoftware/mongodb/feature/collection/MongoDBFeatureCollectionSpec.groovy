package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.MongoClientSettings
import com.mongodb.ServerAddress
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import groovy.json.JsonSlurper
import org.bson.Document
import com.spidasoftware.mongodb.data.MongoDBDataAccess
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.geotools.data.Query
import org.geotools.feature.NameImpl
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType
import org.opengis.filter.Filter
import spock.lang.Specification

import java.util.logging.Logger

class MongoDBFeatureCollectionSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBFeatureCollectionSpec.class.getPackage().getName())

    FeatureType featureType
    MongoDatabase database
    Document locationJSON
    FindIterable<Document> findIterable
    MongoDBDataAccess mongoDBDataAccess
    MongoDBFeatureSource mongoDBFeatureSource
    BasicDBList jsonMapping
    String namespace = "http://spida/db"

    private static BasicDBList parseJsonResource(String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(MongoDBFeatureCollectionSpec.class.getResourceAsStream(resourcePath))
        return convertToBasicDBList(parsed)
    }

    private static Document parseJsonResourceAsDocument(String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(MongoDBFeatureCollectionSpec.class.getResourceAsStream(resourcePath))
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

    void setup() {
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
        findIterable = database.getCollection("locations").find(new Document("id", locationJSON.get("id")))

        jsonMapping = parseJsonResource('/mapping.json')
        mongoDBDataAccess = new MongoDBDataAccess(namespace, System.getProperty("mongoHost"), System.getProperty("mongoPort"), System.getProperty("mongoDatabase"), null, null, null, jsonMapping)
        featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "location"))
        mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType, jsonMapping.find { it.typeName == "location" })
    }

    void cleanup() {
        database.getCollection("locations").deleteOne(new Document("id", locationJSON.get("id")))
    }

    void testGetLocation() {
        setup:
            BasicDBObject mapping = jsonMapping.find { it.typeName == "location" }
            Query query = new Query("location", Filter.INCLUDE)
            def locationFeatureCollectionIterator = new MongoDBFeatureCollection(findIterable, featureType, mapping, query, mongoDBFeatureSource)
        when:
            Feature feature = locationFeatureCollectionIterator.featuresList.get(0)
        then:
            locationFeatureCollectionIterator.size() == 1
            feature.attributeCount == 22
            feature.getAttribute("id") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("name") == "684704E"
            feature.getAttribute("projectId") == "55fac7fde4b0e7f2e3be344f"
            feature.getAttribute("projectName") == "IJUS-44-2015-08-26-053"
            feature.getAttribute("dateModified") == 1442498557079
            feature.getAttribute("clientFile") == "SCE.client"
            feature.getAttribute("clientFileVersion") == "6ee5fba14760878be22701e1b3b7c05b"
            feature.getAttribute("comments") == "Two transformers connected to lower two cross arms"
            feature.getAttribute("streetNumber") == "8812"
            feature.getAttribute("street") == "Eberhart Rd NW"
            feature.getAttribute("city") == "Bolivar"
            feature.getAttribute("county") == "Tuscarawas"
            feature.getAttribute("state") == "OH"
            feature.getAttribute("zipCode") == "44622"
            feature.getAttribute("technician") == "Arle Rodriguez"
            feature.getAttribute("geographicCoordinate").coordinate.y == 33.80541229248047
            feature.getAttribute("geographicCoordinate").coordinate.x == -118.3824234008789
            feature.getAttribute("latitude") == 33.80541229248047
            feature.getAttribute("longitude") == -118.3824234008789
    }

    void testLimitLocationPropertyNames() {
        setup:
            BasicDBObject mapping = jsonMapping.find { it.typeName == "location" }
            Query query = new Query("location", Filter.INCLUDE, ["id", "name"] as String[])
            def locationFeatureCollectionIterator = new MongoDBFeatureCollection(findIterable, featureType, mapping, query, mongoDBFeatureSource)
        when:
            Feature feature = locationFeatureCollectionIterator.featuresList.get(0)
        then:
            locationFeatureCollectionIterator.size() == 1
            feature.attributeCount == 22
            feature.getAttribute("id") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("name") == "684704E"
            feature.getAttribute("projectId") == null
            feature.getAttribute("projectName") == null
            feature.getAttribute("dateModified") == null
            feature.getAttribute("clientFile") == null
            feature.getAttribute("clientFileVersion") == null
            feature.getAttribute("comments") == null
            feature.getAttribute("streetNumber") == null
            feature.getAttribute("street") == null
            feature.getAttribute("city") == null
            feature.getAttribute("county") == null
            feature.getAttribute("state") == null
            feature.getAttribute("zipCode") == null
            feature.getAttribute("user") == null
            feature.getAttribute("geographicCoordinate") == null
            feature.getAttribute("latitude") == null
            feature.getAttribute("longitude") == null
    }
}
