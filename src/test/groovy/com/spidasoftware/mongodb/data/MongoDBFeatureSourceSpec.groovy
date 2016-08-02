package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.util.JSON
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
    @Shared DB database
    @Shared BasicDBObject locationJSON
    @Shared MongoDBDataAccess MongoDBDataAccess
    @Shared MongoDBFeatureSource mongoDBFeatureSource
    @Shared BasicDBList jsonMapping
    @Shared String namespace = "http://spida/db"

    void setupSpec() {
        locationJSON = JSON.parse(getClass().getResourceAsStream('/location.json').text)
        String host = System.getProperty("mongoHost")
        String port = System.getProperty("mongoPort")
        String databaseName = System.getProperty("mongoDatabase")
        def serverAddress = new ServerAddress(host, Integer.valueOf(port))
        MongoClient mongoClient = new MongoClient(serverAddress)
        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        mongoDBDataAccess = new MongoDBDataAccess(namespace, host, port, databaseName, null, null, jsonMapping)
        database = mongoClient.getDB(databaseName)
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("locations").insert(locationJSON)

        featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "location"))

        mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType,jsonMapping.find { it.typeName == "location" })
    }

    void cleanupSpec () {
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
    }

    void "get getFeatures no filter or query"() {
        when:
            FeatureCollection featureCollection = mongoDBFeatureSource.getFeatures()
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == database.getCollection("locations").count
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
