package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.ServerAddress
import com.mongodb.util.JSON
import com.spidasoftware.mongodb.feature.MongoDBFeatureCollectionIterator
import org.geotools.data.Query
import org.geotools.feature.FeatureCollection
import org.geotools.feature.NameImpl
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import spock.lang.Shared
import spock.lang.Specification

import java.util.logging.Logger

class SpidaDbFeatureSourceSpec extends Specification {

    static final Logger log = Logging.getLogger(SpidaDbFeatureSourceSpec.class.getPackage().getName())

    @Shared FeatureType featureType
    @Shared DB database
    @Shared BasicDBObject locationJSON
    @Shared SpidaDbDataAccess spidaDbDataAccess
    @Shared SpidaDbFeatureSource spidaDbFeatureSource
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
        spidaDbDataAccess = new SpidaDbDataAccess(namespace, host, port, databaseName, null, null, jsonMapping)
        database = mongoClient.getDB(databaseName)
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("locations").insert(locationJSON)

        featureType = spidaDbDataAccess.getSchema(new NameImpl(namespace, "location"))

        spidaDbFeatureSource = new SpidaDbFeatureSource(spidaDbDataAccess, database, featureType,jsonMapping.find { it.typeName == "location" })
    }

    void cleanupSpec () {
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
    }

    void "get getFeatures no filter or query"() {
        when:
            FeatureCollection featureCollection = spidaDbFeatureSource.getFeatures()
        then:
            featureCollection instanceof MongoDBFeatureCollectionIterator
            featureCollection.size() == database.getCollection("locations").count
    }

    void "get getFeatures with filter"() {
        when:
            FeatureCollection featureCollection = spidaDbFeatureSource.getFeatures(CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'"))
        then:
            featureCollection instanceof MongoDBFeatureCollectionIterator
            featureCollection.size() == 1
    }

    void "get getFeatures with query"() {
        when:
            FeatureCollection featureCollection = spidaDbFeatureSource.getFeatures(new Query("location", CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'")))
        then:
            featureCollection instanceof MongoDBFeatureCollectionIterator
            featureCollection.size() == 1
    }
}
