package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.client.FindIterable
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.bson.Document
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType

import java.util.logging.Logger

class MongoDBFeatureCollection extends AbstractMongoDBFeatureCollection {

    private static final Logger log = Logging.getLogger(MongoDBFeatureCollection.class.getPackage().getName())

    MongoDBFeatureCollection(FindIterable<Document> findIterable, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
        super(findIterable, featureType, mapping, query, mongoDBFeatureSource)
    }

    @Override
    void initFeaturesList() {
        while(this.mongoCursor.hasNext()) {
            SimpleFeatureBuilder simpleFeatureBuilder = new SimpleFeatureBuilder(this.featureType)
            Document dbObject = this.mongoCursor.next()
            addGeometry(simpleFeatureBuilder, dbObject)

            Map attributes = [:]
            this.mapping.attributes.each { attributeMapping ->
                attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping))
            }
            this.featuresList.add(buildFromAttributes(attributes, dbObject))
        }
    }
}
