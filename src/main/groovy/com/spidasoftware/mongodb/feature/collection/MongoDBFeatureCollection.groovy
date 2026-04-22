package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.client.FindIterable
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.bson.Document
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.logging.Logging
import org.opengis.feature.simple.SimpleFeature
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
            Document dbObject = this.mongoCursor.next()
            this.featuresListDirect.addAll(buildFeaturesFromDocument(dbObject))
        }
    }

    @Override
    List<SimpleFeature> buildFeaturesFromDocument(Document dbObject) {
        SimpleFeatureBuilder simpleFeatureBuilder = new SimpleFeatureBuilder(this.featureType)
        addGeometry(simpleFeatureBuilder, dbObject)

        Map attributes = [:]
        this.mapping.attributes.each { attributeMapping ->
            attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping))
        }
        return [buildFromAttributes(attributes, dbObject)]
    }
}
