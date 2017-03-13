package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType

import java.util.logging.Logger

class MongoDBFeatureCollection extends AbstractMongoDBFeatureCollection {

    private static final Logger log = Logging.getLogger(MongoDBFeatureCollection.class.getPackage().getName())

    MongoDBFeatureCollection(DBCursor dbCursor, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
        super(dbCursor, featureType, mapping, query, mongoDBFeatureSource)
    }

    @Override
    void initFeaturesList() {
        while(this.dbCursor.hasNext()) {
            SimpleFeatureBuilder simpleFeatureBuilder = new SimpleFeatureBuilder(this.featureType)
            DBObject dbObject =  this.dbCursor.next()
            addGeometry(simpleFeatureBuilder, dbObject)

            Map attributes = [:]
            this.mapping.attributes.each { attributeMapping ->
                attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping))
            }
            this.featuresList.add(buildFromAttributes(attributes, dbObject))
        }
    }
}
