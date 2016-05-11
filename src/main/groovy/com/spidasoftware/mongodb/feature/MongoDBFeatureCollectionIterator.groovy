package com.spidasoftware.mongodb.feature

import com.mongodb.BasicDBObject
import com.mongodb.DBCursor
import com.mongodb.DBObject
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType

import java.util.logging.Logger

class MongoDBFeatureCollectionIterator extends AbstractMongoDBFeatureCollectionIterator {

    private static final Logger log = Logging.getLogger(MongoDBFeatureCollectionIterator.class.getPackage().getName())

    MongoDBFeatureCollectionIterator(DBCursor dbCursor, FeatureType featureType, BasicDBObject mapping, Query query) {
        super(dbCursor, featureType, mapping, query)
    }

    @Override
    boolean isEmpty() {
        return this.dbCursor.hasNext()
    }

    @Override
    int size() {
        return this.dbCursor.size()
    }

    @Override
    boolean hasNext() {
        return this.dbCursor.hasNext()
    }

    @Override
    Feature next() throws NoSuchElementException {
        SimpleFeatureBuilder simpleFeatureBuilder = new SimpleFeatureBuilder(this.featureType)
        DBObject dbObject =  dbCursor.next()
        addGeometry(simpleFeatureBuilder, dbObject)

        Map attributes = [:]
        this.mapping.attributes.each { attributeMapping ->
            attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping))
        }
        return buildFromAttributes(attributes, dbObject)
    }
}
