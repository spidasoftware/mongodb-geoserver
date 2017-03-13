package com.spidasoftware.mongodb.feature.iterator

import com.mongodb.DBCursor
import com.mongodb.DBObject
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.util.logging.Logging
import org.opengis.feature.simple.SimpleFeature

import java.util.logging.Logger

class MongoDBFeatureIterator implements SimpleFeatureIterator {

    private static final Logger log = Logging.getLogger(MongoDBFeatureIterator.class.getPackage().getName())

    List<SimpleFeature> featuresList = []
    DBCursor dbCursor

    MongoDBFeatureIterator(DBCursor dbCursor, List<SimpleFeature> featuresList) {
        this.dbCursor = dbCursor
        this.featuresList = featuresList
    }

    @Override
    boolean hasNext() {
        return this.featuresList.size() > 0
    }

    @Override
    SimpleFeature next() throws NoSuchElementException {
        if(this.hasNext()) {
            return this.featuresList.remove(0)
        }
        throw new NoSuchElementException()
    }

    @Override
    void close() {
        this.dbCursor.close()
    }
}
