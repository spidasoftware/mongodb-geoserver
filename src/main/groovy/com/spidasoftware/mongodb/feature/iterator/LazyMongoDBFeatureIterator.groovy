package com.spidasoftware.mongodb.feature.iterator

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCursor
import com.spidasoftware.mongodb.feature.collection.AbstractMongoDBFeatureCollection
import org.bson.Document
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.util.logging.Logging
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import java.util.logging.Logger

/**
 * Lazy-loading feature iterator that fetches features on-demand from MongoDB cursor.
 * This reduces memory usage significantly for large result sets by not loading all
 * features into memory at once.
 */
class LazyMongoDBFeatureIterator implements SimpleFeatureIterator {

    private static final Logger log = Logging.getLogger(LazyMongoDBFeatureIterator.class.getPackage().getName())

    MongoCursor<Document> mongoCursor
    AbstractMongoDBFeatureCollection featureCollection
    BasicDBObject mapping
    Filter filter
    Integer max
    Integer offset
    // Flag to indicate if filter needs to be applied at Java level (for subcollections)
    private boolean applyFilterInJava = false

    // Buffer for sub-collection features (when one document produces multiple features)
    private List<SimpleFeature> featureBuffer = []
    private int featuresReturned = 0
    private int offsetSkipped = 0
    private SimpleFeature nextFeature = null
    private boolean exhausted = false

    LazyMongoDBFeatureIterator(MongoCursor<Document> mongoCursor, AbstractMongoDBFeatureCollection featureCollection) {
        this.mongoCursor = mongoCursor
        this.featureCollection = featureCollection
        this.mapping = featureCollection.mapping
        this.filter = featureCollection.filter
        this.max = featureCollection.max
        this.offset = featureCollection.offset ?: 0

        // For subcollections, filters need to be applied at Java level
        // because MongoDB can't filter on expanded/flattened nested documents
        this.applyFilterInJava = (mapping.subCollections != null && mapping.subCollections.size() > 0)

        // Pre-fetch first feature
        advanceToNext()
    }

    @Override
    boolean hasNext() {
        return nextFeature != null
    }

    @Override
    SimpleFeature next() throws NoSuchElementException {
        if (nextFeature == null) {
            throw new NoSuchElementException("No more features available")
        }

        SimpleFeature current = nextFeature
        featuresReturned++
        advanceToNext()
        return current
    }

    /**
     * Advance to the next valid feature, applying offset and max limits.
     */
    private void advanceToNext() {
        nextFeature = null

        if (exhausted || (max != null && featuresReturned >= max)) {
            return
        }

        while (nextFeature == null && !exhausted) {
            // First check the buffer
            if (!featureBuffer.isEmpty()) {
                SimpleFeature candidate = featureBuffer.remove(0)
                if (shouldInclude(candidate)) {
                    if (offsetSkipped < offset) {
                        offsetSkipped++
                    } else {
                        nextFeature = candidate
                    }
                }
                continue
            }

            // Buffer empty, fetch next document
            if (!mongoCursor.hasNext()) {
                exhausted = true
                return
            }

            Document dbObject = mongoCursor.next()
            List<SimpleFeature> features = featureCollection.buildFeaturesFromDocument(dbObject)

            if (features != null && !features.isEmpty()) {
                featureBuffer.addAll(features)
            }
        }
    }

    private boolean shouldInclude(SimpleFeature feature) {
        // Only apply filter at Java level for subcollections
        // For simple collections, filter was already applied by MongoDB
        if (!applyFilterInJava) {
            return true
        }
        return filter == null || filter.evaluate(feature)
    }

    @Override
    void close() {
        mongoCursor.close()
        featureBuffer.clear()
    }
}
