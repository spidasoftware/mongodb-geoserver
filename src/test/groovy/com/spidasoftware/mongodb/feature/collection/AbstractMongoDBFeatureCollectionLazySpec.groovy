package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCursor
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.bson.Document
import org.geotools.data.Query
import org.geotools.feature.NameImpl
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.type.FeatureType
import org.opengis.filter.Filter
import spock.lang.Specification

class AbstractMongoDBFeatureCollectionLazySpec extends Specification {

    void "lazy features and materialization close their cursors"() {
        setup:
            boolean previousLazyLoading = AbstractMongoDBFeatureCollection.ENABLE_LAZY_LOADING
            AbstractMongoDBFeatureCollection.ENABLE_LAZY_LOADING = true

            def firstCursor = Mock(MongoCursor)
            firstCursor.hasNext() >>> [true, false]
            firstCursor.next() >> new Document("id", "feature-1")

            def secondCursor = Mock(MongoCursor)
            secondCursor.hasNext() >>> [true, false]
            secondCursor.next() >> new Document("id", "feature-2")

            def findIterable = Mock(FindIterable)
            findIterable.iterator() >>> [firstCursor, secondCursor]

            FeatureType featureType = Stub(FeatureType) {
                getName() >> new NameImpl("http://spida/db", "testType")
            }

            Query query = new Query("testType", Filter.INCLUDE)
            BasicDBObject mapping = new BasicDBObject([
                displayGeometry : false,
                subCollections  : []
            ])

            def featureSource = Mock(MongoDBFeatureSource)
            def collection = new TestLazyCollection(findIterable, featureType, mapping, query, featureSource)
        when:
            def iterator = collection.features()
            iterator.hasNext()
            iterator.next()
            iterator.close()

            collection.toArray()
        then:
            1 * firstCursor.close()
            1 * secondCursor.close()
        cleanup:
            AbstractMongoDBFeatureCollection.ENABLE_LAZY_LOADING = previousLazyLoading
    }

    private static class TestLazyCollection extends AbstractMongoDBFeatureCollection {

        TestLazyCollection(FindIterable<Document> findIterable, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
            super(findIterable, featureType, mapping, query, mongoDBFeatureSource)
        }

        @Override
        void initFeaturesList() {
        }

        @Override
        List<SimpleFeature> buildFeaturesFromDocument(Document dbObject) {
            return [[
                getID: { -> dbObject.getString("id") }
            ] as SimpleFeature]
        }
    }
}
