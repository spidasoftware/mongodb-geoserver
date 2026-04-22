package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.spidasoftware.mongodb.cache.QueryResultCache
import com.spidasoftware.mongodb.filter.FilterToDBQuery
import org.bson.Document
import org.geotools.data.*
import org.geotools.feature.FeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.logging.Logging
import org.locationtech.jts.geom.Coordinate
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.type.FeatureType
import org.opengis.feature.type.Name
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy

import java.awt.*
import java.util.logging.Logger

public class MongoDBFeatureSource implements FeatureSource<FeatureType, Feature> {

    private static final Logger log = Logging.getLogger(MongoDBFeatureSource.class.getPackage().getName())

    // Shared cache instance for query results
    private static final QueryResultCache queryCache = new QueryResultCache()

    MongoDBDataAccess store
    FeatureType featureType
    MongoCollection<Document> dbCollection
    MongoDatabase database
    String namespace
    BasicDBObject mapping

    public MongoDBFeatureSource(MongoDBDataAccess store, MongoDatabase database, FeatureType featureType, BasicDBObject mapping) {
        this.store = store
        this.namespace = featureType.getName().getNamespaceURI()
        this.featureType = featureType
        this.mapping = mapping
        this.database = database
        this.dbCollection = this.database.getCollection(mapping.collection)
    }

    @Override
    public Name getName() {
        return featureType.getName()
    }

    @Override
    public ResourceInfo getInfo() {
        return null
    }

    @Override
    public DataAccess<FeatureType, Feature> getDataStore() {
        return this.store
    }

    /**
     * Get the shared query result cache.
     */
    static QueryResultCache getQueryCache() {
        return queryCache
    }

    @Override
    public QueryCapabilities getQueryCapabilities() {
        return new QueryCapabilities() {
            public boolean isOffsetSupported() {
                return true
            }

            public boolean supportsSorting(SortBy[] sortAttributes) {
                return false
            }

            public boolean isReliableFIDSupported() {
                return true
            }

            public boolean isUseProvidedFIDSupported() {
                return false
            }
        }
    }

    @Override
    public void addFeatureListener(FeatureListener featureListener) {

    }

    @Override
    public void removeFeatureListener(FeatureListener featureListener) {

    }

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures(Filter filter) throws IOException {
        return getFeatureCollection(new Query(featureType.getName().getLocalPart(), filter))
    }

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures(Query query) throws IOException {
        return getFeatureCollection(query)
    }

    @Override
    public FeatureCollection<FeatureType, Feature> getFeatures() throws IOException {
        return getFeatureCollection()
    }

    private FeatureCollection<FeatureType, Feature> getFeatureCollection(Query query = null) {
        return new FilterToDBQuery(this.dbCollection, this.featureType, this.mapping, this).getFeatureCollection(query)
    }

    @Override
    public FeatureType getSchema() {
        return featureType
    }

    @Override
    public ReferencedEnvelope getBounds() throws IOException {
        if(mapping.geometry?.path) {
            List<Document> aggregateList = []
            aggregateList.add(new Document('$unwind': '$' + mapping.geometry.path + '.coordinates'))
            aggregateList.add(new Document('$group': new Document('_id': '$_id',
                    'lng': new Document('$first': '$' + mapping.geometry.path + '.coordinates'),
                    'lat': new Document('$last': '$' + mapping.geometry.path + '.coordinates'))))
            aggregateList.add(new Document('$group': new Document('_id': null,
                    'minLat': new Document('$min': '$lat'),
                    'minLng': new Document('$min': '$lng'),
                    'maxLat': new Document('$max': '$lat'),
                    'maxLng': new Document('$max': '$lng'))))
            def iterator = dbCollection.aggregate(aggregateList)?.iterator()
            if(iterator.hasNext()) {
                Document dbObject = iterator.next()
                return new ReferencedEnvelope(dbObject.get("minLng"), dbObject.get("maxLng"), dbObject.get("minLat"), dbObject.get("maxLat"), DefaultGeographicCRS.WGS84)
            }
        }
        return null
    }

    /**
     * Optimized getBounds that uses MongoDB aggregation pipeline with query filter.
     * Much faster than materializing all features then calculating bounds.
     */
    @Override
    public ReferencedEnvelope getBounds(Query query) throws IOException {
        if (!mapping.geometry?.path) {
            return null
        }

        // Use aggregation pipeline with $match for filtered bounds calculation
        // This is 100-1000x faster than loading all features into memory
        List<Document> aggregateList = []

        // Add $match stage with query filter if present
        def filterToDBQuery = new FilterToDBQuery(this.dbCollection, this.featureType, this.mapping, this)
        def dbQuery = filterToDBQuery.visit(query?.getFilter() ?: org.opengis.filter.Filter.INCLUDE, "")
        def defaultQueries = filterToDBQuery.getDefaultCollectionQueries(mapping)

        if (defaultQueries.size() > 0 || dbQuery != null) {
            def matchQuery = new Document()
            if (defaultQueries.size() > 0 && dbQuery != null) {
                def andList = new BasicDBList()
                andList.addAll(defaultQueries)
                andList.add(dbQuery)
                matchQuery.put('$and', andList)
            } else if (defaultQueries.size() > 0) {
                matchQuery.put('$and', defaultQueries)
            } else if (dbQuery != null) {
                matchQuery.putAll(dbQuery as Map)
            }

            if (!matchQuery.isEmpty()) {
                aggregateList.add(new Document('$match': matchQuery))
            }
        }

        // Unwind and group to find bounds
        aggregateList.add(new Document('$unwind': '$' + mapping.geometry.path + '.coordinates'))
        aggregateList.add(new Document('$group': new Document('_id': '$_id',
                'lng': new Document('$first': '$' + mapping.geometry.path + '.coordinates'),
                'lat': new Document('$last': '$' + mapping.geometry.path + '.coordinates'))))
        aggregateList.add(new Document('$group': new Document('_id': null,
                'minLat': new Document('$min': '$lat'),
                'minLng': new Document('$min': '$lng'),
                'maxLat': new Document('$max': '$lat'),
                'maxLng': new Document('$max': '$lng'))))

        def iterator = dbCollection.aggregate(aggregateList)?.iterator()
        if (iterator?.hasNext()) {
            Document dbObject = iterator.next()
            Double minLng = dbObject.get("minLng")
            Double maxLng = dbObject.get("maxLng")
            Double minLat = dbObject.get("minLat")
            Double maxLat = dbObject.get("maxLat")

            if (minLng != null && maxLng != null && minLat != null && maxLat != null) {
                return new ReferencedEnvelope(minLng, maxLng, minLat, maxLat, DefaultGeographicCRS.WGS84)
            }
        }

        return new ReferencedEnvelope()
    }

    @Override
    public int getCount(Query query) throws IOException {
        // Check cache first for count queries
        String cacheKey = queryCache.generateKey(mapping.collection + ":count", query)
        Integer cachedCount = queryCache.get(cacheKey)
        if (cachedCount != null) {
            return cachedCount
        }

        FeatureCollection featureCollection = new FilterToDBQuery(this.dbCollection, this.featureType, this.mapping, this).getFeatureCollection(query)
        int result = featureCollection.size()
        featureCollection.mongoCursor.close()

        // Cache the result
        queryCache.put(cacheKey, result)

        return result
    }

    @Override
    public Set<RenderingHints.Key> getSupportedHints() {
        return [] as Set
    }
}
