package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.spidasoftware.mongodb.filter.FilterToDBQuery
import org.geotools.data.*
import org.geotools.feature.FeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType
import org.opengis.feature.type.Name
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy

import java.awt.*
import java.util.*
import java.util.logging.Logger

public class SpidaDbFeatureSource implements FeatureSource<FeatureType, Feature> {

    private static final Logger log = Logging.getLogger(SpidaDbFeatureSource.class.getPackage().getName())

    SpidaDbDataAccess store
    FeatureType featureType
    DBCollection dbCollection
    DB database
    String namespace
    BasicDBObject mapping

    public SpidaDbFeatureSource(SpidaDbDataAccess store, DB database, FeatureType featureType, BasicDBObject mapping) {
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
        return new FilterToDBQuery(this.dbCollection, this.featureType, this.mapping).getFeatureCollection(query)
    }

    @Override
    public FeatureType getSchema() {
        return featureType
    }

    @Override
    public ReferencedEnvelope getBounds() throws IOException { // TODO
        /*BasicDBList aggregateList = new BasicDBList()
        aggregateList.add(new BasicDBObject('$unwind': '$calcLocation.geographicCoordinate.coordinates'))
        aggregateList.add(new BasicDBObject('$group': new BasicDBObject('_id': '$_id',
                'lng': new BasicDBObject('$first': '$calcLocation.geographicCoordinate.coordinates'),
                'lat': new BasicDBObject('$last': '$calcLocation.geographicCoordinate.coordinates'))))
        aggregateList.add(new BasicDBObject('$group': new BasicDBObject('_id': null,
                'minLat': new BasicDBObject('$min': '$lat'),
                'minLng': new BasicDBObject('$min': '$lng'),
                'maxLat': new BasicDBObject('$max': '$lat'),
                'maxLng': new BasicDBObject('$max': '$lng'))))
        def iterator = dbCollection.aggregate(aggregateList)?.results()?.iterator()

        if(iterator.hasNext()) {
            DBObject dbObject = iterator.next()
            return new ReferencedEnvelope(dbObject.get("minLng"), dbObject.get("maxLng"), dbObject.get("minLat"), dbObject.get("maxLat"), DefaultGeographicCRS.WGS84)
        }*/
        return null
    }

    @Override
    public ReferencedEnvelope getBounds(Query query) throws IOException {
        return null
    }

    @Override
    public int getCount(Query query) throws IOException {
        return dbCollection.find().size() // TODO
    }

    @Override
    public Set<RenderingHints.Key> getSupportedHints() {
        return [] as Set
    }
}
