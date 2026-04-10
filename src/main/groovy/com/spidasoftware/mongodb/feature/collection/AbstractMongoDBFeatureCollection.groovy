package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCursor
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import com.spidasoftware.mongodb.feature.iterator.MongoDBFeatureIterator
import org.apache.commons.lang3.StringUtils
import org.bson.Document
import org.geotools.data.Query
import org.geotools.data.complex.feature.type.ComplexFeatureTypeFactoryImpl
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.GeometryAttributeImpl
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTS
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultProjectedCRS
import org.geotools.util.logging.Logging
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Point
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.feature.type.FeatureType
import org.opengis.feature.type.GeometryDescriptor
import org.opengis.feature.type.GeometryType
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.cs.AxisDirection
import org.opengis.referencing.operation.MathTransform
import org.opengis.util.ProgressListener

import java.util.logging.Logger

abstract class AbstractMongoDBFeatureCollection implements SimpleFeatureCollection {

    private static final Logger log = Logging.getLogger(AbstractMongoDBFeatureCollection.class.getPackage().getName())

    FindIterable<Document> findIterable
    MongoCursor<Document> mongoCursor
    FeatureType featureType
    BasicDBObject mapping
    List propertyNames
    CoordinateReferenceSystem targetCRS = null
    CoordinateReferenceSystem sourceCRS
    MathTransform transform
    String namespace
    Integer max
    Integer offset
    Filter filter
    Query query
    MongoDBFeatureSource mongoDBFeatureSource
    List<SimpleFeature> featuresList = []

    static final int LONGITUDE_POSITION = 0
    static final int LATITUDE_POSITION = 1

    AbstractMongoDBFeatureCollection(FindIterable<Document> findIterable, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
        this.findIterable = findIterable
        this.mongoCursor = findIterable.iterator()
        this.featureType = featureType
        this.mapping = mapping
        this.query = query
        this.propertyNames = query?.getPropertyNames()
        this.namespace = featureType.getName().getNamespaceURI()
        this.max = query?.getMaxFeatures()
        this.offset = query?.getStartIndex()
        this.filter = query?.getFilter()
        this.mongoDBFeatureSource = mongoDBFeatureSource

        if(this.mapping.displayGeometry && this.mapping.geometry) {
            this.sourceCRS = CRS.decode(this.mapping.geometry.crs)
            this.targetCRS = query?.getCoordinateSystemReproject() ?: sourceCRS
            if (this.sourceCRS != this.targetCRS) {
                try {
                    /*
                    For some reason this errors out the first call to find the transform but works on the second call
                     */
                    this.transform = CRS.findMathTransform(this.sourceCRS, this.targetCRS)
                } catch (e) {
                    this.transform = CRS.findMathTransform(this.sourceCRS, this.targetCRS)
                }
            }
        }
        this.initFeaturesList()
    }

    abstract void initFeaturesList()

    @Override
    boolean isEmpty() {
        return featuresList.isEmpty()
    }

    @Override
    int size() {
        return this.featuresList.size()
    }

    protected SimpleFeature buildFromAttributes(Map attributes, Document dbObject) {
        SimpleFeatureBuilder simpleFeatureBuilder = new SimpleFeatureBuilder(this.featureType)

        addGeometry(simpleFeatureBuilder, dbObject)

        def id = attributes.get("id")
        if(!this.mapping.idAsAttribute) {
            attributes.remove("id")
        }

        attributes.each { name, value ->
            addAttribute(simpleFeatureBuilder, value, name)
        }

        return simpleFeatureBuilder.buildFeature(id ?: dbObject.get("id"))
    }

    protected String getAttributeValueFromDBObject(Document dbObject, BasicDBObject attributeMapping, Document subCollectionObject = null, List<Integer> indices = null) {
        String value
        if(attributeMapping.value) {
            value = attributeMapping.value
        } else if(attributeMapping.concatenate) {
            value = ""
            attributeMapping.concatenate.eachWithIndex { BasicDBObject concatObject, int idx ->
                if(idx > 0) {
                    value += "_"
                }
                if(concatObject.path) {
                    value += getObjectFromPath(dbObject, concatObject.path)
                } else if(concatObject.value) {
                    value += concatObject.value
                } else if(concatObject.subCollectionPath) {
                    value += getObjectFromPath(subCollectionObject, concatObject.subCollectionPath)
                } else if(concatObject.currentIndex) {
                    value += indices.join("_")
                }
            }
        } else if(attributeMapping.path) {
            value = getObjectFromPath(dbObject, attributeMapping.path)
        } else if(attributeMapping.subCollectionPath) {
            value = getObjectFromPath(subCollectionObject, attributeMapping.subCollectionPath)
        }
        return value
    }

    protected void addAttribute(SimpleFeatureBuilder simpleFeatureBuilder, def value, String name) {
        if(value != null && containsProperty(name)) {
            simpleFeatureBuilder.set(new NameImpl(this.namespace, name), value)
        }
    }

    protected boolean containsProperty(String property) {
        return propertyNames == null || propertyNames.size() == 0 || propertyNames.contains(property)
    }

    protected Object getObjectFromPath(Object dbObject, String path) {
        Object result = dbObject
        path.split("\\.").each { String key ->
            if(!StringUtils.isBlank(key)) {
                if (result instanceof List) {
                    // Handle array access with numeric index
                    try {
                        int index = Integer.parseInt(key)
                        result = result.get(index)
                    } catch (NumberFormatException e) {
                        // Key is not a number, can't access list with it
                        result = null
                    }
                } else if (result instanceof Map) {
                    result = result.get(key)
                } else {
                    result = null
                }
            }
        }
        return result
    }

    protected void addGeometry(SimpleFeatureBuilder simpleFeatureBuilder, Document dbObject) {

        if (this.mapping.displayGeometry && this.mapping.geometry && containsProperty(this.mapping.geometry.name)) {
            // See Axis ordering here: http://docs.geoserver.org/2.7.2/user/services/wfs/basics.html
            // geographicCoordinate
            Document geographicCoordinate = (Document) getObjectFromPath(dbObject, this.mapping.geometry.path)
            List cooridnates = (List) geographicCoordinate.get("coordinates")
            Double latitude = cooridnates.get(LATITUDE_POSITION)
            Double longitude = cooridnates.get(LONGITUDE_POSITION)
            Point point = new GeometryFactory().createPoint(new Coordinate(longitude, latitude))
            point.setUserData(this.sourceCRS)

            if (this.transform) {
                point = JTS.transform(point, transform)
            }

            // If targetCRS is DefaultProjectedCRS then transform doesn't change lng, lat to lat, lng so check if it should be changed
            if (this.targetCRS instanceof DefaultProjectedCRS && this.targetCRS?.baseCRS?.coordinateSystem?.getAxis(0)?.direction == AxisDirection.NORTH) {
                point = new GeometryFactory().createPoint(new Coordinate(point.getCoordinate().y, point.getCoordinate().x))
            }

            point.setUserData(this.targetCRS)

            GeometryDescriptor geometryDescriptor = featureType.getGeometryDescriptor()
            if (this.targetCRS != this.sourceCRS) {
                ComplexFeatureTypeFactoryImpl complexFeatureTypeFactory = new ComplexFeatureTypeFactoryImpl()
                NameImpl pointName = featureType.getGeometryDescriptor().getName()
                GeometryType geometryType = complexFeatureTypeFactory.createGeometryType(pointName, org.opengis.geometry.primitive.Point.class, this.targetCRS, true, false, new ArrayList(), null, null)
                geometryDescriptor = complexFeatureTypeFactory.createGeometryDescriptor(geometryType, pointName, 1, 1, false, "")
            }
            simpleFeatureBuilder.set(new NameImpl(namespace, this.mapping.geometry.name), new GeometryAttributeImpl(point, geometryDescriptor, null))
        }
    }

    @Override
    SimpleFeatureIterator features() {
        return new MongoDBFeatureIterator(this.mongoCursor, this.featuresList)
    }

    @Override
    SimpleFeatureType getSchema() {
        return this.featureType
    }

    @Override
    String getID() {
        return null
    }

    @Override
    void accepts(FeatureVisitor visitor, ProgressListener progress) throws IOException {
    }

    @Override
    SimpleFeatureCollection subCollection(Filter filter) {
    }

    @Override
    SimpleFeatureCollection sort(SortBy order) {
        return null
    }

    @Override
    ReferencedEnvelope getBounds() {
        return this.mongoDBFeatureSource.getBounds(query)
    }

    @Override
    boolean contains(Object o) {
        return this.featuresList.contains(o)
    }

    @Override
    boolean containsAll(Collection<?> o) {
        return this.featuresList.containsAll(o)
    }

    @Override
    Object[] toArray() {
        return this.featuresList.toArray()
    }

    @Override
    def <O> O[] toArray(O[] a) {
        a = this.featuresList.toArray()
        return a
    }
}
