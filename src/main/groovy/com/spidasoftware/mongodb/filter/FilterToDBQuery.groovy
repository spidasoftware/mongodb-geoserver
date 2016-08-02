package com.spidasoftware.mongodb.filter

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import com.spidasoftware.mongodb.feature.collection.MongoDBFeatureCollection
import com.spidasoftware.mongodb.feature.collection.MongoDBSubCollectionFeatureCollection
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.feature.FeatureCollection
import org.geotools.util.Converters
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import org.opengis.filter.And
import org.opengis.filter.BinaryComparisonOperator
import org.opengis.filter.ExcludeFilter
import org.opengis.filter.Filter
import org.opengis.filter.FilterVisitor
import org.opengis.filter.Id
import org.opengis.filter.IncludeFilter
import org.opengis.filter.Not
import org.opengis.filter.Or
import org.opengis.filter.PropertyIsBetween
import org.opengis.filter.PropertyIsEqualTo
import org.opengis.filter.PropertyIsGreaterThan
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo
import org.opengis.filter.PropertyIsLessThan
import org.opengis.filter.PropertyIsLessThanOrEqualTo
import org.opengis.filter.PropertyIsLike
import org.opengis.filter.PropertyIsNil
import org.opengis.filter.PropertyIsNotEqualTo
import org.opengis.filter.PropertyIsNull
import org.opengis.filter.expression.Add
import org.opengis.filter.expression.Divide
import org.opengis.filter.expression.ExpressionVisitor
import org.opengis.filter.expression.Function
import org.opengis.filter.expression.Literal
import org.opengis.filter.expression.Multiply
import org.opengis.filter.expression.NilExpression
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.expression.Subtract
import org.opengis.filter.spatial.BBOX
import org.opengis.filter.spatial.Beyond
import org.opengis.filter.spatial.Contains
import org.opengis.filter.spatial.Crosses
import org.opengis.filter.spatial.DWithin
import org.opengis.filter.spatial.Disjoint
import org.opengis.filter.spatial.Equals
import org.opengis.filter.spatial.Intersects
import org.opengis.filter.spatial.Overlaps
import org.opengis.filter.spatial.Touches
import org.opengis.filter.spatial.Within
import org.opengis.filter.temporal.After
import org.opengis.filter.temporal.AnyInteracts
import org.opengis.filter.temporal.Before
import org.opengis.filter.temporal.Begins
import org.opengis.filter.temporal.BegunBy
import org.opengis.filter.temporal.During
import org.opengis.filter.temporal.EndedBy
import org.opengis.filter.temporal.Ends
import org.opengis.filter.temporal.Meets
import org.opengis.filter.temporal.MetBy
import org.opengis.filter.temporal.OverlappedBy
import org.opengis.filter.temporal.TContains
import org.opengis.filter.temporal.TEquals
import org.opengis.filter.temporal.TOverlaps
import org.opengis.geometry.BoundingBox

import java.util.logging.Level
import java.util.logging.Logger
import java.util.regex.Pattern


class FilterToDBQuery implements FilterVisitor, ExpressionVisitor {

    DBCollection dbCollection
    FeatureType featureType
    BasicDBObject mapping
    MongoDBFeatureSource mongoDBFeatureSource

    private static final Logger log = Logging.getLogger(FilterToDBQuery.class.getPackage().getName())

     FilterToDBQuery(DBCollection dbCollection, FeatureType featureType, BasicDBObject mapping, MongoDBFeatureSource mongoDBFeatureSource) {
        this.dbCollection = dbCollection
        this.featureType = featureType
        this.mapping = mapping
        this.mongoDBFeatureSource = mongoDBFeatureSource
        this.fillTypeMaps(this.mapping)
    }

    List doubleQueryKeys = []
    List longQueryKeys = []
    List booleanQueryKeys = []
    List concatenateQueryNames = []
    List useKeyQueryNames = []
    List useValueQueryNames = []
    List stringValueQueryNames = []
    List allQueryPaths = []

    private void fillTypeMaps(BasicDBObject map, String subCollectionPath = "") {
        map.attributes.each { attr ->
            String path = attr.path
            if(attr.path == null) {
                path = "${subCollectionPath ? subCollectionPath + '.' : ''}${attr.subCollectionPath}"
            }

            allQueryPaths << path

            if(attr.class == "Double") {
                doubleQueryKeys << path
            } else if(attr.class == "Long") {
                doubleQueryKeys << path
            } else if(attr.class == "Boolean") {
                booleanQueryKeys << path
            } else if(attr.concatenate) {
                concatenateQueryNames << attr.name
            } else if(attr.useKey || attr.useObjectKey) {
                useKeyQueryNames << attr.name
            } else if(attr.useValue) {
                useValueQueryNames << attr.name
            } else if(attr.stringValue) {
                stringValueQueryNames << attr.name
            }
        }

        map.subCollections.each { subCollection ->
            def subPath = "${subCollectionPath}${subCollectionPath ? '.' : ''}${subCollection.subCollectionPath}"
            fillTypeMaps(subCollection, subPath)
        }
    }


    // Only query for objects with geometries and if the object has the sub collection object
    List getDefaultCollectionQueries(BasicDBObject objectMapping, String currentPath = null) {
        List defaultQueries = []
        if(objectMapping.geometry) {
            defaultQueries.add(new BasicDBObject(objectMapping.geometry.path , new BasicDBObject('$exists', true)))
        }
        objectMapping.subCollections?.each { subCollection ->
            if(subCollection.includeInDefaultQuery) {
                defaultQueries.add(new BasicDBObject('$where', "this.${currentPath ? currentPath + '.' : ''}${subCollection.subCollectionPath}.length > 0".toString()))
            }
            subCollection.subCollections.each {
                def nestedQueries = getDefaultCollectionQueries(it, it.subCollectionPath)
                if(nestedQueries.size() > 0) {
                    defaultQueries.addAll(nestedQueries)
                }
            }
        }
        return defaultQueries
    }

    boolean supportsMaxAndOffsetQueries() {
        return mapping.subCollections == null || mapping.subCollections.size() == 0
    }

    FeatureCollection getFeatureCollection(Query query = null) {
        def andQuery = new BasicDBList()
        def defaultCollectionQueries = getDefaultCollectionQueries(mapping)
        def dbQuery = defaultCollectionQueries
        def filter = query?.getFilter() ?: Filter.INCLUDE
        def queryFromFilter
        if(filter != null) {
            queryFromFilter = visit(filter, "")
        }

        if(defaultCollectionQueries.size() > 0 && queryFromFilter != null) {
            andQuery.addAll(defaultCollectionQueries)
            andQuery.add(queryFromFilter)
            dbQuery = new BasicDBObject('$and', andQuery)
        } else if(defaultCollectionQueries.size() == 0 && queryFromFilter != null) {
            dbQuery = queryFromFilter
        }

        if (log.isLoggable(Level.FINE)) {
            log.fine "dbQuery = ${dbQuery}"
        }

        DBCursor dbCursor = this.dbCollection.find(dbQuery)
        if(supportsMaxAndOffsetQueries() &&  query?.getMaxFeatures() != null) {
            dbCursor?.limit(query.getMaxFeatures())
        }

        if (supportsMaxAndOffsetQueries() && query?.getStartIndex() != null) {
            dbCursor?.skip(query.getStartIndex())
        }
        //log.info "dbQuery = ${dbQuery}"
        //log.info "dbCursor.size() = ${dbCursor.size()}"
        if (log.isLoggable(Level.FINE)) {
            log.fine "dbCursor.size() = ${dbCursor.size()}"
        }

        if(mapping.subCollections) {
            return new MongoDBSubCollectionFeatureCollection(dbCursor, this.featureType, this.mapping, query, this.mongoDBFeatureSource)
        } else {
            return new MongoDBFeatureCollection(dbCursor, this.featureType, this.mapping, query, this.mongoDBFeatureSource)
        }
    }

    @Override
    Object visit(Literal expression, Object extraData) {
        def literal = expression.getValue()
        if(literal instanceof Geometry) {
            def coordinates = new BasicDBList()
            coordinates.addAll(((Geometry) literal).coordinates.collect {
                def latLng = new BasicDBList()
                latLng.add(it.x)
                latLng.add(it.y)
                return latLng
            })
            return new BasicDBObject('$polygon', coordinates)
        } else {
            return literal.toString()
        }
    }

    @Override
    Object visit(PropertyName expression, Object extraData) {
        String propertyName = expression.getPropertyName()
        return getDBQueryPathForPropertyName(propertyName, this.mapping)
    }

    String getDBQueryPathForPropertyName(String propertyName, BasicDBObject map, String subCollectionPath = null) {
        def attr =  map.attributes.find {  it.name == propertyName }
        if(attr?.path) {
            return attr.path
        } else if(attr?.subCollectionPath) {
            return "${subCollectionPath}.${attr?.subCollectionPath}"
        } else if(map.geometry?.name == propertyName) {
            return map.geometry.path
        } else if(attr?.concatenate || attr?.useKey || attr?.useValue || attr?.value) {
            return null // Can't query on a constant value or concatenate
        }
        String path = null
        map.subCollections.each { subCollection ->
            def pathFromSubCollection = getDBQueryPathForPropertyName(propertyName, subCollection, "${subCollectionPath ? subCollectionPath + '.' : ''}${subCollection.subCollectionPath}")
            if(pathFromSubCollection) {
                path = pathFromSubCollection
            }
        }
        return path
    }


    @Override
    Object visit(ExcludeFilter filter, Object extraData) { // Shouldn't exist
        BasicDBList andQuery = new BasicDBList()
        andQuery.add(new BasicDBObject("id", "-1"))
        andQuery.add(new BasicDBObject("id", "-2"))
        return new BasicDBObject('$and', andQuery)
    }

    @Override
    Object visit(IncludeFilter filter, Object extraData) {
        return new BasicDBObject() // Should include all
    }

    @Override
    Object visit(And filter, Object extraData) {
        BasicDBList andQuery = new BasicDBList()
        filter.getChildren().each { Filter childFilter ->
            andQuery.add(visit(childFilter, ""))
        }
        return new BasicDBObject('$and', andQuery)
    }

    @Override
    Object visit(Id filter, Object extraData) {
        BasicDBList orQuery = new BasicDBList()
        filter.getIDs()*.toString().each { String id ->
            orQuery.add(getPropertyIsEqualToQuery("id", id))
        }
        if(orQuery.size() == 1) {
            return orQuery.get(0)
        } else {
            return new BasicDBObject('$or', orQuery)
        }
    }

    BasicDBObject getPropertyIsEqualToQuery(String propertyName, String value) {
        String dbQueryPath = getDBQueryPathForPropertyName(propertyName, this.mapping)
        if(dbQueryPath) {
            return new BasicDBObject(propertyName, value)
        }

        if(concatenateQueryNames.contains(propertyName) || useKeyQueryNames.contains(propertyName) || useValueQueryNames.contains(propertyName) || stringValueQueryNames.contains(propertyName)) {
            List<Map> attributeObjects = getAttributeObjects(propertyName, this.mapping)
            BasicDBList orQuery = new BasicDBList()
            attributeObjects.each { attributeObject ->
                BasicDBObject attribute = attributeObject.attribute
                String subCollectionPath = attributeObject.subCollectionPath
                if(attribute.concatenate) {
                    def splitValue = value.split("_")
                    BasicDBList andQuery = new BasicDBList()
                    attribute.concatenate.eachWithIndex { BasicDBObject concatObject, int index ->
                        if (index < splitValue.size()) {
                            if (concatObject.path) {
                                andQuery.add(new BasicDBObject(concatObject.path, splitValue[index]))
                            } else if (concatObject.subCollectionPath) {
                                andQuery.add(new BasicDBObject("${subCollectionPath}.${concatObject.subCollectionPath}", splitValue[index]))
                            } else if (concatObject.currentIndex || concatObject.value) {
                                // Do nothing hardcoded value will be filtered out later
                            }
                        }
                    }
                    if (andQuery.size() == 1) {
                        orQuery << andQuery.get(0)
                    } else {
                        orQuery << new BasicDBObject('$and', andQuery)
                    }
                } else if(attribute.useKey || attribute.useObjectKey) {
                    orQuery <<  new BasicDBObject("${subCollectionPath ? subCollectionPath + '.' : ''}${value}", new BasicDBObject('$exists', true))
                } else if(attribute.useValue) {
                    orQuery << new BasicDBObject()
                    // TODO
                } else if(attribute.stringValue) {
                    orQuery <<  new BasicDBObject(subCollectionPath, value)
                }
            }

            orQuery.unique()

            if(orQuery.size() > 1) {
                return new BasicDBObject('$or', orQuery)
            } else if(orQuery.size() == 1) {
                return orQuery.get(0)
            }
            return new BasicDBObject(propertyName, value)
        }
        return new BasicDBObject()
    }

    List<Map> getAttributeObjects(String propertyName, BasicDBObject map) {
        BasicDBObject attribute =  map.attributes.find {  it.name == propertyName }
        if(attribute != null) {
            return [[subCollectionPath: null, attribute: attribute]]
        }

        List<Map> attributes = []
        map.subCollections.each { subCollection ->
            List<Map> subCollectionAttributes = getAttributeObjects(propertyName, subCollection)
            subCollectionAttributes.each { subCollectionAttribute ->
                subCollectionAttribute.subCollectionPath = "${subCollection.subCollectionPath}${subCollectionAttribute.subCollectionPath ? '.' + subCollectionAttribute.subCollectionPath : ''}".toString()
                attributes << subCollectionAttribute
            }
        }
        attributes.unique()
        return attributes
    }

    /*BasicDBObject getPropertyIsEqualToQuery(String propertName, String id) {
        List<Map> idInfos = getIdInfos(this.mapping)
        log.info "idInfos = ${idInfos}"
        BasicDBList orQuery = new BasicDBList()
        idInfos.each { Map idInfo ->
            if (idInfo?.idMapping?.concatenate) {
                def splitId = id.split("_")
                BasicDBList andQuery = new BasicDBList()
                idInfo.idMapping.concatenate.eachWithIndex { BasicDBObject concatObject, int index ->
                    if (index < splitId.size()) {
                        if (concatObject.path) {
                            andQuery.add(new BasicDBObject(concatObject.path, splitId[index]))
                        } else if (concatObject.subCollectionPath) {
                            andQuery.add(new BasicDBObject("${idInfo.subCollectionPath}.${concatObject.subCollectionPath}", splitId[index]))
                        } else if (concatObject.currentIndex || concatObject.value) {
                            // Do nothing hardcoded value will be filtered out later
                        }
                    }
                }
                if (andQuery.size() == 1) {
                    orQuery << andQuery.get(0)
                } else {
                    orQuery << new BasicDBObject('$and', andQuery)
                }
            } else if (idInfo?.idMapping) {
                orQuery << new BasicDBObject(idInfo?.idMapping.path, id)
            }
        }

        orQuery.unique()

        if(orQuery.size() > 1) {
            return new BasicDBObject('$or', orQuery)
        } else if(orQuery.size() == 1) {
            return orQuery.get(0)
        }
        return new BasicDBObject("id", id)
    }

    private List<Map> getIdInfos(BasicDBObject map) {
        BasicDBObject idMapping =  map.attributes.find {  it.name == "id" }
        if(idMapping != null) {
            return [[subCollectionPath: null, idMapping: idMapping]]
        }

        List<Map> idInfos = []
        map.subCollections.each { subCollection ->
            List<Map> ids = getIdInfos(subCollection)
            ids.each { info ->
                if(info.subCollectionPath != null) {
                    info.subCollectionPath = subCollection.subCollectionPath + info.subCollectionPath
                } else {
                    info.subCollectionPath = subCollection.subCollectionPath
                }
                idInfos << info
            }
        }
        return idInfos
    }*/

    @Override
    Object visit(Not filter, Object extraData) {
        BasicDBObject expr = (BasicDBObject) filter.getFilter().accept(this, null)
        if(filter.getFilter() instanceof PropertyIsEqualTo) {
            def key = expr.keySet().first()
            return new BasicDBObject(key, new BasicDBObject('$ne', expr.get(key)))
        }
        return new BasicDBObject('$not': expr)
    }

    @Override
    Object visit(Or filter, Object extraData) {
        BasicDBList orQuery = new BasicDBList()
        filter.getChildren().each { Filter childFilter ->
            orQuery.add(visit(childFilter, ""))
        }
        return new BasicDBObject('$or', orQuery)
    }

    @Override
    Object visit(PropertyIsBetween filter, Object extraData) {
        String target = Converters.convert(filter.getExpression().accept(this, null), String.class)
        Object lowerbound = convertValueIfNeeded(target, filter.getLowerBoundary().accept(this, null))
        Object upperbound = convertValueIfNeeded(target, filter.getUpperBoundary().accept(this, null))

        return new BasicDBObject(target, new BasicDBObject(['$gte': lowerbound, '$lte': upperbound]))
    }

    @Override
    Object visit(PropertyIsEqualTo filter, Object extraData) {
        return getBinaryComparisonQuery(filter)
    }

    @Override
    Object visit(PropertyIsNotEqualTo filter, Object extraData) {
        return getBinaryComparisonQuery(filter, '$ne')
    }

    @Override
    Object visit(PropertyIsGreaterThan filter, Object extraData) {
        return getBinaryComparisonQuery(filter, '$gt')
    }

    @Override
    Object visit(PropertyIsGreaterThanOrEqualTo filter, Object extraData) {
        return getBinaryComparisonQuery(filter, '$gte')
    }

    @Override
    Object visit(PropertyIsLessThan filter, Object extraData) {
        return getBinaryComparisonQuery(filter, '$lt')
    }

    @Override
    Object visit(PropertyIsLessThanOrEqualTo filter, Object extraData) {
        return getBinaryComparisonQuery(filter, '$lte')
    }

    protected BasicDBObject getBinaryComparisonQuery(BinaryComparisonOperator filter, String mongoOperator = null) {
         def propertyName = filter.getExpression1().accept(this, null)
        def value = filter.getExpression2().accept(this, null)
        // can be 2 < actual or it can be actual < 2, find when 2 < actual format and reverse
        if (!allQueryPaths.contains(propertyName) && allQueryPaths.contains(value)) {
            def tmp = propertyName
            propertyName = value
            value = tmp
        }

        if(propertyName == null) {
            if(filter.getExpression1().respondsTo("getPropertyName") && filter.getExpression1().getPropertyName() != null && mongoOperator == null) {
                return getPropertyIsEqualToQuery(filter.getExpression1().getPropertyName(), filter.getExpression2().accept(this, null))
            } else {
                return new BasicDBObject() // Can't be queried, will be filtered after found
            }
        }

        value = convertValueIfNeeded(propertyName, value)
        def propQuery = mongoOperator == null ? value : new BasicDBObject(mongoOperator, value)

        return new BasicDBObject(propertyName, propQuery)
    }

    protected def convertValueIfNeeded(String dbQueryPath, String valueString) {
        def convertedValue = valueString // TODO
        if(doubleQueryKeys.contains(dbQueryPath)) {
            try {
                convertedValue = valueString.toDouble()
            } catch(e) {
                if(log.isLoggable(Level.FINE)) {
                    log.fine "error converting ${valueString} to double"
                }
            }
        } else if(longQueryKeys.contains(dbQueryPath)) {
            try {
                convertedValue = valueString.toLong()
            } catch (e) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine "error converting ${valueString} to long"
                }
            }
        } else if(booleanQueryKeys.contains(dbQueryPath)) {
            try {
                convertedValue = valueString.toBoolean()
            } catch (e) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine "error converting ${valueString} to boolean"
                }
            }
        }
        return convertedValue
    }

    @Override
    Object visit(PropertyIsLike filter, Object extraData) {
        String propertyName = filter.getExpression().accept(this, null)

        String regex = '^' + filter.getLiteral().replace(filter.getWildCard(), ".*").replace(filter.getSingleChar(), ".") + '$'

        Pattern pattern = Pattern.compile(regex, (filter.isMatchingCase() ? 0 : Pattern.CASE_INSENSITIVE))
        return new BasicDBObject(propertyName, pattern)
    }

    @Override
    Object visit(PropertyIsNull filter, Object extraData) {
        String propertyName = filter.accept(this, null)
        return new BasicDBObject(propertyName, new BasicDBObject('$exists', false))
    }

    @Override
    Object visit(PropertyIsNil filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(BBOX filter, Object extraData) { // TODO: test this
        BoundingBox boundingBox = filter.getBounds()
        List bottomLeft = [boundingBox.getMinX(), boundingBox.getMinY()]
        List topRight = [ boundingBox.getMaxX(), boundingBox.getMaxY()]
        List box = [bottomLeft, topRight]
        return new BasicDBObject(filter.getExpression1().accept(this, null), new BasicDBObject('$geoWithin', new BasicDBObject('$box', box)))
    }

    @Override
    Object visit(Beyond filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Contains filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Crosses filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Disjoint filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(DWithin filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Equals filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Intersects filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Overlaps filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Touches filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Within filter, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(After after, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(AnyInteracts anyInteracts, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Before before, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Begins begins, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(BegunBy begunBy, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(During during, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(EndedBy endedBy, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Ends ends, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Meets meets, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(MetBy metBy, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(OverlappedBy overlappedBy, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(TContains contains, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(TEquals equals, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(TOverlaps contains, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Multiply expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(NilExpression expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Add expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Divide expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Function expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visit(Subtract expression, Object extraData) {
        throw new UnsupportedOperationException()
    }

    @Override
    Object visitNullFilter(Object extraData) {
        throw new UnsupportedOperationException()
    }
}
