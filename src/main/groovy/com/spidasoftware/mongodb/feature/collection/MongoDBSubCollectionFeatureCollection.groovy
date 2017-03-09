package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.bson.types.BasicBSONList
import org.geotools.data.Query
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType

import java.util.logging.Logger

class MongoDBSubCollectionFeatureCollection extends AbstractMongoDBFeatureCollection {

    private static final Logger log = Logging.getLogger(MongoDBSubCollectionFeatureCollection.class.getPackage().getName())

    MongoDBSubCollectionFeatureCollection(DBCursor dbCursor, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
        super(dbCursor, featureType, mapping, query, mongoDBFeatureSource)
    }

    void initFeaturesList() {
        int offsetSkipped = 0
        while(this.dbCursor.hasNext() && (this.max == null || this.featuresList.size() < this.max)) {
            DBObject dbObject =  this.dbCursor.next()
            List<Feature> features = getFeatures([:], dbObject, this.mapping) - null
            features.each { Feature feature ->
                if (this.filter == null || this.filter.evaluate(feature)) {
                    if(offsetSkipped < this.offset) {
                        offsetSkipped++
                    } else if(this.max == null || this.featuresList.size() < this.max) {
                        this.featuresList.add(feature)
                    }
                }
            }
        }
    }

    private List<Feature> getFeatures(Map attributes, DBObject dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, Integer index = null) {
        if(objectMapping.attributes.any { it.stringValue } ) {
            return getStringValueFeatures(attributes, dbObject, objectMapping, subCollectionObject, index)
        } else if(objectMapping.attributes.any { it.useKey || it.useValue }) {
            return getUseKeyOrUseValueFeatures(attributes, dbObject, objectMapping, subCollectionObject, index)

        } else if(objectMapping.attributes.any { it.useObjectKey }) {
            return getUseObjectKeyFeatures(attributes, dbObject, objectMapping, subCollectionObject, index)
        } else {
            objectMapping.attributes.each { attributeMapping ->
                attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject, index))
            }
        }

        if(objectMapping.subCollections) {
            return getSubcollectionFeatures(objectMapping.subCollections, attributes, dbObject, subCollectionObject)
        } else {
            return [buildFromAttributes(attributes, dbObject)]
        }
    }

    private List<Feature> getSubcollectionFeatures(def subCollections, Map attributes, DBObject dbObject, DBObject subCollectionObject = null) {
        return  subCollections.collect { subCollectionMapping ->
            DBObject subCollectionFromObject = (subCollectionObject ? getObjectFromPath(subCollectionObject, subCollectionMapping.subCollectionPath) : getObjectFromPath(dbObject, subCollectionMapping.subCollectionPath))
            if(subCollectionFromObject instanceof BasicBSONList) {
                List<Feature> features = []
                subCollectionFromObject.eachWithIndex { subCollection, idx ->
                    features.addAll(getFeatures(attributes, dbObject, subCollectionMapping, subCollection, idx))
                }
                return features
            } else {
                return getFeatures(attributes, dbObject, subCollectionMapping, subCollectionFromObject)
            }

        }.flatten()
    }

    private List<Feature> getStringValueFeatures(Map attributes, DBObject dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, Integer index = null) {
        def stringValueAttr = objectMapping.attributes.find { it.stringValue }
        if(subCollectionObject instanceof String) {
            def clonedAttributes = attributes.clone()
            clonedAttributes.put(stringValueAttr.name, subCollectionObject)
            return [buildFromAttributes(clonedAttributes, dbObject)]
        }
        return []
    }

    private List<Feature> getUseKeyOrUseValueFeatures(Map attributes, DBObject dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, Integer index = null) {
        def keyAttr = objectMapping.attributes.find { it.useKey }
        def valueAttr = objectMapping.attributes.find { it.useValue }

        def getKeyAndValueFeatures = { def key, def value ->
            if (value instanceof String) {
                def clonedAttributes = attributes.clone()
                if (keyAttr) {
                    clonedAttributes.put(keyAttr.name, key)
                }
                if (valueAttr) {
                    clonedAttributes.put(valueAttr.name, value)
                }
                objectMapping.attributes.each { attributeMapping ->
                    if (attributeMapping != keyAttr && attributeMapping != valueAttr)
                        clonedAttributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject, index))
                }
                if (objectMapping.subCollections) {
                    return getSubcollectionFeatures(objectMapping.subCollections, clonedAttributes, dbObject, subCollectionObject)
                } else {
                    return [buildFromAttributes(clonedAttributes, dbObject)]
                }

            }
            return []
        }

        // Handle a list of strings
        if(subCollectionObject instanceof String) {
            return getKeyAndValueFeatures(null, subCollectionObject).flatten()
        } else {
            return (subCollectionObject ?: dbObject).collect { def key, value ->
                return getKeyAndValueFeatures(key, value)
            }.flatten()
        }
    }

    private List<Feature> getUseObjectKeyFeatures(Map attributes, DBObject dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, Integer index = null) {
        if(!(subCollectionObject ?: dbObject).any { key, value -> value instanceof BasicDBObject}) {
            return []
        }
        def userObjectKeyAttr = objectMapping.attributes.find { it.useObjectKey }
        return (subCollectionObject ?: dbObject).collect { def key, def value ->
            if(value instanceof BasicDBObject) {

                def clonedAttributes = attributes.clone()
                clonedAttributes.put(userObjectKeyAttr.name, key)

                objectMapping.attributes.each { attributeMapping ->
                    if(attributeMapping != userObjectKeyAttr)
                        clonedAttributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject, index))
                }
                if(objectMapping.subCollections) {
                    return getSubcollectionFeatures(objectMapping.subCollections, clonedAttributes, dbObject, value)
                } else {
                    return [buildFromAttributes(clonedAttributes, dbObject)]
                }
            } else {
                return []
            }
        }
    }
}
