package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBObject
import com.mongodb.client.FindIterable
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.bson.Document
import org.geotools.data.Query
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.type.FeatureType

import java.util.logging.Logger

class MongoDBSubCollectionFeatureCollection extends AbstractMongoDBFeatureCollection {

    private static final Logger log = Logging.getLogger(MongoDBSubCollectionFeatureCollection.class.getPackage().getName())

    MongoDBSubCollectionFeatureCollection(FindIterable<Document> findIterable, FeatureType featureType, BasicDBObject mapping, Query query, MongoDBFeatureSource mongoDBFeatureSource) {
        super(findIterable, featureType, mapping, query, mongoDBFeatureSource)
    }

    void initFeaturesList() {
        int offsetSkipped = 0
        int offsetTarget = this.offset ?: 0

        while(this.mongoCursor.hasNext() && (this.max == null || this.featuresListDirect.size() < this.max)) {
            Document dbObject = this.mongoCursor.next()

            List<Feature> features = buildFeaturesFromDocument(dbObject)
            for (Feature feature : features) {
                if (this.filter == null || this.filter.evaluate(feature)) {
                    if (offsetSkipped < offsetTarget) {
                        offsetSkipped++
                    } else if (this.max == null || this.featuresListDirect.size() < this.max) {
                        this.featuresListDirect.add(feature)
                    } else {
                        break
                    }
                }
            }
        }
    }

    @Override
    List<SimpleFeature> buildFeaturesFromDocument(Document dbObject) {
        return (getFeatures([:], dbObject, this.mapping) - null) as List<SimpleFeature>
    }

    private List<Feature> getFeatures(Map attributes, Document dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, List<Integer> indices = null) {
        if(objectMapping.attributes.any { it.stringValue } ) {
            return getStringValueFeatures(attributes, dbObject, objectMapping, subCollectionObject)
        } else if(objectMapping.attributes.any { it.useKey || it.useValue }) {
            return getUseKeyOrUseValueFeatures(attributes, dbObject, objectMapping, subCollectionObject, indices)

        } else if(objectMapping.attributes.any { it.useObjectKey }) {
            return getUseObjectKeyFeatures(attributes, dbObject, objectMapping, subCollectionObject, indices)
        } else {
            objectMapping.attributes.each { attributeMapping ->
                attributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject as Document, indices))
            }
        }

        if(objectMapping.subCollections) {
            return getSubcollectionFeatures(objectMapping.subCollections, attributes, dbObject, subCollectionObject as Document, indices)
        } else {
            return [buildFromAttributes(attributes, dbObject)]
        }
    }

    private List<Feature> getSubcollectionFeatures(def subCollections, Map attributes, Document dbObject, Document subCollectionObject = null, List<Integer> indices = null) {
        return subCollections.collect { subCollectionMapping ->
            def subCollectionFromObject = (subCollectionObject ? getObjectFromPath(subCollectionObject, subCollectionMapping.subCollectionPath) : getObjectFromPath(dbObject, subCollectionMapping.subCollectionPath))
            if(subCollectionFromObject instanceof List) {
                List<Feature> features = []
                if(indices == null) {
                    indices = []
                }
                subCollectionFromObject.eachWithIndex { subCollection, idx ->
                    features.addAll(getFeatures(attributes, dbObject, subCollectionMapping, subCollection, indices.collect() + idx))
                }
                return features
            } else if(subCollectionFromObject != null) {
                return getFeatures(attributes, dbObject, subCollectionMapping, subCollectionFromObject)
            }
            return []

        }.flatten()
    }

    private List<Feature> getStringValueFeatures(Map attributes, Document dbObject, BasicDBObject objectMapping, Object subCollectionObject = null) {
        def stringValueAttr = objectMapping.attributes.find { it.stringValue }
        if(subCollectionObject instanceof String) {
            def clonedAttributes = attributes.clone()
            clonedAttributes.put(stringValueAttr.name, subCollectionObject)
            return [buildFromAttributes(clonedAttributes, dbObject)]
        }
        return []
    }

    private List<Feature> getUseKeyOrUseValueFeatures(Map attributes, Document dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, List<Integer> indices = null) {
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
                        clonedAttributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject as Document, indices))
                }
                if (objectMapping.subCollections) {
                    return getSubcollectionFeatures(objectMapping.subCollections, clonedAttributes, dbObject, subCollectionObject as Document)
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

    private List<Feature> getUseObjectKeyFeatures(Map attributes, Document dbObject, BasicDBObject objectMapping, Object subCollectionObject = null, List<Integer> indices = null) {
        if(!(subCollectionObject ?: dbObject).any { key, value -> value instanceof Document}) {
            return []
        }
        def userObjectKeyAttr = objectMapping.attributes.find { it.useObjectKey }
        return (subCollectionObject ?: dbObject).collect { def key, def value ->
            if(value instanceof Document) {

                def clonedAttributes = attributes.clone()
                clonedAttributes.put(userObjectKeyAttr.name, key)

                objectMapping.attributes.each { attributeMapping ->
                    if(attributeMapping != userObjectKeyAttr)
                        clonedAttributes.put(attributeMapping.name, getAttributeValueFromDBObject(dbObject, attributeMapping, subCollectionObject as Document, indices))
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
