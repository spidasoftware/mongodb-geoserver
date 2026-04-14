package com.spidasoftware.mongodb

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import groovy.json.JsonSlurper
import org.bson.Document

/**
 * Shared utility class for parsing JSON resources in tests.
 */
class TestJsonUtils {

    static BasicDBList parseJsonResource(Class<?> resourceClass, String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(resourceClass.getResourceAsStream(resourcePath))
        return convertToBasicDBList(parsed)
    }

    static Document parseJsonResourceAsDocument(Class<?> resourceClass, String resourcePath) {
        def jsonSlurper = new JsonSlurper()
        def parsed = jsonSlurper.parse(resourceClass.getResourceAsStream(resourcePath))
        return new Document(convertToBasicDBObject(parsed))
    }

    static Object convertValue(Object value) {
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue()
        } else if (value instanceof Map) {
            return convertToBasicDBObject(value)
        } else if (value instanceof List) {
            return convertToBasicDBList(value)
        }
        return value
    }

    static BasicDBList convertToBasicDBList(List list) {
        BasicDBList dbList = new BasicDBList()
        list.each { item ->
            dbList.add(convertValue(item))
        }
        return dbList
    }

    static BasicDBObject convertToBasicDBObject(Map map) {
        BasicDBObject dbObject = new BasicDBObject()
        map.each { key, value ->
            dbObject.put(key, convertValue(value))
        }
        return dbObject
    }
}
