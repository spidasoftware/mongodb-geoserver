package com.spidasoftware.mongodb.filter

import com.mongodb.*
import com.mongodb.util.JSON
import com.spidasoftware.mongodb.data.MongoDBDataAccess
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import java.util.regex.Pattern
import org.geotools.data.Query
import org.geotools.feature.FeatureCollection
import org.geotools.feature.FeatureIterator
import org.geotools.feature.NameImpl
import org.geotools.filter.FilterFactoryImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import org.opengis.filter.Filter
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.logging.Logger

class FilterToDBQuerySpec extends Specification {

    static final Logger log = Logging.getLogger(FilterToDBQuerySpec.class.getPackage().getName())

    @Shared DB database
    @Shared BasicDBObject locationJSON
    @Shared BasicDBObject designJSON
    @Shared BasicDBList jsonMapping
    @Shared MongoDBDataAccess mongoDBDataAccess
    @Shared String namespace = "http://spida/db"
    @Shared MongoDBFeatureSource mongoDBFeatureSource

    void setupSpec() {
        designJSON = JSON.parse(getClass().getResourceAsStream('/design.json').text)
        locationJSON = JSON.parse(getClass().getResourceAsStream('/location.json').text)

        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)

        String host = System.getProperty("mongoHost")
        String port = System.getProperty("mongoPort")
        String databaseName = System.getProperty("mongoDatabase")
        def serverAddress = new ServerAddress(host, Integer.valueOf(port))
        MongoClient mongoClient = new MongoClient(serverAddress)
        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        mongoDBDataAccess = new MongoDBDataAccess(namespace, host, port, databaseName, null, null, jsonMapping)
        database = mongoClient.getDB(databaseName)

        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("locations").insert(locationJSON)

        database.getCollection("designs").remove(new BasicDBObject("id", designJSON.get("id")))
        database.getCollection("designs").insert(designJSON)
    }

    void cleanupSpec() {
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("designs").remove(new BasicDBObject("id", designJSON.get("id")))
    }

    @Unroll("test get #typeName Features no query or filter")
    void "test getFeatures no query or filter"() {
        setup:
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection()
        then:
            featureCollection.size() == expectedSize
        where:
            typeName       | collectionName | expectedSize
            "location"     | "locations"    | 1
            "poleTag"      | "locations"    | 3
            "remedy"       | "locations"    | 1
            "summaryNote"  | "locations"    | 4
            "form"         | "locations"    | 2
            "formField"    | "locations"    | 2
            "pole"         | "designs"      | 1
            "analysis"     | "designs"      | 6
            "wire"         | "designs"      | 2
            "spanPoint"    | "designs"      | 1
            "spanGuy"      | "designs"      | 1
            "guy"          | "designs"      | 1
            "insulator"    | "designs"      | 1
            "equipment"    | "designs"      | 1
            "damage"       | "designs"      | 1
            "crossArm"     | "designs"      | 1
            "anchor"       | "designs"      | 1
            "wireEndPoint" | "designs"      | 1
            "notePoint"    | "designs"      | 1
            "pointLoad"    | "designs"      | 1
    }

    @Unroll("test include filter for #typeName")
    void "test include filter"() {
        setup:
            Query query = new Query(collectionName, Filter.INCLUDE)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(query.getFilter(), null)
        then:
            dbQuery == new BasicDBObject()
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            typeName       | collectionName | expectedSize
            "location"     | "locations"    | 1
            "poleTag"      | "locations"    | 3
            "remedy"       | "locations"    | 1
            "summaryNote"  | "locations"    | 4
            "form"         | "locations"    | 2
            "formField"    | "locations"    | 2
            "pole"         | "designs"      | 1
            "analysis"     | "designs"      | 6
            "wire"         | "designs"      | 2
            "spanPoint"    | "designs"      | 1
            "spanGuy"      | "designs"      | 1
            "guy"          | "designs"      | 1
            "insulator"    | "designs"      | 1
            "equipment"    | "designs"      | 1
            "damage"       | "designs"      | 1
            "crossArm"     | "designs"      | 1
            "anchor"       | "designs"      | 1
            "wireEndPoint" | "designs"      | 1
            "notePoint"    | "designs"      | 1
            "pointLoad"    | "designs"      | 1
    }

    @Unroll("test exclude filter for #typeName")
    void "test exclude filter"() {
        setup:
            Query query = new Query(collectionName, Filter.EXCLUDE)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(query.getFilter(), null)
        then:
            dbQuery == new BasicDBObject('$and', JSON.parse('[{"id": "-1"}, {"id": "-2"}]'))
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == 0
        where:
            typeName       | collectionName
            "location"     | "locations"
            "poleTag"      | "locations"
            "remedy"       | "locations"
            "summaryNote"  | "locations"
            "form"         | "locations"
            "formField"    | "locations"
            "pole"         | "designs"
            "analysis"     | "designs"
            "wire"         | "designs"
            "spanPoint"    | "designs"
            "spanGuy"      | "designs"
            "guy"          | "designs"
            "insulator"    | "designs"
            "equipment"    | "designs"
            "damage"       | "designs"
            "crossArm"     | "designs"
            "anchor"       | "designs"
            "wireEndPoint" | "designs"
            "notePoint"    | "designs"
            "pointLoad"    | "designs"
    }

    @Unroll("test Id query typeName=#typeName collectionName=#collectionName id=#id")
    void "test Id query"() {
        setup:
            Query query = new Query(typeName, new FilterFactoryImpl().id([new FeatureIdImpl(id)] as Set))
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(query.getFilter(), null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            typeName       | collectionName | id                                                                   | expectedQuery                                                                                                                                             | expectedSize
            "location"     | "locations"    | "55fac7fde4b0e7f2e3be342c"                                           | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")                                                                                                       | 1
            "poleTag"      | "locations"    | "55fac7fde4b0e7f2e3be342c_MAP"                                       | new BasicDBObject('$and', JSON.parse('[{"id":"55fac7fde4b0e7f2e3be342c"}, {"calcLocation.poleTags.type":"MAP"}]'))                                        | 1
            "form"         | "locations"    | "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form" | new BasicDBObject('$and', JSON.parse('[{"id":"55fac7fde4b0e7f2e3be342c"}, {"calcLocation.forms.template":"6ee5fba14760878be22701e1b3b7c05b-HTA Form"}]')) | 1
            "pole"         | "designs"      | "56e9b7137d84511d8dd0f13c"                                           | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                                                                       | 1
            "analysis"     | "designs"      | "56e9b7137d84511d8dd0f13c_ANALYSIS_0_0"                                | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                                                                       | 1
            "wire"         | "designs"      | "56e9b7137d84511d8dd0f13c_Wire#1"                                    | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.wires.id":"Wire#1"}]'))                                  | 1
            "spanPoint"    | "designs"      | "56e9b7137d84511d8dd0f13c_SpanPoint#1"                               | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.spanPoints.id":"SpanPoint#1"}]'))                        | 1
            "spanGuy"      | "designs"      | "56e9b7137d84511d8dd0f13c_SpanGuy#1"                                 | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.spanGuys.id":"SpanGuy#1"}]'))                            | 1
            "guy"          | "designs"      | "56e9b7137d84511d8dd0f13c_Guy#1"                                     | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.guys.id":"Guy#1"}]'))                                    | 1
            "insulator"    | "designs"      | "56e9b7137d84511d8dd0f13c_Insulator#1"                               | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.insulators.id":"Insulator#1"}]'))                        | 1
            "equipment"    | "designs"      | "56e9b7137d84511d8dd0f13c_Equip#1"                                   | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.equipments.id":"Equip#1"}]'))                            | 1
            "damage"       | "designs"      | "56e9b7137d84511d8dd0f13c_Damage#1"                                  | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.damages.id":"Damage#1"}]'))                              | 1
            "crossArm"     | "designs"      | "56e9b7137d84511d8dd0f13c_CrossArm#1"                                | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.crossArms.id":"CrossArm#1"}]'))                          | 1
            "anchor"       | "designs"      | "56e9b7137d84511d8dd0f13c_Anchor#1"                                  | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.anchors.id":"Anchor#1"}]'))                              | 1
            "wireEndPoint" | "designs"      | "56e9b7137d84511d8dd0f13c_WEP#1"                                     | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.wireEndPoints.id":"WEP#1"}]'))                           | 1
            "notePoint"    | "designs"      | "56e9b7137d84511d8dd0f13c_NotePoint#1"                               | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.notePoints.id":"NotePoint#1"}]'))                        | 1
            "pointLoad"    | "designs"      | "56e9b7137d84511d8dd0f13c_PointLoad#1"                               | new BasicDBObject('$and', JSON.parse('[{"id":"56e9b7137d84511d8dd0f13c"}, {"calcDesign.structure.pointLoads.id":"PointLoad#1"}]'))                        | 1
    }

    @Unroll("Test location property query for #description")
    void "test location property queries"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                            | filter                                                                        | expectedQuery                                                                                    | expectedSize
            "id that exists"                       | CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'")                                 | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")                                              | 1
            "id that doesn't exist"                | CQL.toFilter("id='TEST'")                                                     | new BasicDBObject("id", "TEST")                                                                  | 0
            "label that exists"                    | CQL.toFilter("label='684704E'")                                               | new BasicDBObject("calcLocation.label", "684704E")                                               | 1
            "label that doesn't exist"             | CQL.toFilter("label='TEST'")                                                  | new BasicDBObject("calcLocation.label", "TEST")                                                  | 0
            "projectId that exists"                | CQL.toFilter("projectId='55fac7fde4b0e7f2e3be344f'")                          | new BasicDBObject("projectId", "55fac7fde4b0e7f2e3be344f")                                       | 1
            "projectId that doesn't exist"         | CQL.toFilter("projectId='TEST'")                                              | new BasicDBObject("projectId", "TEST")                                                           | 0
            "projectName that exists"              | CQL.toFilter("projectName='IJUS-44-2015-08-26-053'")                          | new BasicDBObject("projectLabel", "IJUS-44-2015-08-26-053")                                      | 1
            "projectName that doesn't exist"       | CQL.toFilter("projectName='TEST'")                                            | new BasicDBObject("projectLabel", "TEST")                                                        | 0
            "dateModified that exists"             | CQL.toFilter("dateModified=1442498557079")                                    | new BasicDBObject("dateModified", 1442498557079)                                                 | 1
            "dateModified that doesn't exist"      | CQL.toFilter("dateModified=3333333")                                          | new BasicDBObject("dateModified", 3333333)                                                       | 0
            "dateModified gt exists"               | CQL.toFilter("dateModified>1442498557078")                                    | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557078))                       | 1
            "dateModified gt doesn't exist"        | CQL.toFilter("dateModified>1442498557080")                                    | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557080))                       | 0
            "dateModified gte exists"              | CQL.toFilter("dateModified>=1442498557079")                                   | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557079))                      | 1
            "dateModified gte doesn't exist"       | CQL.toFilter("dateModified>=1442498557080")                                   | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557080))                      | 0
            "dateModified lt exists"               | CQL.toFilter("dateModified<1442498557080")                                    | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557080))                       | 1
            "dateModified lt doesn't exist"        | CQL.toFilter("dateModified<1442498557079")                                    | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557079))                       | 0
            "dateModified lte exists"              | CQL.toFilter("dateModified<=1442498557079")                                   | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557079))                      | 1
            "dateModified lte doesn't exist"       | CQL.toFilter("dateModified<=1442498557078")                                   | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557078))                      | 0
            "clientFile that exists"               | CQL.toFilter("clientFile='SCE.client'")                                       | new BasicDBObject("clientFile", "SCE.client")                                                    | 1
            "clientFile that doesn't exist"        | CQL.toFilter("clientFile='AEP.client'")                                       | new BasicDBObject("clientFile", "AEP.client")                                                    | 0
            "clientFileVersion that exists"        | CQL.toFilter("clientFileVersion='6ee5fba14760878be22701e1b3b7c05b'")          | new BasicDBObject("clientFileVersion", "6ee5fba14760878be22701e1b3b7c05b")                       | 1
            "clientFileVersion that doesn't exist" | CQL.toFilter("clientFileVersion='TEST'")                                      | new BasicDBObject("clientFileVersion", "TEST")                                                   | 0
            "mapNumber that exists"                | CQL.toFilter("mapNumber='ROME AVE.'")                                         | new BasicDBObject("calcLocation.mapNumber", "ROME AVE.")                                         | 1
            "mapNumber that doesn't exist"         | CQL.toFilter("mapNumber='TEST'")                                              | new BasicDBObject("calcLocation.mapNumber", "TEST")                                              | 0
            "comments that exist"                  | CQL.toFilter("comments='Two transformers connected to lower two cross arms'") | new BasicDBObject("calcLocation.comments", "Two transformers connected to lower two cross arms") | 1
            "comments that doesn't exist"          | CQL.toFilter("comments='TEST'")                                               | new BasicDBObject("calcLocation.comments", "TEST")                                               | 0
            "streetNumber that exists"             | CQL.toFilter("streetNumber='8812'")                                           | new BasicDBObject("calcLocation.address.number", "8812")                                         | 1
            "streetNumber that doesn't exist"      | CQL.toFilter("streetNumber='123'")                                            | new BasicDBObject("calcLocation.address.number", "123")                                          | 0
            "street that exists"                   | CQL.toFilter("street='Eberhart Rd NW'")                                       | new BasicDBObject("calcLocation.address.street", "Eberhart Rd NW")                               | 1
            "street that doesn't exist"            | CQL.toFilter("street='Test Rd'")                                              | new BasicDBObject("calcLocation.address.street", "Test Rd")                                      | 0
            "city that exists"                     | CQL.toFilter("city='Bolivar'")                                                | new BasicDBObject("calcLocation.address.city", "Bolivar")                                        | 1
            "city that doesn't exist"              | CQL.toFilter("city='Columbus'")                                               | new BasicDBObject("calcLocation.address.city", "Columbus")                                       | 0
            "county that exists"                   | CQL.toFilter("county='Tuscarawas'")                                           | new BasicDBObject("calcLocation.address.county", "Tuscarawas")                                   | 1
            "county that doesn't exist"            | CQL.toFilter("county='Franklin'")                                             | new BasicDBObject("calcLocation.address.county", "Franklin")                                     | 0
            "state that exists"                    | CQL.toFilter("state='OH'")                                                    | new BasicDBObject("calcLocation.address.state", "OH")                                            | 1
            "state that doesn't exist"             | CQL.toFilter("state='MI'")                                                    | new BasicDBObject("calcLocation.address.state", "MI")                                            | 0
            "zip_code that exists"                 | CQL.toFilter("zipCode='44622'")                                               | new BasicDBObject("calcLocation.address.zip_code", "44622")                                      | 1
            "zip_code that doesn't exist"          | CQL.toFilter("zipCode='43230'")                                               | new BasicDBObject("calcLocation.address.zip_code", "43230")                                      | 0
    }

    @Unroll("Test poleTag property query for #description")
    void "test poleTag property queries"() {
        setup:
            String typeName = "poleTag"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                | filter                                                | expectedQuery                                              | expectedSize
            "type exists"              | CQL.toFilter("type='MAP'")                            | new BasicDBObject("calcLocation.poleTags.type", "MAP")     | 1
            "type doesn't exist"       | CQL.toFilter("type='POLE'")                           | new BasicDBObject("calcLocation.poleTags.type", "POLE")    | 0
            "value exists"             | CQL.toFilter("value='34-63D'")                        | new BasicDBObject("calcLocation.poleTags.value", "34-63D") | 1
            "value doesn't exist"      | CQL.toFilter("value='TEST'")                          | new BasicDBObject("calcLocation.poleTags.value", "TEST")   | 0
            "locationId exists"        | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'") | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")        | 3
            "locationId doesn't exist" | CQL.toFilter("locationId='TEST'")                     | new BasicDBObject("id", "TEST")                            | 0
    }

    @Unroll("Test summaryNote property query for #description")
    void "test summaryNote property queries"() {
        setup:
            String typeName = "summaryNote"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery || dbQuery.toString() == expectedQuery.toString() // Pattern objects are not easily comparable, but their string representations are
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                | filter                                                                    | expectedQuery                                                                                       | expectedSize
            "value exists"             | CQL.toFilter("value='Windstream/KDL install down guy for span to the W'") | new BasicDBObject("calcLocation.summaryNotes", "Windstream/KDL install down guy for span to the W") | 1
            "value doesn't exist"      | CQL.toFilter("value='TEST'")                                              | new BasicDBObject("calcLocation.summaryNotes", "TEST")                                              | 0
            "locationId exists"        | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'")                     | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")                                                 | 4
            "locationId doesn't exist" | CQL.toFilter("locationId='TEST'")                                         | new BasicDBObject("id", "TEST")                                                                     | 0
            "value like"               | CQL.toFilter("value LIKE 'Windstream%'")                                  | new BasicDBObject("calcLocation.summaryNotes", Pattern.compile("^Windstream.*\$"))                  | 2
            "value like"               | CQL.toFilter("value LIKE 'Test%'")                                        | new BasicDBObject("calcLocation.summaryNotes", Pattern.compile("^Test.*\$"))                        | 0
    }

    @Unroll("Test remedy property query for #description")
    void "test remedy property queries"() {
        setup:
            String typeName = "remedy"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                | filter                                                                                             | expectedQuery                                                                                                                        | expectedSize
            "value exists"             | CQL.toFilter("value='Duplicate pole from other Windstrean/KDL proposal, do not put on cover map'") | new BasicDBObject("calcLocation.remedies.description", "Duplicate pole from other Windstrean/KDL proposal, do not put on cover map") | 1
            "value doesn't exist"      | CQL.toFilter("value='TEST'")                                                                       | new BasicDBObject("calcLocation.remedies.description", "TEST")                                                                       | 0
            "locationId exists"        | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'")                                              | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")                                                                                  | 1
            "locationId doesn't exist" | CQL.toFilter("locationId='TEST'")                                                                  | new BasicDBObject("id", "TEST")                                                                                                      | 0
    }

    @Unroll("Test form property query for #description")
    void "test form property queries"() {
        setup:
            String typeName = "form"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                | filter                                                                                  | expectedQuery                                                                                                                                             | expectedSize
            "title exists"             | CQL.toFilter("title='HTA Form'")                                                        | new BasicDBObject("calcLocation.forms.title", "HTA Form")                                                                                                 | 1
            "title doesn't exist"      | CQL.toFilter("title='TEST'")                                                            | new BasicDBObject("calcLocation.forms.title", "TEST")                                                                                                     | 0
            "template exists"          | CQL.toFilter("template='6ee5fba14760878be22701e1b3b7c05b-HTA Form'")                    | new BasicDBObject("calcLocation.forms.template", "6ee5fba14760878be22701e1b3b7c05b-HTA Form")                                                             | 1
            "template doesn't exist"   | CQL.toFilter("template='TEST'")                                                         | new BasicDBObject("calcLocation.forms.template", "TEST")                                                                                                  | 0
            "locationId exists"        | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'")                                   | new BasicDBObject("id", "55fac7fde4b0e7f2e3be342c")                                                                                                       | 2
            "locationId doesn't exist" | CQL.toFilter("locationId='TEST'")                                                       | new BasicDBObject("id", "TEST")                                                                                                                           | 0
            "id exists"                | CQL.toFilter("id='55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form'") | new BasicDBObject('$and', JSON.parse('[{"id":"55fac7fde4b0e7f2e3be342c"}, {"calcLocation.forms.template":"6ee5fba14760878be22701e1b3b7c05b-HTA Form"}]')) | 1
            "id doesn't exist"         | CQL.toFilter("id='TEST'")                                                               | new BasicDBObject("id", "TEST")                                                                                                                           | 0
    }

    @Unroll("Test formField property query for #description")
    void "test formField property queries"() {
        setup:
            String typeName = "formField"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description               | filter                                                                                      | expectedQuery                                                                                                                                             | expectedSize
            "name exists"             | CQL.toFilter("name='HTA Pole'")                                                             | new BasicDBObject("calcLocation.forms.fields.HTA Pole", new BasicDBObject('$exists', true))                                                               | 1
            "name doesn't exist"      | CQL.toFilter("name='POLE'")                                                                 | new BasicDBObject("calcLocation.forms.fields.POLE", new BasicDBObject('$exists', true))                                                                   | 0
            "value exists"            | CQL.toFilter("value='TesterValue123'")                                                      | new BasicDBObject()                                                                                                                                       | 1
            "value doesn't exist"     | CQL.toFilter("value='TEST'")                                                                | new BasicDBObject()                                                                                                                                       | 0
            "groupName exists"        | CQL.toFilter("groupName='Group Name'")                                                      | new BasicDBObject("calcLocation.forms.fields.Group Name", new BasicDBObject('$exists', true))                                                             | 1
            "groupName doesn't exist" | CQL.toFilter("groupName='TEST'")                                                            | new BasicDBObject("calcLocation.forms.fields.TEST", new BasicDBObject('$exists', true))                                                                   | 0
            "formId exists"           | CQL.toFilter("formId='55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form'") | new BasicDBObject('$and', JSON.parse('[{"id":"55fac7fde4b0e7f2e3be342c"}, {"calcLocation.forms.template":"6ee5fba14760878be22701e1b3b7c05b-HTA Form"}]')) | 1
            "formId doesn't exist"    | CQL.toFilter("formId='TEST'")                                                               | new BasicDBObject("id", "TEST")                                                                                                                           | 0
    }

    @Unroll("Test pole property query for #description")
    void "test pole property queries"() {
        setup:
            String typeName = "pole"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                            | filter                                                               | expectedQuery                                                                                           | expectedSize
            "design type that exists"              | CQL.toFilter("designType='Measured Design'")                         | new BasicDBObject("calcDesign.label", "Measured Design")                                                | 1
            "design type that doesn't exist"       | CQL.toFilter("designType='Existing Design'")                         | new BasicDBObject("calcDesign.label", "Existing Design")                                                | 0

            "location label that exists"           | CQL.toFilter("locationLabel='684704E'")                              | new BasicDBObject("locationLabel", "684704E")                                                           | 1
            "location label that doesn't exist"    | CQL.toFilter("locationLabel='TEST'")                                 | new BasicDBObject("locationLabel", "TEST")                                                              | 0

            "locationId that exists"               | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'")                | new BasicDBObject("locationId", "55fac7fde4b0e7f2e3be342c")                                             | 1
            "locationId that doesn't exist"        | CQL.toFilter("locationId='TEST'")                                    | new BasicDBObject("locationId", "TEST")                                                                 | 0

            "clientFile that exists"               | CQL.toFilter("clientFile='SCE.client'")                              | new BasicDBObject("clientFile", "SCE.client")                                                           | 1
            "clientFile that doesn't exist"        | CQL.toFilter("clientFile='AEP.client'")                              | new BasicDBObject("clientFile", "AEP.client")                                                           | 0

            "clientFileVersion that exists"        | CQL.toFilter("clientFileVersion='6ee5fba14760878be22701e1b3b7c05b'") | new BasicDBObject("clientFileVersion", "6ee5fba14760878be22701e1b3b7c05b")                              | 1
            "clientFileVersion that doesn't exist" | CQL.toFilter("clientFileVersion='TEST'")                             | new BasicDBObject("clientFileVersion", "TEST")                                                          | 0

            "dateModified that exists"             | CQL.toFilter("dateModified=1442498557079")                           | new BasicDBObject("dateModified", 1442498557079)                                                        | 1
            "dateModified that doesn't exist"      | CQL.toFilter("dateModified=3333333")                                 | new BasicDBObject("dateModified", 3333333)                                                              | 0
            "dateModified gt exists"               | CQL.toFilter("dateModified>1442498557078")                           | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557078))                              | 1
            "dateModified gt doesn't exist"        | CQL.toFilter("dateModified>1442498557080")                           | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557080))                              | 0
            "dateModified gte exists"              | CQL.toFilter("dateModified>=1442498557079")                          | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557079))                             | 1
            "dateModified gte doesn't exist"       | CQL.toFilter("dateModified>=1442498557080")                          | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557080))                             | 0
            "dateModified lt exists"               | CQL.toFilter("dateModified<1442498557080")                           | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557080))                              | 1
            "dateModified lt doesn't exist"        | CQL.toFilter("dateModified<1442498557079")                           | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557079))                              | 0
            "dateModified lte exists"              | CQL.toFilter("dateModified<=1442498557079")                          | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557079))                             | 1
            "dateModified lte doesn't exist"       | CQL.toFilter("dateModified<=1442498557078")                          | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557078))                             | 0

            "glc that exists"                      | CQL.toFilter("glc=2.8990375130504664")                               | new BasicDBObject("calcDesign.structure.pole.glc.value", 2.8990375130504664)                            | 1
            "glc that doesn't exist"               | CQL.toFilter("glc=3333333")                                          | new BasicDBObject("calcDesign.structure.pole.glc.value", 3333333)                                       | 0
            "glc gt exists"                        | CQL.toFilter("glc>2")                                                | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$gt', 2))                   | 1
            "glc gt doesn't exist"                 | CQL.toFilter("glc>3")                                                | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$gt', 3))                   | 0
            "glc gte exists"                       | CQL.toFilter("glc>=2.8990375130504664")                              | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$gte', 2.8990375130504664)) | 1
            "glc gte doesn't exist"                | CQL.toFilter("glc>=3")                                               | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$gte', 3))                  | 0
            "glc lt exists"                        | CQL.toFilter("glc<3")                                                | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$lt', 3))                   | 1
            "glc lt doesn't exist"                 | CQL.toFilter("glc<2")                                                | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$lt', 2))                   | 0
            "glc lte exists"                       | CQL.toFilter("glc<=2.8990375130504664")                              | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$lte', 2.8990375130504664)) | 1
            "glc lte doesn't exist"                | CQL.toFilter("glc<=2")                                               | new BasicDBObject("calcDesign.structure.pole.glc.value", new BasicDBObject('$lte', 2))                  | 0

            "agl that exists"                      | CQL.toFilter("agl=38.5")                                             | new BasicDBObject("calcDesign.structure.pole.agl.value", 38.5)                                          | 1
            "agl that doesn't exist"               | CQL.toFilter("agl=3333333")                                          | new BasicDBObject("calcDesign.structure.pole.agl.value", 3333333)                                       | 0
            "agl gt exists"                        | CQL.toFilter("agl>2")                                                | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$gt', 2))                   | 1
            "agl gt doesn't exist"                 | CQL.toFilter("agl>40")                                               | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$gt', 40))                  | 0
            "agl gte exists"                       | CQL.toFilter("agl>=38.5")                                            | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$gte', 38.5))               | 1
            "agl gte doesn't exist"                | CQL.toFilter("agl>=40")                                              | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$gte', 40))                 | 0
            "agl lt exists"                        | CQL.toFilter("agl<40")                                               | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$lt', 40))                  | 1
            "agl lt doesn't exist"                 | CQL.toFilter("agl<30")                                               | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$lt', 30))                  | 0
            "agl lte exists"                       | CQL.toFilter("agl<=38.5")                                            | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$lte', 38.5))               | 1
            "agl lte doesn't exist"                | CQL.toFilter("agl<=30")                                              | new BasicDBObject("calcDesign.structure.pole.agl.value", new BasicDBObject('$lte', 30))                 | 0

            "species does exist"                   | CQL.toFilter("species='Southern Yellow Pine'")                       | new BasicDBObject("calcDesign.structure.pole.clientItem.species", "Southern Yellow Pine")               | 1
            "species doesn't exist"                | CQL.toFilter("species='TEST'")                                       | new BasicDBObject("calcDesign.structure.pole.clientItem.species", "TEST")                               | 0

            "class does exist"                     | CQL.toFilter("class='4'")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.classOfPole", "4")                              | 1
            "class doesn't exist"                  | CQL.toFilter("class='5'")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.classOfPole", "5")                              | 0

            "length that exists"                   | CQL.toFilter("length=45")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", 45)                              | 1
            "length that doesn't exist"            | CQL.toFilter("length=3333333")                                       | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", 3333333)                         | 0
            "length gt exists"                     | CQL.toFilter("length>40")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$gt', 40))    | 1
            "length gt doesn't exist"              | CQL.toFilter("length>50")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$gt', 50))    | 0
            "length gte exists"                    | CQL.toFilter("length>=45")                                           | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$gte', 45))   | 1
            "length gte doesn't exist"             | CQL.toFilter("length>=50")                                           | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$gte', 50))   | 0
            "length lt exists"                     | CQL.toFilter("length<46")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$lt', 46))    | 1
            "length lt doesn't exist"              | CQL.toFilter("length<40")                                            | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$lt', 40))    | 0
            "length lte exists"                    | CQL.toFilter("length<=45")                                           | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$lte', 45))   | 1
            "length lte doesn't exist"             | CQL.toFilter("length<=40")                                           | new BasicDBObject("calcDesign.structure.pole.clientItem.height.value", new BasicDBObject('$lte', 40))   | 0

            "owner does exist"                     | CQL.toFilter("owner='Acme Power'")                                   | new BasicDBObject("calcDesign.structure.pole.owner.id", "Acme Power")                                   | 1
            "owner doesn't exist"                  | CQL.toFilter("owner='TEST'")                                         | new BasicDBObject("calcDesign.structure.pole.owner.id", "TEST")                                         | 0

            "id that exists"                       | CQL.toFilter("id='56e9b7137d84511d8dd0f13c'")                        | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                     | 1
            "id that doesn't exist"                | CQL.toFilter("id='TEST'")                                            | new BasicDBObject("id", "TEST")                                                                         | 0
    }

    @Unroll("Test analysis property query for #description")
    void "test analysis property queries"() {
        setup:
            String typeName = "analysis"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery || dbQuery.toString() == expectedQuery.toString() // Pattern objects are not easily comparable, but their string representations are
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                            | filter                                                               | expectedQuery                                                                                       | expectedSize
            "design type that exists"              | CQL.toFilter("designType='Measured Design'")                         | new BasicDBObject("calcDesign.label", "Measured Design")                                            | 6
            "design type that doesn't exist"       | CQL.toFilter("designType='Existing Design'")                         | new BasicDBObject("calcDesign.label", "Existing Design")                                            | 0

            "loadInfo that exists"                 | CQL.toFilter("loadInfo='CSA Heavy'")                                 | new BasicDBObject("analysisSummary.id", "CSA Heavy")                                                | 6
            "loadInfo that doesn't exist"          | CQL.toFilter("loadInfo='TEST'")                                      | new BasicDBObject("analysisSummary.id", "TEST")                                                     | 0

            "location label that exists"           | CQL.toFilter("locationLabel='684704E'")                              | new BasicDBObject("locationLabel", "684704E")                                                       | 6
            "location label that doesn't exist"    | CQL.toFilter("locationLabel='TEST'")                                 | new BasicDBObject("locationLabel", "TEST")                                                          | 0

            "locationId that exists"               | CQL.toFilter("locationId='55fac7fde4b0e7f2e3be342c'")                | new BasicDBObject("locationId", "55fac7fde4b0e7f2e3be342c")                                         | 6
            "locationId that doesn't exist"        | CQL.toFilter("locationId='TEST'")                                    | new BasicDBObject("locationId", "TEST")                                                             | 0

            "clientFile that exists"               | CQL.toFilter("clientFile='SCE.client'")                              | new BasicDBObject("clientFile", "SCE.client")                                                       | 6
            "clientFile that doesn't exist"        | CQL.toFilter("clientFile='AEP.client'")                              | new BasicDBObject("clientFile", "AEP.client")                                                       | 0

            "clientFileVersion that exists"        | CQL.toFilter("clientFileVersion='6ee5fba14760878be22701e1b3b7c05b'") | new BasicDBObject("clientFileVersion", "6ee5fba14760878be22701e1b3b7c05b")                          | 6
            "clientFileVersion that doesn't exist" | CQL.toFilter("clientFileVersion='TEST'")                             | new BasicDBObject("clientFileVersion", "TEST")                                                      | 0

            "dateModified that exists"             | CQL.toFilter("dateModified=1442498557079")                           | new BasicDBObject("dateModified", 1442498557079)                                                    | 6
            "dateModified that doesn't exist"      | CQL.toFilter("dateModified=3333333")                                 | new BasicDBObject("dateModified", 3333333)                                                          | 0
            "dateModified gt exists"               | CQL.toFilter("dateModified>1442498557078")                           | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557078))                          | 6
            "dateModified gt doesn't exist"        | CQL.toFilter("dateModified>1442498557080")                           | new BasicDBObject("dateModified", new BasicDBObject('$gt', 1442498557080))                          | 0
            "dateModified gte exists"              | CQL.toFilter("dateModified>=1442498557079")                          | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557079))                         | 6
            "dateModified gte doesn't exist"       | CQL.toFilter("dateModified>=1442498557080")                          | new BasicDBObject("dateModified", new BasicDBObject('$gte', 1442498557080))                         | 0
            "dateModified lt exists"               | CQL.toFilter("dateModified<1442498557080")                           | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557080))                          | 6
            "dateModified lt doesn't exist"        | CQL.toFilter("dateModified<1442498557079")                           | new BasicDBObject("dateModified", new BasicDBObject('$lt', 1442498557079))                          | 0
            "dateModified lte exists"              | CQL.toFilter("dateModified<=1442498557079")                          | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557079))                         | 6
            "dateModified lte doesn't exist"       | CQL.toFilter("dateModified<=1442498557078")                          | new BasicDBObject("dateModified", new BasicDBObject('$lte', 1442498557078))                         | 0

            "actual that exists"                   | CQL.toFilter("actual=1.5677448671814123")                            | new BasicDBObject("analysisSummary.results.actual", 1.5677448671814123)                             | 1
            "actual that doesn't exist"            | CQL.toFilter("actual=1")                                             | new BasicDBObject("analysisSummary.results.actual", 1)                                              | 0
            "actual gt exists"                     | CQL.toFilter("actual>1")                                             | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$gt', 1))                    | 6
            "actual gt doesn't exist"              | CQL.toFilter("actual>45")                                            | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$gt', 45))                   | 0
            "actual gte exists"                    | CQL.toFilter("actual>=1.5677448671814123")                           | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$gte', 1.5677448671814123))  | 5
            "actual gte doesn't exist"             | CQL.toFilter("actual>=45")                                           | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$gte', 45))                  | 0
            "actual lt exists"                     | CQL.toFilter("actual<10")                                            | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$lt', 10))                   | 6
            "actual lt doesn't exist"              | CQL.toFilter("actual<1")                                             | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$lt', 1))                    | 0
            "actual lte exists"                    | CQL.toFilter("actual<=1.5677448671814123")                           | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$lte', 1.5677448671814123))  | 2
            "actual lte doesn't exist"             | CQL.toFilter("actual<=1")                                            | new BasicDBObject("analysisSummary.results.actual", new BasicDBObject('$lte', 1))                   | 0

            "allowable that exists"                | CQL.toFilter("allowable=100")                                        | new BasicDBObject("analysisSummary.results.allowable", 100)                                         | 6
            "allowable that doesn't exist"         | CQL.toFilter("allowable=3333333")                                    | new BasicDBObject("analysisSummary.results.allowable", 3333333)                                     | 0
            "allowable gt exists"                  | CQL.toFilter("allowable>99")                                         | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$gt', 99))                | 6
            "allowable gt doesn't exist"           | CQL.toFilter("allowable>110")                                        | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$gt', 110))               | 0
            "allowable gte exists"                 | CQL.toFilter("allowable>=100")                                       | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$gte', 100))              | 6
            "allowable gte doesn't exist"          | CQL.toFilter("allowable>=110")                                       | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$gte', 110))              | 0
            "allowable lt exists"                  | CQL.toFilter("allowable<110")                                        | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$lt', 110))               | 6
            "allowable lt doesn't exist"           | CQL.toFilter("allowable<90")                                         | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$lt', 90))                | 0
            "allowable lte exists"                 | CQL.toFilter("allowable<=100")                                       | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$lte', 100))              | 6
            "allowable lte doesn't exist"          | CQL.toFilter("allowable<=90")                                        | new BasicDBObject("analysisSummary.results.allowable", new BasicDBObject('$lte', 90))               | 0

            "unit that exists"                     | CQL.toFilter("unit='PERCENT'")                                       | new BasicDBObject("analysisSummary.results.unit", "PERCENT")                                        | 6
            "unit that doesn't exist"              | CQL.toFilter("unit='TEST'")                                          | new BasicDBObject("analysisSummary.results.unit", "TEST")                                           | 0

            "analysisDate that exists"             | CQL.toFilter("analysisDate=1446037442824")                           | new BasicDBObject("analysisSummary.results.analysisDate", 1446037442824)                            | 6
            "analysisDate that doesn't exist"      | CQL.toFilter("analysisDate=3333333")                                 | new BasicDBObject("analysisSummary.results.analysisDate", 3333333)                                  | 0
            "analysisDate gt exists"               | CQL.toFilter("analysisDate>1446037442823")                           | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$gt', 1446037442823))  | 6
            "analysisDate gt doesn't exist"        | CQL.toFilter("analysisDate>1446037442825")                           | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$gt', 1446037442825))  | 0
            "analysisDate gte exists"              | CQL.toFilter("analysisDate>=1446037442824")                          | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$gte', 1446037442824)) | 6
            "analysisDate gte doesn't exist"       | CQL.toFilter("analysisDate>=1446037442825")                          | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$gte', 1446037442825)) | 0
            "analysisDate lt exists"               | CQL.toFilter("analysisDate<1446037442825")                           | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$lt', 1446037442825))  | 6
            "analysisDate lt doesn't exist"        | CQL.toFilter("analysisDate<1446037442823")                           | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$lt', 1446037442823))  | 0
            "analysisDate lte exists"              | CQL.toFilter("analysisDate<=1446037442824")                          | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$lte', 1446037442824)) | 6
            "analysisDate lte doesn't exist"       | CQL.toFilter("analysisDate<=1446037442823")                          | new BasicDBObject("analysisSummary.results.analysisDate", new BasicDBObject('$lte', 1446037442823)) | 0

            "component that exists"                | CQL.toFilter("component='Pole'")                                     | new BasicDBObject("analysisSummary.results.component", "Pole")                                      | 6
            "component that doesn't exist"         | CQL.toFilter("component='Strength'")                                 | new BasicDBObject("analysisSummary.results.component", "Strength")                                  | 0
            "component like"                       | CQL.toFilter("component LIKE 'Pol%'")                                | new BasicDBObject("analysisSummary.results.component", Pattern.compile("^Pol.*\$"))           | 6
            "component like doesn't exist"         | CQL.toFilter("component LIKE 'Guy%'")                                | new BasicDBObject("analysisSummary.results.component", Pattern.compile("^Guy.*\$"))           | 0

            "analysisType that exists"             | CQL.toFilter("analysisType='BUCKLING'")                              | new BasicDBObject("analysisSummary.results.analysisType", "BUCKLING")                               | 2
            "analysisType that doesn't exist"      | CQL.toFilter("analysisType='TEST'")                                  | new BasicDBObject("analysisSummary.results.analysisType", "TEST")                                   | 0
            "analysisType like"                    | CQL.toFilter("analysisType LIKE 'BUCKLIN%'")                         | new BasicDBObject("analysisSummary.results.analysisType", Pattern.compile("^BUCKLIN.*\$"))    | 2
            "analysisTypelike doesn't exist"       | CQL.toFilter("analysisType LIKE 'STRENGT%'")                         | new BasicDBObject("analysisSummary.results.analysisType", Pattern.compile("^STRENGT.*\$"))    | 0

            "passes that exists"                   | CQL.toFilter("passes='true'")                                        | new BasicDBObject("analysisSummary.results.passes", true)                                           | 6
            "passes that doesn't exist"            | CQL.toFilter("passes='false'")                                       | new BasicDBObject("analysisSummary.results.passes", false)                                          | 0

            "poleId that exists"                   | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'")                    | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                 | 6
            "poleId that doesn't exist"            | CQL.toFilter("poleId='TEST'")                                        | new BasicDBObject("id", "TEST")                                                                     | 0

            "id that exists"                       | CQL.toFilter("id='56e9b7137d84511d8dd0f13c_ANALYSIS_0_0'")           | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                 | 1
            "id that doesn't exist"                | CQL.toFilter("id='TEST'")                                            | new BasicDBObject("id", "TEST")                                                                     | 0
    }

    @Unroll("Test wire property query for #description")
    void "test wire property queries"() {
        setup:
            String typeName = "wire"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                            | filter                                               | expectedQuery                                                                                                         | expectedSize
            "owner that exists"                    | CQL.toFilter("owner='AEP'")                          | new BasicDBObject("calcDesign.structure.wires.owner.id", "AEP")                                                       | 1
            "owner that doesn't exist"             | CQL.toFilter("owner='SCE'")                          | new BasicDBObject("calcDesign.structure.wires.owner.id", "SCE")                                                       | 0

            "size that exists"                     | CQL.toFilter("size='1/0 AAAC'")                      | new BasicDBObject("calcDesign.structure.wires.clientItem.size", "1/0 AAAC")                                           | 1
            "size that doesn't exist"              | CQL.toFilter("size='TEST'")                          | new BasicDBObject("calcDesign.structure.wires.clientItem.size", "TEST")                                               | 0

            "coreStrands that exists"              | CQL.toFilter("coreStrands=1")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", 1)                                             | 1
            "coreStrands that doesn't exist"       | CQL.toFilter("coreStrands=3")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", 3)                                             | 0
            "coreStrands gt exists"                | CQL.toFilter("coreStrands>3")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$gt', 3))                   | 1
            "coreStrands gt doesn't exist"         | CQL.toFilter("coreStrands>5")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$gt', 5))                   | 0
            "coreStrands gte exists"               | CQL.toFilter("coreStrands>=4")                       | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$gte', 4))                  | 1
            "coreStrands gte doesn't exist"        | CQL.toFilter("coreStrands>=5")                       | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$gte', 5))                  | 0
            "coreStrands lt exists"                | CQL.toFilter("coreStrands<2")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$lt', 2))                   | 1
            "coreStrands lt doesn't exist"         | CQL.toFilter("coreStrands<0")                        | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$lt', 0))                   | 0
            "coreStrands lte exists"               | CQL.toFilter("coreStrands<=1")                       | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$lte', 1))                  | 1
            "coreStrands lte doesn't exist"        | CQL.toFilter("coreStrands<=0")                       | new BasicDBObject("calcDesign.structure.wires.clientItem.coreStrands", new BasicDBObject('$lte', 0))                  | 0

            "conductorStrands that exists"         | CQL.toFilter("conductorStrands=2")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", 2)                                        | 1
            "conductorStrands that doesn't exist"  | CQL.toFilter("conductorStrands=5")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", 5)                                        | 0
            "conductorStrands gt exists"           | CQL.toFilter("conductorStrands>6")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$gt', 6))              | 1
            "conductorStrands gt doesn't exist"    | CQL.toFilter("conductorStrands>8")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$gt', 8))              | 0
            "conductorStrands gte exists"          | CQL.toFilter("conductorStrands>=7")                  | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$gte', 7))             | 1
            "conductorStrands gte doesn't exist"   | CQL.toFilter("conductorStrands>=8")                  | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$gte', 8))             | 0
            "conductorStrands lt exists"           | CQL.toFilter("conductorStrands<3")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$lt', 3))              | 1
            "conductorStrands lt doesn't exist"    | CQL.toFilter("conductorStrands<1")                   | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$lt', 1))              | 0
            "conductorStrands lte exists"          | CQL.toFilter("conductorStrands<=3")                  | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$lte', 3))             | 1
            "conductorStrands lte doesn't exist"   | CQL.toFilter("conductorStrands<=1")                  | new BasicDBObject("calcDesign.structure.wires.clientItem.conductorStrands", new BasicDBObject('$lte', 1))             | 0

            "attachmentHeight that exists"         | CQL.toFilter("attachmentHeight=33.25")               | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", 33.25)                                         | 1
            "attachmentHeight that doesn't exist"  | CQL.toFilter("attachmentHeight=31")                  | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", 31)                                            | 0
            "attachmentHeight gt exists"           | CQL.toFilter("attachmentHeight>35")                  | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$gt', 35))                  | 1
            "attachmentHeight gt doesn't exist"    | CQL.toFilter("attachmentHeight>40")                  | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$gt', 40))                  | 0
            "attachmentHeight gte exists"          | CQL.toFilter("attachmentHeight>=38.604166666666664") | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$gte', 38.604166666666664)) | 1
            "attachmentHeight gte doesn't exist"   | CQL.toFilter("attachmentHeight>=40")                 | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$gte', 40))                 | 0
            "attachmentHeight lt exists"           | CQL.toFilter("attachmentHeight<34")                  | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$lt', 34))                  | 1
            "attachmentHeight lt doesn't exist"    | CQL.toFilter("attachmentHeight<30")                  | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$lt', 30))                  | 0
            "attachmentHeight lte exists"          | CQL.toFilter("attachmentHeight<=33.25")              | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$lte', 33.25))              | 1
            "attachmentHeight lte doesn't exist"   | CQL.toFilter("attachmentHeight<=30")                 | new BasicDBObject("calcDesign.structure.wires.attachmentHeight.value", new BasicDBObject('$lte', 30))                 | 0

            "usageGroup that exists"               | CQL.toFilter("usageGroup='NEUTRAL'")                 | new BasicDBObject("calcDesign.structure.wires.usageGroup", "NEUTRAL")                                                 | 1
            "usageGroup that doesn't exist"        | CQL.toFilter("usageGroup='TEST'")                    | new BasicDBObject("calcDesign.structure.wires.usageGroup", "TEST")                                                    | 0

            "tensionGroup that exists"             | CQL.toFilter("tensionGroup='Full'")                  | new BasicDBObject("calcDesign.structure.wires.tensionGroup", "Full")                                                  | 2
            "tensionGroup that doesn't exist"      | CQL.toFilter("tensionGroup='TEST'")                  | new BasicDBObject("calcDesign.structure.wires.tensionGroup", "TEST")                                                  | 0

            "midspanHeight that exists"            | CQL.toFilter("midspanHeight=27.5")                   | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", 27.5)                                             | 1
            "midspanHeight that doesn't exist"     | CQL.toFilter("midspanHeight=22")                     | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", 22)                                               | 0
            "midspanHeight gt exists"              | CQL.toFilter("midspanHeight>25")                     | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$gt', 25))                     | 1
            "midspanHeight gt doesn't exist"       | CQL.toFilter("midspanHeight>28")                     | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$gt', 28))                     | 0
            "midspanHeight gte exists"             | CQL.toFilter("midspanHeight>=27.5")                  | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$gte', 27.5))                  | 1
            "midspanHeight gte doesn't exist"      | CQL.toFilter("midspanHeight>=28")                    | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$gte', 28))                    | 0
            "midspanHeight lt exists"              | CQL.toFilter("midspanHeight<28")                     | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$lt', 28))                     | 2
            "midspanHeight lt doesn't exist"       | CQL.toFilter("midspanHeight<2")                      | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$lt', 2))                      | 0
            "midspanHeight lte exists"             | CQL.toFilter("midspanHeight<=27.5")                  | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$lte', 27.5))                  | 2
            "midspanHeight lte doesn't exist"      | CQL.toFilter("midspanHeight<=1")                     | new BasicDBObject("calcDesign.structure.wires.midspanHeight.value", new BasicDBObject('$lte', 1))                     | 0

            "tensionAdjustment that exists"        | CQL.toFilter("tensionAdjustment=0.9")                | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", 0.9)                                                | 1
            "tensionAdjustment that doesn't exist" | CQL.toFilter("tensionAdjustment=0.8")                | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", 0.8)                                                | 0
            "tensionAdjustment gt exists"          | CQL.toFilter("tensionAdjustment>0.95")               | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$gt', 0.95))                     | 1
            "tensionAdjustment gt doesn't exist"   | CQL.toFilter("tensionAdjustment>1.15")               | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$gt', 1.15))                     | 0
            "tensionAdjustment gte exists"         | CQL.toFilter("tensionAdjustment>=1")                 | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$gte', 1))                       | 1
            "tensionAdjustment gte doesn't exist"  | CQL.toFilter("tensionAdjustment>=1.15")              | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$gte', 1.15))                    | 0
            "tensionAdjustment lt exists"          | CQL.toFilter("tensionAdjustment<0.95")               | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$lt', 0.95))                     | 1
            "tensionAdjustment lt doesn't exist"   | CQL.toFilter("tensionAdjustment<0.9")                | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$lt', 0.9))                      | 0
            "tensionAdjustment lte exists"         | CQL.toFilter("tensionAdjustment<=0.9")               | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$lte', 0.9))                     | 1
            "tensionAdjustment lte doesn't exist"  | CQL.toFilter("tensionAdjustment<=0.85")              | new BasicDBObject("calcDesign.structure.wires.tensionAdjustment", new BasicDBObject('$lte', 0.85))                    | 0

            "poleId that exists"                   | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'")    | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                                   | 2
            "poleId that doesn't exist"            | CQL.toFilter("poleId='Measured Design'")             | new BasicDBObject("id", "Measured Design")                                                                            | 0
    }

    @Unroll("Test spanPoint property query for #description")
    void "test spanPoint property queries"() {
        String typeName = "spanPoint"
        String collectionName = "designs"
        Query query = new Query(typeName, filter)
        FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                   | filter                                            | expectedQuery                                                                                      | expectedSize
            "distance that exists"        | CQL.toFilter("distance=88")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", 88)                            | 1
            "distance that doesn't exist" | CQL.toFilter("distance=99")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", 99)                            | 0
            "distance gt exists"          | CQL.toFilter("distance>80")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$gt', 80))  | 1
            "distance gt doesn't exist"   | CQL.toFilter("distance>90")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$gt', 90))  | 0
            "distance gte exists"         | CQL.toFilter("distance>=88")                      | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$gte', 88)) | 1
            "distance gte doesn't exist"  | CQL.toFilter("distance>=90")                      | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$gte', 90)) | 0
            "distance lt exists"          | CQL.toFilter("distance<90")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$lt', 90))  | 1
            "distance lt doesn't exist"   | CQL.toFilter("distance<85")                       | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$lt', 85))  | 0
            "distance lte exists"         | CQL.toFilter("distance<=88")                      | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$lte', 88)) | 1
            "distance lte doesn't exist"  | CQL.toFilter("distance<=87")                      | new BasicDBObject("calcDesign.structure.spanPoints.distance.value", new BasicDBObject('$lte', 87)) | 0

            "poleId that exists"          | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                | 1
            "poleId that doesn't exist"   | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                         | 0
    }

    @Unroll("Test spanGuy property query for #description")
    void "test spanGuy property queries"() {
        setup:
            String typeName = "spanGuy"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                           | filter                                              | expectedQuery                                                                                                 | expectedSize
            "owner that exists"                   | CQL.toFilter("owner='AEP'")                         | new BasicDBObject("calcDesign.structure.spanGuys.owner.id", "AEP")                                            | 1
            "owner that doesn't exist"            | CQL.toFilter("owner='TEST'")                        | new BasicDBObject("calcDesign.structure.spanGuys.owner.id", "TEST")                                           | 0

            "size that exists"                    | CQL.toFilter("size='3/8\" EHS'")                    | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.size", "3/8\" EHS")                               | 1
            "size that doesn't exist"             | CQL.toFilter("size='TEST'")                         | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.size", "TEST")                                    | 0

            "coreStrands that exists"             | CQL.toFilter("coreStrands=1")                       | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", 1)                                  | 1
            "coreStrands that doesn't exist"      | CQL.toFilter("coreStrands=99")                      | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", 99)                                 | 0
            "coreStrands gt exists"               | CQL.toFilter("coreStrands>0")                       | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$gt', 0))        | 1
            "coreStrands gt doesn't exist"        | CQL.toFilter("coreStrands>3")                       | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$gt', 3))        | 0
            "coreStrands gte exists"              | CQL.toFilter("coreStrands>=1")                      | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$gte', 1))       | 1
            "coreStrands gte doesn't exist"       | CQL.toFilter("coreStrands>=4")                      | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$gte', 4))       | 0
            "coreStrands lt exists"               | CQL.toFilter("coreStrands<4")                       | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$lt', 4))        | 1
            "coreStrands lt doesn't exist"        | CQL.toFilter("coreStrands<1")                       | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$lt', 1))        | 0
            "coreStrands lte exists"              | CQL.toFilter("coreStrands<=1")                      | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$lte', 1))       | 1
            "coreStrands lte doesn't exist"       | CQL.toFilter("coreStrands<=0")                      | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.coreStrands", new BasicDBObject('$lte', 0))       | 0

            "conductorStrands that exists"        | CQL.toFilter("conductorStrands=7")                  | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", 7)                             | 1
            "conductorStrands that doesn't exist" | CQL.toFilter("conductorStrands=99")                 | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", 99)                            | 0
            "conductorStrands gt exists"          | CQL.toFilter("conductorStrands>5")                  | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$gt', 5))   | 1
            "conductorStrands gt doesn't exist"   | CQL.toFilter("conductorStrands>8")                  | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$gt', 8))   | 0
            "conductorStrands gte exists"         | CQL.toFilter("conductorStrands>=6")                 | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$gte', 6))  | 1
            "conductorStrands gte doesn't exist"  | CQL.toFilter("conductorStrands>=9")                 | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$gte', 9))  | 0
            "conductorStrands lt exists"          | CQL.toFilter("conductorStrands<8")                  | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$lt', 8))   | 1
            "conductorStrands lt doesn't exist"   | CQL.toFilter("conductorStrands<3")                  | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$lt', 3))   | 0
            "conductorStrands lte exists"         | CQL.toFilter("conductorStrands<=10")                | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$lte', 10)) | 1
            "conductorStrands lte doesn't exist"  | CQL.toFilter("conductorStrands<=2")                 | new BasicDBObject("calcDesign.structure.spanGuys.clientItem.conductorStrands", new BasicDBObject('$lte', 2))  | 0

            "attachmentHeight that exists"        | CQL.toFilter("attachmentHeight=32.666666666666664") | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", 32.666666666666664)                 | 1
            "attachmentHeight that doesn't exist" | CQL.toFilter("attachmentHeight=99")                 | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", 99)                                 | 0
            "attachmentHeight gt exists"          | CQL.toFilter("attachmentHeight>30")                 | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$gt', 30))       | 1
            "attachmentHeight gt doesn't exist"   | CQL.toFilter("attachmentHeight>40")                 | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$gt', 40))       | 0
            "attachmentHeight gte exists"         | CQL.toFilter("attachmentHeight>=30")                | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$gte', 30))      | 1
            "attachmentHeight gte doesn't exist"  | CQL.toFilter("attachmentHeight>=40")                | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$gte', 40))      | 0
            "attachmentHeight lt exists"          | CQL.toFilter("attachmentHeight<40")                 | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$lt', 40))       | 1
            "attachmentHeight lt doesn't exist"   | CQL.toFilter("attachmentHeight<20")                 | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$lt', 20))       | 0
            "attachmentHeight lte exists"         | CQL.toFilter("attachmentHeight<=40")                | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$lte', 40))      | 1
            "attachmentHeight lte doesn't exist"  | CQL.toFilter("attachmentHeight<=20")                | new BasicDBObject("calcDesign.structure.spanGuys.attachmentHeight.value", new BasicDBObject('$lte', 20))      | 0

            "midspanHeight that exists"           | CQL.toFilter("midspanHeight=28.916666666666668")    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", 28.916666666666668)                    | 1
            "midspanHeight that doesn't exist"    | CQL.toFilter("midspanHeight=99")                    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", 99)                                    | 0
            "midspanHeight gt exists"             | CQL.toFilter("midspanHeight>20")                    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$gt', 20))          | 1
            "midspanHeight gt doesn't exist"      | CQL.toFilter("midspanHeight>40")                    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$gt', 40))          | 0
            "midspanHeight gte exists"            | CQL.toFilter("midspanHeight>=25")                   | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$gte', 25))         | 1
            "midspanHeight gte doesn't exist"     | CQL.toFilter("midspanHeight>=40")                   | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$gte', 40))         | 0
            "midspanHeight lt exists"             | CQL.toFilter("midspanHeight<40")                    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$lt', 40))          | 1
            "midspanHeight lt doesn't exist"      | CQL.toFilter("midspanHeight<20")                    | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$lt', 20))          | 0
            "midspanHeight lte exists"            | CQL.toFilter("midspanHeight<=40")                   | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$lte', 40))         | 1
            "midspanHeight lte doesn't exist"     | CQL.toFilter("midspanHeight<=20")                   | new BasicDBObject("calcDesign.structure.spanGuys.midspanHeight.value", new BasicDBObject('$lte', 20))         | 0

            "height that exists"                  | CQL.toFilter("height=27.25")                        | new BasicDBObject("calcDesign.structure.spanGuys.height.value", 27.25)                                        | 1
            "height that doesn't exist"           | CQL.toFilter("height=99")                           | new BasicDBObject("calcDesign.structure.spanGuys.height.value", 99)                                           | 0
            "height gt exists"                    | CQL.toFilter("height>20")                           | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$gt', 20))                 | 1
            "height gt doesn't exist"             | CQL.toFilter("height>40")                           | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$gt', 40))                 | 0
            "height gte exists"                   | CQL.toFilter("height>=20")                          | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$gte', 20))                | 1
            "height gte doesn't exist"            | CQL.toFilter("height>=40")                          | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$gte', 40))                | 0
            "height lt exists"                    | CQL.toFilter("height<40")                           | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$lt', 40))                 | 1
            "height lt doesn't exist"             | CQL.toFilter("height<20")                           | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$lt', 20))                 | 0
            "height lte exists"                   | CQL.toFilter("height<=40")                          | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$lte', 40))                | 1
            "height lte doesn't exist"            | CQL.toFilter("height<=20")                          | new BasicDBObject("calcDesign.structure.spanGuys.height.value", new BasicDBObject('$lte', 20))                | 0

            "poleId that exists"                  | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'")   | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                           | 1
            "poleId that doesn't exist"           | CQL.toFilter("poleId='Measured Design'")            | new BasicDBObject("id", "Measured Design")                                                                    | 0
    }

    @Unroll("Test guy property query for #description")
    void "test guy property queries"() {
        setup:
            String typeName = "guy"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                           | filter                                            | expectedQuery                                                                                             | expectedSize
            "owner that exists"                   | CQL.toFilter("owner='AEP'")                       | new BasicDBObject("calcDesign.structure.guys.owner.id", "AEP")                                            | 1
            "owner that doesn't exist"            | CQL.toFilter("owner='TEST'")                      | new BasicDBObject("calcDesign.structure.guys.owner.id", "TEST")                                           | 0

            "size that exists"                    | CQL.toFilter("size='3/8\" EHS'")                  | new BasicDBObject("calcDesign.structure.guys.clientItem.size", "3/8\" EHS")                               | 1
            "size that doesn't exist"             | CQL.toFilter("size='TEST'")                       | new BasicDBObject("calcDesign.structure.guys.clientItem.size", "TEST")                                    | 0

            "coreStrands that exists"             | CQL.toFilter("coreStrands=1")                     | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", 1)                                  | 1
            "coreStrands that doesn't exist"      | CQL.toFilter("coreStrands=99")                    | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", 99)                                 | 0
            "coreStrands gt exists"               | CQL.toFilter("coreStrands>0")                     | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$gt', 0))        | 1
            "coreStrands gt doesn't exist"        | CQL.toFilter("coreStrands>3")                     | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$gt', 3))        | 0
            "coreStrands gte exists"              | CQL.toFilter("coreStrands>=1")                    | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$gte', 1))       | 1
            "coreStrands gte doesn't exist"       | CQL.toFilter("coreStrands>=4")                    | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$gte', 4))       | 0
            "coreStrands lt exists"               | CQL.toFilter("coreStrands<4")                     | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$lt', 4))        | 1
            "coreStrands lt doesn't exist"        | CQL.toFilter("coreStrands<1")                     | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$lt', 1))        | 0
            "coreStrands lte exists"              | CQL.toFilter("coreStrands<=1")                    | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$lte', 1))       | 1
            "coreStrands lte doesn't exist"       | CQL.toFilter("coreStrands<=0")                    | new BasicDBObject("calcDesign.structure.guys.clientItem.coreStrands", new BasicDBObject('$lte', 0))       | 0

            "conductorStrands that exists"        | CQL.toFilter("conductorStrands=7")                | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", 7)                             | 1
            "conductorStrands that doesn't exist" | CQL.toFilter("conductorStrands=99")               | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", 99)                            | 0
            "conductorStrands gt exists"          | CQL.toFilter("conductorStrands>5")                | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$gt', 5))   | 1
            "conductorStrands gt doesn't exist"   | CQL.toFilter("conductorStrands>8")                | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$gt', 8))   | 0
            "conductorStrands gte exists"         | CQL.toFilter("conductorStrands>=6")               | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$gte', 6))  | 1
            "conductorStrands gte doesn't exist"  | CQL.toFilter("conductorStrands>=9")               | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$gte', 9))  | 0
            "conductorStrands lt exists"          | CQL.toFilter("conductorStrands<8")                | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$lt', 8))   | 1
            "conductorStrands lt doesn't exist"   | CQL.toFilter("conductorStrands<3")                | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$lt', 3))   | 0
            "conductorStrands lte exists"         | CQL.toFilter("conductorStrands<=10")              | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$lte', 10)) | 1
            "conductorStrands lte doesn't exist"  | CQL.toFilter("conductorStrands<=2")               | new BasicDBObject("calcDesign.structure.guys.clientItem.conductorStrands", new BasicDBObject('$lte', 2))  | 0

            "attachmentHeight that exists"        | CQL.toFilter("attachmentHeight=28.25")            | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", 28.25)                              | 1
            "attachmentHeight that doesn't exist" | CQL.toFilter("attachmentHeight=99")               | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", 99)                                 | 0
            "attachmentHeight gt exists"          | CQL.toFilter("attachmentHeight>20")               | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$gt', 20))       | 1
            "attachmentHeight gt doesn't exist"   | CQL.toFilter("attachmentHeight>40")               | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$gt', 40))       | 0
            "attachmentHeight gte exists"         | CQL.toFilter("attachmentHeight>=20")              | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$gte', 20))      | 1
            "attachmentHeight gte doesn't exist"  | CQL.toFilter("attachmentHeight>=40")              | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$gte', 40))      | 0
            "attachmentHeight lt exists"          | CQL.toFilter("attachmentHeight<40")               | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$lt', 40))       | 1
            "attachmentHeight lt doesn't exist"   | CQL.toFilter("attachmentHeight<20")               | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$lt', 20))       | 0
            "attachmentHeight lte exists"         | CQL.toFilter("attachmentHeight<=40")              | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$lte', 40))      | 1
            "attachmentHeight lte doesn't exist"  | CQL.toFilter("attachmentHeight<=20")              | new BasicDBObject("calcDesign.structure.guys.attachmentHeight.value", new BasicDBObject('$lte', 20))      | 0

            "poleId that exists"                  | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                       | 1
            "poleId that doesn't exist"           | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                                | 0
    }

    @Unroll("Test insulator property query for #description")
    void "test insulator property queries"() {
        setup:
            String typeName = "insulator"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                           | filter                                               | expectedQuery                                                                                                              | expectedSize
            "owner that exists"                   | CQL.toFilter("owner='AEP'")                          | new BasicDBObject("calcDesign.structure.insulators.owner.id", "AEP")                                                       | 1
            "owner that doesn't exist"            | CQL.toFilter("owner='SCE'")                          | new BasicDBObject("calcDesign.structure.insulators.owner.id", "SCE")                                                       | 0

            "type that exists"                    | CQL.toFilter("type='15kV Dead End Insulator'")       | new BasicDBObject("calcDesign.structure.insulators.clientItem", "15kV Dead End Insulator")                                 | 1
            "type that doesn't exist"             | CQL.toFilter("type='TEST'")                          | new BasicDBObject("calcDesign.structure.insulators.clientItem", "TEST")                                                    | 0

            "attachmentHeight that exists"        | CQL.toFilter("attachmentHeight=27.083333333333332")  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", 27.083333333333332)                            | 1
            "attachmentHeight that doesn't exist" | CQL.toFilter("attachmentHeight=31")                  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", 31)                                            | 0
            "attachmentHeight gt exists"          | CQL.toFilter("attachmentHeight>24")                  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$gt', 24))                  | 1
            "attachmentHeight gt doesn't exist"   | CQL.toFilter("attachmentHeight>30")                  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$gt', 30))                  | 0
            "attachmentHeight gte exists"         | CQL.toFilter("attachmentHeight>=27.083333333333332") | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$gte', 27.083333333333332)) | 1
            "attachmentHeight gte doesn't exist"  | CQL.toFilter("attachmentHeight>=30")                 | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$gte', 30))                 | 0
            "attachmentHeight lt exists"          | CQL.toFilter("attachmentHeight<28")                  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$lt', 28))                  | 1
            "attachmentHeight lt doesn't exist"   | CQL.toFilter("attachmentHeight<22")                  | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$lt', 22))                  | 0
            "attachmentHeight lte exists"         | CQL.toFilter("attachmentHeight<=27.083333333333332") | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$lte', 27.083333333333332)) | 1
            "attachmentHeight lte doesn't exist"  | CQL.toFilter("attachmentHeight<=22")                 | new BasicDBObject("calcDesign.structure.insulators.attachmentHeight.value", new BasicDBObject('$lte', 22))                 | 0

            "offset that exists"                  | CQL.toFilter("offset=325")                           | new BasicDBObject("calcDesign.structure.insulators.offset.value", 325)                                                     | 1
            "offset that doesn't exist"           | CQL.toFilter("offset=31")                            | new BasicDBObject("calcDesign.structure.insulators.offset.value", 31)                                                      | 0
            "offset gt exists"                    | CQL.toFilter("offset>24")                            | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$gt', 24))                            | 1
            "offset gt doesn't exist"             | CQL.toFilter("offset>400")                           | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$gt', 400))                           | 0
            "offset gte exists"                   | CQL.toFilter("offset>=325")                          | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$gte', 325))                          | 1
            "offset gte doesn't exist"            | CQL.toFilter("offset>=400")                          | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$gte', 400))                          | 0
            "offset lt exists"                    | CQL.toFilter("offset<400")                           | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$lt', 400))                           | 1
            "offset lt doesn't exist"             | CQL.toFilter("offset<2")                             | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$lt', 2))                             | 0
            "offset lte exists"                   | CQL.toFilter("offset<=325")                          | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$lte', 325))                          | 1
            "offset lte doesn't exist"            | CQL.toFilter("offset<=20")                           | new BasicDBObject("calcDesign.structure.insulators.offset.value", new BasicDBObject('$lte', 20))                           | 0

            "direction that exists"               | CQL.toFilter("direction=95")                         | new BasicDBObject("calcDesign.structure.insulators.direction", 95)                                                         | 1
            "direction that doesn't exist"        | CQL.toFilter("direction=120")                        | new BasicDBObject("calcDesign.structure.insulators.direction", 120)                                                        | 0
            "direction gt exists"                 | CQL.toFilter("direction>90")                         | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$gt', 90))                               | 1
            "direction gt doesn't exist"          | CQL.toFilter("direction>110")                        | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$gt', 110))                              | 0
            "direction gte exists"                | CQL.toFilter("direction>=90")                        | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$gte', 90))                              | 1
            "direction gte doesn't exist"         | CQL.toFilter("direction>=110")                       | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$gte', 110))                             | 0
            "direction lt exists"                 | CQL.toFilter("direction<100")                        | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$lt', 100))                              | 1
            "direction lt doesn't exist"          | CQL.toFilter("direction<90")                         | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$lt', 90))                               | 0
            "direction lte exists"                | CQL.toFilter("direction<=95")                        | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$lte', 95))                              | 1
            "direction lte doesn't exist"         | CQL.toFilter("direction<=20")                        | new BasicDBObject("calcDesign.structure.insulators.direction", new BasicDBObject('$lte', 20))                              | 0

            "doubleInsulator that exists"         | CQL.toFilter("doubleInsulator=false")                | new BasicDBObject("calcDesign.structure.insulators.doubleInsulator", false)                                                | 1
            "doubleInsulator that doesn't exist"  | CQL.toFilter("doubleInsulator=true")                 | new BasicDBObject("calcDesign.structure.insulators.doubleInsulator", true)                                                 | 0

            "poleId that exists"                  | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'")    | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                                        | 1
            "poleId that doesn't exist"           | CQL.toFilter("poleId='Measured Design'")             | new BasicDBObject("id", "Measured Design")                                                                                 | 0
    }

    @Unroll("Test equipment property query for #description")
    void "test equipment property queries"() {
        setup:
            String typeName = "equipment"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                           | filter                                            | expectedQuery                                                                                                 | expectedSize
            "owner that exists"                   | CQL.toFilter("owner='AEP'")                       | new BasicDBObject("calcDesign.structure.equipments.owner.id", "AEP")                                          | 1
            "owner that doesn't exist"            | CQL.toFilter("owner='SCE'")                       | new BasicDBObject("calcDesign.structure.equipments.owner.id", "SCE")                                          | 0

            "type that exists"                    | CQL.toFilter("type='CUTOUT_ARRESTOR'")            | new BasicDBObject("calcDesign.structure.equipments.clientItem.type", "CUTOUT_ARRESTOR")                       | 1
            "type that doesn't exist"             | CQL.toFilter("type='TEST'")                       | new BasicDBObject("calcDesign.structure.equipments.clientItem.type", "TEST")                                  | 0

            "size that exists"                    | CQL.toFilter("size='1 Cutout'")                   | new BasicDBObject("calcDesign.structure.equipments.clientItem.size", "1 Cutout")                              | 1
            "size that doesn't exist"             | CQL.toFilter("size='TEST'")                       | new BasicDBObject("calcDesign.structure.equipments.clientItem.size", "TEST")                                  | 0

            "attachmentHeight that exists"        | CQL.toFilter("attachmentHeight=24.75")            | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", 24.75)                            | 1
            "attachmentHeight that doesn't exist" | CQL.toFilter("attachmentHeight=31")               | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", 31)                               | 0
            "attachmentHeight gt exists"          | CQL.toFilter("attachmentHeight>24")               | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$gt', 24))     | 1
            "attachmentHeight gt doesn't exist"   | CQL.toFilter("attachmentHeight>25")               | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$gt', 25))     | 0
            "attachmentHeight gte exists"         | CQL.toFilter("attachmentHeight>=24.75")           | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$gte', 24.75)) | 1
            "attachmentHeight gte doesn't exist"  | CQL.toFilter("attachmentHeight>=25")              | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$gte', 25))    | 0
            "attachmentHeight lt exists"          | CQL.toFilter("attachmentHeight<25")               | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$lt', 25))     | 1
            "attachmentHeight lt doesn't exist"   | CQL.toFilter("attachmentHeight<22")               | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$lt', 22))     | 0
            "attachmentHeight lte exists"         | CQL.toFilter("attachmentHeight<=24.75")           | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$lte', 24.75)) | 1
            "attachmentHeight lte doesn't exist"  | CQL.toFilter("attachmentHeight<=20")              | new BasicDBObject("calcDesign.structure.equipments.attachmentHeight.value", new BasicDBObject('$lte', 20))    | 0

            "bottomHeight that exists"            | CQL.toFilter("bottomHeight=24.75")                | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", 24.75)                                | 1
            "bottomHeight that doesn't exist"     | CQL.toFilter("bottomHeight=31")                   | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", 31)                                   | 0
            "bottomHeight gt exists"              | CQL.toFilter("bottomHeight>24")                   | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$gt', 24))         | 1
            "bottomHeight gt doesn't exist"       | CQL.toFilter("bottomHeight>25")                   | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$gt', 25))         | 0
            "bottomHeight gte exists"             | CQL.toFilter("bottomHeight>=24.75")               | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$gte', 24.75))     | 1
            "bottomHeight gte doesn't exist"      | CQL.toFilter("bottomHeight>=25")                  | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$gte', 25))        | 0
            "bottomHeight lt exists"              | CQL.toFilter("bottomHeight<25")                   | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$lt', 25))         | 1
            "bottomHeight lt doesn't exist"       | CQL.toFilter("bottomHeight<22")                   | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$lt', 22))         | 0
            "bottomHeight lte exists"             | CQL.toFilter("bottomHeight<=24.75")               | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$lte', 24.75))     | 1
            "bottomHeight lte doesn't exist"      | CQL.toFilter("bottomHeight<=20")                  | new BasicDBObject("calcDesign.structure.equipments.bottomHeight.value", new BasicDBObject('$lte', 20))        | 0

            "direction that exists"               | CQL.toFilter("direction=108")                     | new BasicDBObject("calcDesign.structure.equipments.direction", 108)                                           | 1
            "direction that doesn't exist"        | CQL.toFilter("direction=120")                     | new BasicDBObject("calcDesign.structure.equipments.direction", 120)                                           | 0
            "direction gt exists"                 | CQL.toFilter("direction>100")                     | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$gt', 100))                 | 1
            "direction gt doesn't exist"          | CQL.toFilter("direction>110")                     | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$gt', 110))                 | 0
            "direction gte exists"                | CQL.toFilter("direction>=108")                    | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$gte', 108))                | 1
            "direction gte doesn't exist"         | CQL.toFilter("direction>=110")                    | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$gte', 110))                | 0
            "direction lt exists"                 | CQL.toFilter("direction<110")                     | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$lt', 110))                 | 1
            "direction lt doesn't exist"          | CQL.toFilter("direction<100")                     | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$lt', 100))                 | 0
            "direction lte exists"                | CQL.toFilter("direction<=110")                    | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$lte', 110))                | 1
            "direction lte doesn't exist"         | CQL.toFilter("direction<=100")                    | new BasicDBObject("calcDesign.structure.equipments.direction", new BasicDBObject('$lte', 100))                | 0

            "poleId that exists"                  | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                           | 1
            "poleId that doesn't exist"           | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                                    | 0
    }

    @Unroll("Test damage property query for #description")
    void "test damage property queries"() {
        setup:
            String typeName = "damage"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                       | filter                                            | expectedQuery                                                                                        | expectedSize
            "attachHeight that exists"        | CQL.toFilter("attachHeight=3")                    | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", 3)                              | 1
            "attachHeight that doesn't exist" | CQL.toFilter("attachHeight=2")                    | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", 2)                              | 0
            "attachHeight gt exists"          | CQL.toFilter("attachHeight>2")                    | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$gt', 2))    | 1
            "attachHeight gt doesn't exist"   | CQL.toFilter("attachHeight>4")                    | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$gt', 4))    | 0
            "attachHeight gte exists"         | CQL.toFilter("attachHeight>=3")                   | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$gte', 3))   | 1
            "attachHeight gte doesn't exist"  | CQL.toFilter("attachHeight>=25")                  | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$gte', 25))  | 0
            "attachHeight lt exists"          | CQL.toFilter("attachHeight<25")                   | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$lt', 25))   | 1
            "attachHeight lt doesn't exist"   | CQL.toFilter("attachHeight<2")                    | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$lt', 2))    | 0
            "attachHeight lte exists"         | CQL.toFilter("attachHeight<=3")                   | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$lte', 3))   | 1
            "attachHeight lte doesn't exist"  | CQL.toFilter("attachHeight<=2")                   | new BasicDBObject("calcDesign.structure.damages.attachHeight.value", new BasicDBObject('$lte', 2))   | 0

            "damageHeight that exists"        | CQL.toFilter("damageHeight=2.3")                  | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", 2.3)                            | 1
            "damageHeight that doesn't exist" | CQL.toFilter("damageHeight=31")                   | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", 31)                             | 0
            "damageHeight gt exists"          | CQL.toFilter("damageHeight>2")                    | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$gt', 2))    | 1
            "damageHeight gt doesn't exist"   | CQL.toFilter("damageHeight>4")                    | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$gt', 4))    | 0
            "damageHeight gte exists"         | CQL.toFilter("damageHeight>=2.3")                 | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$gte', 2.3)) | 1
            "damageHeight gte doesn't exist"  | CQL.toFilter("damageHeight>=25")                  | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$gte', 25))  | 0
            "damageHeight lt exists"          | CQL.toFilter("damageHeight<3")                    | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$lt', 3))    | 1
            "damageHeight lt doesn't exist"   | CQL.toFilter("damageHeight<2")                    | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$lt', 2))    | 0
            "damageHeight lte exists"         | CQL.toFilter("damageHeight<=2.3")                 | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$lte', 2.3)) | 1
            "damageHeight lte doesn't exist"  | CQL.toFilter("damageHeight<=2")                   | new BasicDBObject("calcDesign.structure.damages.damageHeight.value", new BasicDBObject('$lte', 2))   | 0

            "damageType that exists"          | CQL.toFilter("type='SLICE'")                      | new BasicDBObject("calcDesign.structure.damages.damageType", "SLICE")                                | 1
            "damageType that doesn't exist"   | CQL.toFilter("type='ARC'")                        | new BasicDBObject("calcDesign.structure.damages.damageType", "ARC")                                  | 0

            "direction that exists"           | CQL.toFilter("direction=299")                     | new BasicDBObject("calcDesign.structure.damages.direction", 299)                                     | 1
            "direction that doesn't exist"    | CQL.toFilter("direction=120")                     | new BasicDBObject("calcDesign.structure.damages.direction", 120)                                     | 0
            "direction gt exists"             | CQL.toFilter("direction>100")                     | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$gt', 100))           | 1
            "direction gt doesn't exist"      | CQL.toFilter("direction>300")                     | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$gt', 300))           | 0
            "direction gte exists"            | CQL.toFilter("direction>=299")                    | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$gte', 299))          | 1
            "direction gte doesn't exist"     | CQL.toFilter("direction>=300")                    | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$gte', 300))          | 0
            "direction lt exists"             | CQL.toFilter("direction<300")                     | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$lt', 300))           | 1
            "direction lt doesn't exist"      | CQL.toFilter("direction<100")                     | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$lt', 100))           | 0
            "direction lte exists"            | CQL.toFilter("direction<=299")                    | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$lte', 299))          | 1
            "direction lte doesn't exist"     | CQL.toFilter("direction<=100")                    | new BasicDBObject("calcDesign.structure.damages.direction", new BasicDBObject('$lte', 100))          | 0

            "width that exists"               | CQL.toFilter("width=7")                           | new BasicDBObject("calcDesign.structure.damages.width.value", 7)                                     | 1
            "width that doesn't exist"        | CQL.toFilter("width=120")                         | new BasicDBObject("calcDesign.structure.damages.width.value", 120)                                   | 0
            "width gt exists"                 | CQL.toFilter("width>6")                           | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$gt', 6))           | 1
            "width gt doesn't exist"          | CQL.toFilter("width>8")                           | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$gt', 8))           | 0
            "width gte exists"                | CQL.toFilter("width>=7")                          | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$gte', 7))          | 1
            "width gte doesn't exist"         | CQL.toFilter("width>=8")                          | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$gte', 8))          | 0
            "width lt exists"                 | CQL.toFilter("width<8")                           | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$lt', 8))           | 1
            "width lt doesn't exist"          | CQL.toFilter("width<6")                           | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$lt', 6))           | 0
            "width lte exists"                | CQL.toFilter("width<=7")                          | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$lte', 7))          | 1
            "width lte doesn't exist"         | CQL.toFilter("width<=5")                          | new BasicDBObject("calcDesign.structure.damages.width.value", new BasicDBObject('$lte', 5))          | 0

            "poleId that exists"              | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                  | 1
            "poleId that doesn't exist"       | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                           | 0
    }

    @Unroll("Test crossArm property query for #description")
    void "test crossArm property queries"() {
        setup:
            String typeName = "crossArm"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                            | filter                                               | expectedQuery                                                                                                             | expectedSize
            "owner that exists"                    | CQL.toFilter("owner='AEP'")                          | new BasicDBObject("calcDesign.structure.crossArms.owner.id", "AEP")                                                       | 1
            "owner that doesn't exist"             | CQL.toFilter("owner='TEST'")                         | new BasicDBObject("calcDesign.structure.crossArms.owner.id", "TEST")                                                      | 0

            "type that exists"                     | CQL.toFilter("type='8 Foot Cross Arm'")              | new BasicDBObject("calcDesign.structure.crossArms.clientItem", "8 Foot Cross Arm")                                        | 1
            "type that doesn't exist"              | CQL.toFilter("type='TEST'")                          | new BasicDBObject("calcDesign.structure.crossArms.clientItem", "TEST")                                                    | 0

            "attachHeight that exists"             | CQL.toFilter("attachmentHeight=32.666666666666664")  | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", 32.666666666666664)                            | 1
            "attachHeight that doesn't exist"      | CQL.toFilter("attachmentHeight=2")                   | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", 2)                                             | 0
            "attachHeight gt exists"               | CQL.toFilter("attachmentHeight>2")                   | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$gt', 2))                   | 1
            "attachHeight gt doesn't exist"        | CQL.toFilter("attachmentHeight>44")                  | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$gt', 44))                  | 0
            "attachHeight gte exists"              | CQL.toFilter("attachmentHeight>=32.666666666666664") | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$gte', 32.666666666666664)) | 1
            "attachHeight gte doesn't exist"       | CQL.toFilter("attachmentHeight>=33")                 | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$gte', 33))                 | 0
            "attachHeight lt exists"               | CQL.toFilter("attachmentHeight<34")                  | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$lt', 34))                  | 1
            "attachHeight lt doesn't exist"        | CQL.toFilter("attachmentHeight<22")                  | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$lt', 22))                  | 0
            "attachHeight lte exists"              | CQL.toFilter("attachmentHeight<=32.666666666666664") | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$lte', 32.666666666666664)) | 1
            "attachHeight lte doesn't exist"       | CQL.toFilter("attachmentHeight<=2")                  | new BasicDBObject("calcDesign.structure.crossArms.attachmentHeight.value", new BasicDBObject('$lte', 2))                  | 0

            "offset that exists"                   | CQL.toFilter("offset=48")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", 48)                                                      | 1
            "offset that doesn't exist"            | CQL.toFilter("offset=31")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", 31)                                                      | 0
            "offset gt exists"                     | CQL.toFilter("offset>40")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$gt', 40))                            | 1
            "offset gt doesn't exist"              | CQL.toFilter("offset>50")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$gt', 50))                            | 0
            "offset gte exists"                    | CQL.toFilter("offset>=48")                           | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$gte', 48))                           | 1
            "offset gte doesn't exist"             | CQL.toFilter("offset>=50")                           | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$gte', 50))                           | 0
            "offset lt exists"                     | CQL.toFilter("offset<50")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$lt', 50))                            | 1
            "offset lt doesn't exist"              | CQL.toFilter("offset<20")                            | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$lt', 20))                            | 0
            "offset lte exists"                    | CQL.toFilter("offset<=48")                           | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$lte', 48))                           | 1
            "offset lte doesn't exist"             | CQL.toFilter("offset<=10")                           | new BasicDBObject("calcDesign.structure.crossArms.offset.value", new BasicDBObject('$lte', 10))                           | 0

            "direction that exists"                | CQL.toFilter("direction=57")                         | new BasicDBObject("calcDesign.structure.crossArms.direction", 57)                                                         | 1
            "direction that doesn't exist"         | CQL.toFilter("direction=120")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", 120)                                                        | 0
            "direction gt exists"                  | CQL.toFilter("direction>20")                         | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$gt', 20))                               | 1
            "direction gt doesn't exist"           | CQL.toFilter("direction>300")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$gt', 300))                              | 0
            "direction gte exists"                 | CQL.toFilter("direction>=50")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$gte', 50))                              | 1
            "direction gte doesn't exist"          | CQL.toFilter("direction>=300")                       | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$gte', 300))                             | 0
            "direction lt exists"                  | CQL.toFilter("direction<300")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$lt', 300))                              | 1
            "direction lt doesn't exist"           | CQL.toFilter("direction<20")                         | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$lt', 20))                               | 0
            "direction lte exists"                 | CQL.toFilter("direction<=60")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$lte', 60))                              | 1
            "direction lte doesn't exist"          | CQL.toFilter("direction<=10")                        | new BasicDBObject("calcDesign.structure.crossArms.direction", new BasicDBObject('$lte', 10))                              | 0

            "associatedBacking that exists"        | CQL.toFilter("associatedBacking='Other'")            | new BasicDBObject("calcDesign.structure.crossArms.associatedBacking", "Other")                                            | 1
            "associatedBacking that doesn't exist" | CQL.toFilter("associatedBacking='TEST'")             | new BasicDBObject("calcDesign.structure.crossArms.associatedBacking", "TEST")                                             | 0

            "poleId that exists"                   | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'")    | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                                       | 1
            "poleId that doesn't exist"            | CQL.toFilter("poleId='Measured Design'")             | new BasicDBObject("id", "Measured Design")                                                                                | 0
    }

    @Unroll("Test anchor property query for #description")
    void "test anchor property queries"() {
        setup:
            String typeName = "anchor"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                      | filter                                            | expectedQuery                                                                                   | expectedSize
            "distance that exists"           | CQL.toFilter("distance=12")                       | new BasicDBObject("calcDesign.structure.anchors.distance.value", 12)                            | 1
            "distance that doesn't exist"    | CQL.toFilter("distance=2")                        | new BasicDBObject("calcDesign.structure.anchors.distance.value", 2)                             | 0
            "distance gt exists"             | CQL.toFilter("distance>5")                        | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$gt', 5))   | 1
            "distance gt doesn't exist"      | CQL.toFilter("distance>15")                       | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$gt', 15))  | 0
            "distance gte exists"            | CQL.toFilter("distance>=12")                      | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$gte', 12)) | 1
            "distance gte doesn't exist"     | CQL.toFilter("distance>=33")                      | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$gte', 33)) | 0
            "distance lt exists"             | CQL.toFilter("distance<34")                       | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$lt', 34))  | 1
            "distance lt doesn't exist"      | CQL.toFilter("distance<10")                       | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$lt', 10))  | 0
            "distance lte exists"            | CQL.toFilter("distance<=12")                      | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$lte', 12)) | 1
            "distance lte doesn't exist"     | CQL.toFilter("distance<=3")                       | new BasicDBObject("calcDesign.structure.anchors.distance.value", new BasicDBObject('$lte', 3))  | 0

            "direction that exists"          | CQL.toFilter("direction=122")                     | new BasicDBObject("calcDesign.structure.anchors.direction", 122)                                | 1
            "direction that doesn't exist"   | CQL.toFilter("direction=2")                       | new BasicDBObject("calcDesign.structure.anchors.direction", 2)                                  | 0
            "direction gt exists"            | CQL.toFilter("direction>5")                       | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$gt', 5))        | 1
            "direction gt doesn't exist"     | CQL.toFilter("direction>150")                     | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$gt', 150))      | 0
            "direction gte exists"           | CQL.toFilter("direction>=12")                     | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$gte', 12))      | 1
            "direction gte doesn't exist"    | CQL.toFilter("direction>=150")                    | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$gte', 150))     | 0
            "direction lt exists"            | CQL.toFilter("direction<125")                     | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$lt', 125))      | 1
            "direction lt doesn't exist"     | CQL.toFilter("direction<10")                      | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$lt', 10))       | 0
            "direction lte exists"           | CQL.toFilter("direction<=123")                    | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$lte', 123))     | 1
            "direction lte doesn't exist"    | CQL.toFilter("direction<=3")                      | new BasicDBObject("calcDesign.structure.anchors.direction", new BasicDBObject('$lte', 3))       | 0

            "owner that exists"              | CQL.toFilter("owner='AEP'")                       | new BasicDBObject("calcDesign.structure.anchors.owner.id", "AEP")                               | 1
            "owner that doesn't exist"       | CQL.toFilter("owner='TEST'")                      | new BasicDBObject("calcDesign.structure.anchors.owner.id", "TEST")                              | 0

            "height that exists"             | CQL.toFilter("height=0")                          | new BasicDBObject("calcDesign.structure.anchors.height.value", 0)                               | 1
            "height that doesn't exist"      | CQL.toFilter("height=2")                          | new BasicDBObject("calcDesign.structure.anchors.height.value", 2)                               | 0
            "height gt exists"               | CQL.toFilter("height>-1")                         | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$gt', -1))    | 1
            "height gt doesn't exist"        | CQL.toFilter("height>1")                          | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$gt', 1))     | 0
            "height gte exists"              | CQL.toFilter("height>=0")                         | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$gte', 0))    | 1
            "height gte doesn't exist"       | CQL.toFilter("height>=33")                        | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$gte', 33))   | 0
            "height lt exists"               | CQL.toFilter("height<34")                         | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$lt', 34))    | 1
            "height lt doesn't exist"        | CQL.toFilter("height<0")                          | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$lt', 0))     | 0
            "height lte exists"              | CQL.toFilter("height<=12")                        | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$lte', 12))   | 1
            "height lte doesn't exist"       | CQL.toFilter("height<=-1")                        | new BasicDBObject("calcDesign.structure.anchors.height.value", new BasicDBObject('$lte', -1))   | 0

            "supportType that exists"        | CQL.toFilter("supportType='Other'")               | new BasicDBObject("calcDesign.structure.anchors.supportType", "Other")                          | 1
            "supportType that doesn't exist" | CQL.toFilter("supportType='TEST'")                | new BasicDBObject("calcDesign.structure.anchors.supportType", "TEST")                           | 0

            "type that exists"               | CQL.toFilter("type='Single'")                     | new BasicDBObject("calcDesign.structure.anchors.clientItem", "Single")                          | 1
            "type that doesn't exist"        | CQL.toFilter("type='TEST'")                       | new BasicDBObject("calcDesign.structure.anchors.clientItem", "TEST")                            | 0

            "poleId that exists"             | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                             | 1
            "poleId that doesn't exist"      | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                      | 0
    }

    @Unroll("Test wireEndPoint property query for #description")
    void "test wireEndPoint property queries"() {
        setup:
            String typeName = "wireEndPoint"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                      | filter                                            | expectedQuery                                                                                            | expectedSize
            "distance that exists"           | CQL.toFilter("distance=62")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", 62)                               | 1
            "distance that doesn't exist"    | CQL.toFilter("distance=2")                        | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", 2)                                | 0
            "distance gt exists"             | CQL.toFilter("distance>5")                        | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$gt', 5))      | 1
            "distance gt doesn't exist"      | CQL.toFilter("distance>65")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$gt', 65))     | 0
            "distance gte exists"            | CQL.toFilter("distance>=62")                      | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$gte', 62))    | 1
            "distance gte doesn't exist"     | CQL.toFilter("distance>=65")                      | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$gte', 65))    | 0
            "distance lt exists"             | CQL.toFilter("distance<65")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$lt', 65))     | 1
            "distance lt doesn't exist"      | CQL.toFilter("distance<10")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$lt', 10))     | 0
            "distance lte exists"            | CQL.toFilter("distance<=65")                      | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$lte', 65))    | 1
            "distance lte doesn't exist"     | CQL.toFilter("distance<=3")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.distance.value", new BasicDBObject('$lte', 3))     | 0

            "direction that exists"          | CQL.toFilter("direction=302")                     | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", 302)                                   | 1
            "direction that doesn't exist"   | CQL.toFilter("direction=2")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", 2)                                     | 0
            "direction gt exists"            | CQL.toFilter("direction>5")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$gt', 5))           | 1
            "direction gt doesn't exist"     | CQL.toFilter("direction>302")                     | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$gt', 302))         | 0
            "direction gte exists"           | CQL.toFilter("direction>=302")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$gte', 302))        | 1
            "direction gte doesn't exist"    | CQL.toFilter("direction>=305")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$gte', 305))        | 0
            "direction lt exists"            | CQL.toFilter("direction<305")                     | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$lt', 305))         | 1
            "direction lt doesn't exist"     | CQL.toFilter("direction<10")                      | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$lt', 10))          | 0
            "direction lte exists"           | CQL.toFilter("direction<=302")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$lte', 302))        | 1
            "direction lte doesn't exist"    | CQL.toFilter("direction<=3")                      | new BasicDBObject("calcDesign.structure.wireEndPoints.direction", new BasicDBObject('$lte', 3))          | 0

            "inclination that exists"        | CQL.toFilter("inclination=0")                     | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", 0)                             | 1
            "inclination that doesn't exist" | CQL.toFilter("inclination=2")                     | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", 2)                             | 0
            "inclination gt exists"          | CQL.toFilter("inclination>-1")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$gt', -1))  | 1
            "inclination gt doesn't exist"   | CQL.toFilter("inclination>65")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$gt', 65))  | 0
            "inclination gte exists"         | CQL.toFilter("inclination>=0")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$gte', 0))  | 1
            "inclination gte doesn't exist"  | CQL.toFilter("inclination>=65")                   | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$gte', 65)) | 0
            "inclination lt exists"          | CQL.toFilter("inclination<65")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$lt', 65))  | 1
            "inclination lt doesn't exist"   | CQL.toFilter("inclination<-1")                    | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$lt', -1))  | 0
            "inclination lte exists"         | CQL.toFilter("inclination<=65")                   | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$lte', 65)) | 1
            "inclination lte doesn't exist"  | CQL.toFilter("inclination<=-1")                   | new BasicDBObject("calcDesign.structure.wireEndPoints.inclination.value", new BasicDBObject('$lte', -1)) | 0

            "type that exists"               | CQL.toFilter("type='PREVIOUS_POLE'")              | new BasicDBObject("calcDesign.structure.wireEndPoints.type", "PREVIOUS_POLE")                            | 1
            "type that doesn't exist"        | CQL.toFilter("type='TEST'")                       | new BasicDBObject("calcDesign.structure.wireEndPoints.type", "TEST")                                     | 0

            "comments that exists"           | CQL.toFilter("comments='someComments'")           | new BasicDBObject("calcDesign.structure.wireEndPoints.comments", "someComments")                         | 1
            "comments that doesn't exist"    | CQL.toFilter("comments='TEST'")                   | new BasicDBObject("calcDesign.structure.wireEndPoints.comments", "TEST")                                 | 0

            "poleId that exists"             | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                      | 1
            "poleId that doesn't exist"      | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                               | 0
    }

    @Unroll("Test notePoint property query for #description")
    void "test notePoints property queries"() {
        setup:
            String typeName = "notePoint"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                    | filter                                            | expectedQuery                                                                                      | expectedSize
            "distance that exists"         | CQL.toFilter("distance=62")                       | new BasicDBObject("calcDesign.structure.notePoints.distance.value", 62)                            | 1
            "distance that doesn't exist"  | CQL.toFilter("distance=2")                        | new BasicDBObject("calcDesign.structure.notePoints.distance.value", 2)                             | 0
            "distance gt exists"           | CQL.toFilter("distance>5")                        | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$gt', 5))   | 1
            "distance gt doesn't exist"    | CQL.toFilter("distance>65")                       | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$gt', 65))  | 0
            "distance gte exists"          | CQL.toFilter("distance>=62")                      | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$gte', 62)) | 1
            "distance gte doesn't exist"   | CQL.toFilter("distance>=65")                      | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$gte', 65)) | 0
            "distance lt exists"           | CQL.toFilter("distance<65")                       | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$lt', 65))  | 1
            "distance lt doesn't exist"    | CQL.toFilter("distance<10")                       | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$lt', 10))  | 0
            "distance lte exists"          | CQL.toFilter("distance<=65")                      | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$lte', 65)) | 1
            "distance lte doesn't exist"   | CQL.toFilter("distance<=3")                       | new BasicDBObject("calcDesign.structure.notePoints.distance.value", new BasicDBObject('$lte', 3))  | 0

            "direction that exists"        | CQL.toFilter("direction=302")                     | new BasicDBObject("calcDesign.structure.notePoints.direction", 302)                                | 1
            "direction that doesn't exist" | CQL.toFilter("direction=2")                       | new BasicDBObject("calcDesign.structure.notePoints.direction", 2)                                  | 0
            "direction gt exists"          | CQL.toFilter("direction>5")                       | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$gt', 5))        | 1
            "direction gt doesn't exist"   | CQL.toFilter("direction>302")                     | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$gt', 302))      | 0
            "direction gte exists"         | CQL.toFilter("direction>=302")                    | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$gte', 302))     | 1
            "direction gte doesn't exist"  | CQL.toFilter("direction>=305")                    | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$gte', 305))     | 0
            "direction lt exists"          | CQL.toFilter("direction<305")                     | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$lt', 305))      | 1
            "direction lt doesn't exist"   | CQL.toFilter("direction<10")                      | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$lt', 10))       | 0
            "direction lte exists"         | CQL.toFilter("direction<=302")                    | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$lte', 302))     | 1
            "direction lte doesn't exist"  | CQL.toFilter("direction<=3")                      | new BasicDBObject("calcDesign.structure.notePoints.direction", new BasicDBObject('$lte', 3))       | 0

            "note that exists"             | CQL.toFilter("note='Residential Driveway'")       | new BasicDBObject("calcDesign.structure.notePoints.note", "Residential Driveway")                  | 1
            "note that doesn't exist"      | CQL.toFilter("note='TEST'")                       | new BasicDBObject("calcDesign.structure.notePoints.note", "TEST")                                  | 0

            "poleId that exists"           | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                | 1
            "poleId that doesn't exist"    | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                         | 0
    }

    @Unroll("Test pointLoad property query for #description")
    void "test pointLoad property queries"() {
        setup:
            String typeName = "pointLoad"
            String collectionName = "designs"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            BasicDBObject dbQuery = filterToDBQuery.visit(filter, null)
        then:
            dbQuery == expectedQuery
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == expectedSize
        where:
            description                           | filter                                            | expectedQuery                                                                                                | expectedSize
            "owner that exists"                   | CQL.toFilter("owner='SCE'")                       | new BasicDBObject("calcDesign.structure.pointLoads.owner.id", "SCE")                                         | 1
            "owner that doesn't exist"            | CQL.toFilter("owner='TEST'")                      | new BasicDBObject("calcDesign.structure.pointLoads.owner.id", "TEST")                                        | 0

            "elevation that exists"               | CQL.toFilter("elevation=0")                       | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", 0)                                      | 1
            "elevation that doesn't exist"        | CQL.toFilter("elevation=2")                       | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", 2)                                      | 0
            "elevation gt exists"                 | CQL.toFilter("elevation>-1")                      | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$gt', -1))           | 1
            "elevation gt doesn't exist"          | CQL.toFilter("elevation>1")                       | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$gt', 1))            | 0
            "elevation gte exists"                | CQL.toFilter("elevation>=0")                      | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$gte', 0))           | 1
            "elevation gte doesn't exist"         | CQL.toFilter("elevation>=33")                     | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$gte', 33))          | 0
            "elevation lt exists"                 | CQL.toFilter("elevation<34")                      | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$lt', 34))           | 1
            "elevation lt doesn't exist"          | CQL.toFilter("elevation<0")                       | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$lt', 0))            | 0
            "elevation lte exists"                | CQL.toFilter("elevation<=12")                     | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$lte', 12))          | 1
            "elevation lte doesn't exist"         | CQL.toFilter("elevation<=-1")                     | new BasicDBObject("calcDesign.structure.pointLoads.elevation.value", new BasicDBObject('$lte', -1))          | 0

            "attachmentHeight that exists"        | CQL.toFilter("attachmentHeight=32")               | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", 32)                              | 1
            "attachmentHeight that doesn't exist" | CQL.toFilter("attachmentHeight=2")                | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", 2)                               | 0
            "attachmentHeight gt exists"          | CQL.toFilter("attachmentHeight>5")                | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$gt', 5))     | 1
            "attachmentHeight gt doesn't exist"   | CQL.toFilter("attachmentHeight>65")               | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$gt', 65))    | 0
            "attachmentHeight gte exists"         | CQL.toFilter("attachmentHeight>=32")              | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$gte', 32))   | 1
            "attachmentHeight gte doesn't exist"  | CQL.toFilter("attachmentHeight>=65")              | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$gte', 65))   | 0
            "attachmentHeight lt exists"          | CQL.toFilter("attachmentHeight<65")               | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$lt', 65))    | 1
            "attachmentHeight lt doesn't exist"   | CQL.toFilter("attachmentHeight<10")               | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$lt', 10))    | 0
            "attachmentHeight lte exists"         | CQL.toFilter("attachmentHeight<=65")              | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$lte', 65))   | 1
            "attachmentHeight lte doesn't exist"  | CQL.toFilter("attachmentHeight<=3")               | new BasicDBObject("calcDesign.structure.pointLoads.attachmentHeight.value", new BasicDBObject('$lte', 3))    | 0

            "rotation that exists"                | CQL.toFilter("rotation=0")                        | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", 0)                                       | 1
            "rotation that doesn't exist"         | CQL.toFilter("rotation=2")                        | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", 2)                                       | 0
            "rotation gt exists"                  | CQL.toFilter("rotation>-1")                       | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$gt', -1))            | 1
            "rotation gt doesn't exist"           | CQL.toFilter("rotation>1")                        | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$gt', 1))             | 0
            "rotation gte exists"                 | CQL.toFilter("rotation>=0")                       | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$gte', 0))            | 1
            "rotation gte doesn't exist"          | CQL.toFilter("rotation>=33")                      | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$gte', 33))           | 0
            "rotation lt exists"                  | CQL.toFilter("rotation<34")                       | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$lt', 34))            | 1
            "rotation lt doesn't exist"           | CQL.toFilter("rotation<0")                        | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$lt', 0))             | 0
            "rotation lte exists"                 | CQL.toFilter("rotation<=12")                      | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$lte', 12))           | 1
            "rotation lte doesn't exist"          | CQL.toFilter("rotation<=-1")                      | new BasicDBObject("calcDesign.structure.pointLoads.rotation.value", new BasicDBObject('$lte', -1))           | 0

            "x that exists"                       | CQL.toFilter("x=0")                               | new BasicDBObject("calcDesign.structure.pointLoads.x.value", 0)                                              | 1
            "x that doesn't exist"                | CQL.toFilter("x=2")                               | new BasicDBObject("calcDesign.structure.pointLoads.x.value", 2)                                              | 0
            "x gt exists"                         | CQL.toFilter("x>-1")                              | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$gt', -1))                   | 1
            "x gt doesn't exist"                  | CQL.toFilter("x>1")                               | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$gt', 1))                    | 0
            "x gte exists"                        | CQL.toFilter("x>=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$gte', 0))                   | 1
            "x gte doesn't exist"                 | CQL.toFilter("x>=33")                             | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$gte', 33))                  | 0
            "x lt exists"                         | CQL.toFilter("x<34")                              | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$lt', 34))                   | 1
            "x lt doesn't exist"                  | CQL.toFilter("x<0")                               | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$lt', 0))                    | 0
            "x lte exists"                        | CQL.toFilter("x<=12")                             | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$lte', 12))                  | 1
            "x lte doesn't exist"                 | CQL.toFilter("x<=-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.x.value", new BasicDBObject('$lte', -1))                  | 0

            "y that exists"                       | CQL.toFilter("y=0")                               | new BasicDBObject("calcDesign.structure.pointLoads.y.value", 0)                                              | 1
            "y that doesn't exist"                | CQL.toFilter("y=2")                               | new BasicDBObject("calcDesign.structure.pointLoads.y.value", 2)                                              | 0
            "y gt exists"                         | CQL.toFilter("y>-1")                              | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$gt', -1))                   | 1
            "y gt doesn't exist"                  | CQL.toFilter("y>1")                               | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$gt', 1))                    | 0
            "y gte exists"                        | CQL.toFilter("y>=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$gte', 0))                   | 1
            "y gte doesn't exist"                 | CQL.toFilter("y>=33")                             | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$gte', 33))                  | 0
            "y lt exists"                         | CQL.toFilter("y<34")                              | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$lt', 34))                   | 1
            "y lt doesn't exist"                  | CQL.toFilter("y<0")                               | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$lt', 0))                    | 0
            "y lte exists"                        | CQL.toFilter("y<=12")                             | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$lte', 12))                  | 1
            "y lte doesn't exist"                 | CQL.toFilter("y<=-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.y.value", new BasicDBObject('$lte', -1))                  | 0

            "z that exists"                       | CQL.toFilter("z=32.666666666666664")              | new BasicDBObject("calcDesign.structure.pointLoads.z.value", 32.666666666666664)                             | 1
            "z that doesn't exist"                | CQL.toFilter("z=2")                               | new BasicDBObject("calcDesign.structure.pointLoads.z.value", 2)                                              | 0
            "z gt exists"                         | CQL.toFilter("z>1")                               | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$gt', 1))                    | 1
            "z gt doesn't exist"                  | CQL.toFilter("z>50")                              | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$gt', 50))                   | 0
            "z gte exists"                        | CQL.toFilter("z>=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$gte', 0))                   | 1
            "z gte doesn't exist"                 | CQL.toFilter("z>=33")                             | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$gte', 33))                  | 0
            "z lt exists"                         | CQL.toFilter("z<34")                              | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$lt', 34))                   | 1
            "z lt doesn't exist"                  | CQL.toFilter("z<0")                               | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$lt', 0))                    | 0
            "z lte exists"                        | CQL.toFilter("z<=32.666666666666664")             | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$lte', 32.666666666666664))  | 1
            "z lte doesn't exist"                 | CQL.toFilter("z<=-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.z.value", new BasicDBObject('$lte', -1))                  | 0

            "fx that exists"                      | CQL.toFilter("fx=73.92457179425563")              | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", 73.92457179425563)                             | 1
            "fx that doesn't exist"               | CQL.toFilter("fx=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", 2)                                             | 0
            "fx gt exists"                        | CQL.toFilter("fx>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$gt', 1))                   | 1
            "fx gt doesn't exist"                 | CQL.toFilter("fx>75")                             | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$gt', 75))                  | 0
            "fx gte exists"                       | CQL.toFilter("fx>=0")                             | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$gte', 0))                  | 1
            "fx gte doesn't exist"                | CQL.toFilter("fx>=75")                            | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$gte', 75))                 | 0
            "fx lt exists"                        | CQL.toFilter("fx<75")                             | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$lt', 75))                  | 1
            "fx lt doesn't exist"                 | CQL.toFilter("fx<0")                              | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$lt', 0))                   | 0
            "fx lte exists"                       | CQL.toFilter("fx<=75")                            | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$lte', 75))                 | 1
            "fx lte doesn't exist"                | CQL.toFilter("fx<=-1")                            | new BasicDBObject("calcDesign.structure.pointLoads.fx.value", new BasicDBObject('$lte', -1))                 | 0

            "fy that exists"                      | CQL.toFilter("fy=241.92570549706124")             | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", 241.92570549706124)                            | 1
            "fy that doesn't exist"               | CQL.toFilter("fy=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", 2)                                             | 0
            "fy gt exists"                        | CQL.toFilter("fy>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$gt', 1))                   | 1
            "fy gt doesn't exist"                 | CQL.toFilter("fy>250")                            | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$gt', 250))                 | 0
            "fy gte exists"                       | CQL.toFilter("fy>=0")                             | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$gte', 0))                  | 1
            "fy gte doesn't exist"                | CQL.toFilter("fy>=330")                           | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$gte', 330))                | 0
            "fy lt exists"                        | CQL.toFilter("fy<250")                            | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$lt', 250))                 | 1
            "fy lt doesn't exist"                 | CQL.toFilter("fy<0")                              | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$lt', 0))                   | 0
            "fy lte exists"                       | CQL.toFilter("fy<=250")                           | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$lte', 250))                | 1
            "fy lte doesn't exist"                | CQL.toFilter("fy<=1")                             | new BasicDBObject("calcDesign.structure.pointLoads.fy.value", new BasicDBObject('$lte', 1))                  | 0

            "fz that exists"                      | CQL.toFilter("fz=-7.177126069505941")             | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", -7.177126069505941)                            | 1
            "fz that doesn't exist"               | CQL.toFilter("fz=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", 2)                                             | 0
            "fz gt exists"                        | CQL.toFilter("fz>-8")                             | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$gt', -8))                  | 1
            "fz gt doesn't exist"                 | CQL.toFilter("fz>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$gt', 1))                   | 0
            "fz gte exists"                       | CQL.toFilter("fz>=-7.177126069505941")            | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$gte', -7.177126069505941)) | 1
            "fz gte doesn't exist"                | CQL.toFilter("fz>=33")                            | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$gte', 33))                 | 0
            "fz lt exists"                        | CQL.toFilter("fz<34")                             | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$lt', 34))                  | 1
            "fz lt doesn't exist"                 | CQL.toFilter("fz<-8")                             | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$lt', -8))                  | 0
            "fz lte exists"                       | CQL.toFilter("fz<=12")                            | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$lte', 12))                 | 1
            "fz lte doesn't exist"                | CQL.toFilter("fz<=-9")                            | new BasicDBObject("calcDesign.structure.pointLoads.fz.value", new BasicDBObject('$lte', -9))                 | 0

            "mx that exists"                      | CQL.toFilter("mx=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", 0)                                             | 1
            "mx that doesn't exist"               | CQL.toFilter("mx=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", 2)                                             | 0
            "mx gt exists"                        | CQL.toFilter("mx>-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$gt', -1))                  | 1
            "mx gt doesn't exist"                 | CQL.toFilter("mx>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$gt', 1))                   | 0
            "mx gte exists"                       | CQL.toFilter("mx>=0")                             | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$gte', 0))                  | 1
            "mx gte doesn't exist"                | CQL.toFilter("mx>=33")                            | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$gte', 33))                 | 0
            "mx lt exists"                        | CQL.toFilter("mx<34")                             | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$lt', 34))                  | 1
            "mx lt doesn't exist"                 | CQL.toFilter("mx<0")                              | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$lt', 0))                   | 0
            "mx lte exists"                       | CQL.toFilter("mx<=12")                            | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$lte', 12))                 | 1
            "mx lte doesn't exist"                | CQL.toFilter("mx<=-1")                            | new BasicDBObject("calcDesign.structure.pointLoads.mx.value", new BasicDBObject('$lte', -1))                 | 0

            "my that exists"                      | CQL.toFilter("my=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.my.value", 0)                                             | 1
            "my that doesn't exist"               | CQL.toFilter("my=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.my.value", 2)                                             | 0
            "my gt exists"                        | CQL.toFilter("my>-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$gt', -1))                  | 1
            "my gt doesn't exist"                 | CQL.toFilter("my>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$gt', 1))                   | 0
            "my gte exists"                       | CQL.toFilter("my>=0")                             | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$gte', 0))                  | 1
            "my gte doesn't exist"                | CQL.toFilter("my>=33")                            | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$gte', 33))                 | 0
            "my lt exists"                        | CQL.toFilter("my<34")                             | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$lt', 34))                  | 1
            "my lt doesn't exist"                 | CQL.toFilter("my<0")                              | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$lt', 0))                   | 0
            "my lte exists"                       | CQL.toFilter("my<=12")                            | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$lte', 12))                 | 1
            "my lte doesn't exist"                | CQL.toFilter("my<=-1")                            | new BasicDBObject("calcDesign.structure.pointLoads.my.value", new BasicDBObject('$lte', -1))                 | 0

            "mz that exists"                      | CQL.toFilter("mz=0")                              | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", 0)                                             | 1
            "mz that doesn't exist"               | CQL.toFilter("mz=2")                              | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", 2)                                             | 0
            "mz gt exists"                        | CQL.toFilter("mz>-1")                             | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$gt', -1))                  | 1
            "mz gt doesn't exist"                 | CQL.toFilter("mz>1")                              | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$gt', 1))                   | 0
            "mz gte exists"                       | CQL.toFilter("mz>=0")                             | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$gte', 0))                  | 1
            "mz gte doesn't exist"                | CQL.toFilter("mz>=33")                            | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$gte', 33))                 | 0
            "mz lt exists"                        | CQL.toFilter("mz<34")                             | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$lt', 34))                  | 1
            "mz lt doesn't exist"                 | CQL.toFilter("mz<0")                              | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$lt', 0))                   | 0
            "mz lte exists"                       | CQL.toFilter("mz<=12")                            | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$lte', 12))                 | 1
            "mz lte doesn't exist"                | CQL.toFilter("mz<=-1")                            | new BasicDBObject("calcDesign.structure.pointLoads.mz.value", new BasicDBObject('$lte', -1))                 | 0

            "poleId that exists"                  | CQL.toFilter("poleId='56e9b7137d84511d8dd0f13c'") | new BasicDBObject("id", "56e9b7137d84511d8dd0f13c")                                                          | 1
            "poleId that doesn't exist"           | CQL.toFilter("poleId='Measured Design'")          | new BasicDBObject("id", "Measured Design")                                                                   | 0
    }

    void "test PropertyIsBetween query"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            def filterFactory = new FilterFactoryImpl()
            def filter = filterFactory.between(filterFactory.literal("dateModified"), filterFactory.literal(1442498557078), filterFactory.literal(1442498557080))
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 1
            featureCollection.size() == 1
    }

    void "test PropertyIsLike query "() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            Query query = new Query(typeName, filter)
            FilterToDBQuery filterToDBQuery = getFilterToDBQuery(typeName, collectionName)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == expectedResultsSize
            featureCollection.size() == expectedResultsSize
        where:
            filter                                                                | expectedResultsSize
            CQL.toFilter("comments like '%transformers connected to lower two%'") | 1
            CQL.toFilter("comments like 'Two transformers connected%'")           | 1
            CQL.toFilter("comments like '%connected to lower two cross arms'")    | 1
            CQL.toFilter("comments like '%connected to lower two cross arm'")     | 0
    }

    private FilterToDBQuery getFilterToDBQuery(String typeName, String collectionName) {
        DBCollection dbCollection = database.getCollection(collectionName)
        FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
        BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
        mongoDBDataAccess = new MongoDBDataAccess(namespace, System.getProperty("mongoHost"), System.getProperty("mongoPort"), System.getProperty("mongoDatabase"), null, null, jsonMapping)
        mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType, mapping)
        return new FilterToDBQuery(dbCollection, featureType, mapping, mongoDBFeatureSource)
    }

    void "test location bounding box query"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            def minLng = -119
            def maxLng = -118
            def minLat = 33
            def maxLat = 34
            def filter = CQL.toFilter("BBOX(geographicCoordinate,$minLng,$minLat,$maxLng,$maxLat,'EPSG:4326')")
            Query query = new Query(typeName, filter)
            DBCollection dbCollection = database.getCollection(collectionName)
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
            BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
            FilterToDBQuery filterToDBQuery = new FilterToDBQuery(dbCollection, featureType, mapping, mongoDBFeatureSource)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 1
            featureCollection.size() == 1
        when:
            minLng = -120
            maxLng = -119
            minLat = 31
            maxLat = 32

            filter = CQL.toFilter("BBOX(geographicCoordinate,$minLng,$minLat,$maxLng,$maxLat,'EPSG:4326')")
            query = new Query(typeName, filter)
            featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 0
            featureCollection.size() == 0
    }

    void "test location limit and offset"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            def otherLocations = []
            (1..9).each { i ->
                BasicDBObject clonedLocation = locationJSON.clone()
                clonedLocation.put("id", UUID.randomUUID())
                clonedLocation.remove("_id")
                otherLocations << clonedLocation
                database.getCollection(collectionName).insert(clonedLocation)
            }
            Query query = new Query(typeName, Filter.INCLUDE, 5, Query.ALL_NAMES, null)
            query.setStartIndex(0)
            DBCollection dbCollection = database.getCollection(collectionName)
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
            BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
            FilterToDBQuery filterToDBQuery = new FilterToDBQuery(dbCollection, featureType, mapping, mongoDBFeatureSource)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.size() == 5
        when:
            query = new Query(typeName, Filter.INCLUDE, 5, Query.ALL_NAMES, null)
            query.setStartIndex(5)
            FeatureIterator featureIterator = featureCollection.features()
            def firstFiveLocationIds = []
            while (featureIterator.hasNext()) {
                firstFiveLocationIds << featureIterator.next().getProperty("id").value
            }
            featureCollection = filterToDBQuery.getFeatureCollection(query)
            int featureCollectionSize = featureCollection.size()
            def nextFiveLocationIds = []
            featureIterator = featureCollection.features()
            while (featureIterator.hasNext()) {
                nextFiveLocationIds << featureIterator.next().getProperty("id").value
            }
        then:
            featureCollectionSize == 5
            nextFiveLocationIds.every { !firstFiveLocationIds.contains(it) }
        cleanup:
            otherLocations.each { BasicDBObject location ->
                database?.getCollection(collectionName)?.remove(new BasicDBObject("id", location.get("id")))
            }
    }

    void "test AND query"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            def filter = CQL.toFilter("id='55fac7fde4b0e7f2e3be342c' AND dateModified=1442498557079 AND clientFile='SCE.client'")
            Query query = new Query(typeName, filter)
            DBCollection dbCollection = database.getCollection(collectionName)
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
            BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
            FilterToDBQuery filterToDBQuery = new FilterToDBQuery(dbCollection, featureType, mapping, mongoDBFeatureSource)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 1
            featureCollection.size() == 1
        when:
            filter = CQL.toFilter("id='TEST' AND dateModified=1442498557079 AND clientFile='SCE.client'")
            query = new Query(typeName, filter)
            featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 0
            featureCollection.size() == 0
    }

    void "test OR query"() {
        setup:
            String typeName = "location"
            String collectionName = "locations"
            BasicDBObject clonedLocation = locationJSON.clone()
            String otherId = UUID.randomUUID()
            clonedLocation.put("id", otherId)
            clonedLocation.remove("_id")
            database.getCollection(collectionName).insert(clonedLocation)
            def filter = CQL.toFilter("id='55fac7fde4b0e7f2e3be342c' OR id='${otherId}'")
            Query query = new Query(typeName, filter)
            DBCollection dbCollection = database.getCollection(collectionName)
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
            BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
            FilterToDBQuery filterToDBQuery = new FilterToDBQuery(dbCollection, featureType, mapping, mongoDBFeatureSource)
        when:
            FeatureCollection featureCollection = filterToDBQuery.getFeatureCollection(query)
        then:
            featureCollection.dbCursor.size() == 2
            featureCollection.size() == 2
        cleanup:
            database?.getCollection(collectionName)?.remove(new BasicDBObject("id", otherId))
    }
}
