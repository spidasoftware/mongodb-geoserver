package com.spidasoftware.mongodb.feature.collection

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCursor
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.util.JSON
import com.spidasoftware.mongodb.data.MongoDBDataAccess
import com.spidasoftware.mongodb.data.MongoDBFeatureSource
import org.geotools.data.Query
import org.geotools.feature.NameImpl
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType
import org.opengis.feature.simple.SimpleFeature

import org.opengis.filter.Filter
import spock.lang.Shared
import spock.lang.Specification
import org.geotools.filter.text.cql2.CQL
import java.util.logging.Logger

class MongoDBSubCollectionFeatureCollectionSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBSubCollectionFeatureCollectionSpec.class.getPackage().getName())

    @Shared DB database
    @Shared BasicDBObject locationJSON
    @Shared BasicDBObject designJSON
    @Shared BasicDBList jsonMapping
    @Shared MongoDBDataAccess mongoDBDataAccess
    @Shared String namespace = "http://spida/db"
    MongoDBFeatureSource mongoDBFeatureSource

    void setup() {
        locationJSON = JSON.parse(getClass().getResourceAsStream('/location.json').text)
        designJSON = JSON.parse(getClass().getResourceAsStream('/design.json').text)

        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        mongoDBDataAccess = new MongoDBDataAccess(namespace, System.getProperty("mongoHost"), System.getProperty("mongoPort"), System.getProperty("mongoDatabase"), null, null, jsonMapping)

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

    void cleanup() {
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("designs").remove(new BasicDBObject("id", designJSON.get("id")))
        database.getCollection("designs").remove(new BasicDBObject("id", "5b216ef1cff47e0001b9d9f4"))
        database.getCollection("designs").remove(new BasicDBObject("id", "5b216ef1cff47e0001b9d9f5"))
    }

    void testNoAnalysisForLocation() { // Test that we don't NullPointerException when a design doesn't have analysis
        setup:
            BasicDBObject designWithAnalysis = JSON.parse("""{ "dateModified" : 1528917745615, "id" : "5b216ef1cff47e0001b9d9f4", "locationLabel" : "Empty", "locationId" : "5b216ef1cff47e0001b9d9f6", "projectLabel" : "New Project_Empty Design Layer AS", "projectId" : "5b216ef1cff47e0001b9d9f7", "clientFile" : "Getting Started_v7.X.client", "clientFileVersion" : "f2a20ab17b9863d90884b0a65d2f6365", "calcDesign" : { "label" : "Measured Design", "layerType" : "Measured", "structure" : { "pole" : { "glc" : { "unit" : "METRE", "value" : 1.1246310380696452 }, "agl" : { "unit" : "METRE", "value" : 15.849599999999999 }, "environment" : "NONE", "temperature" : { "unit" : "CELSIUS", "value" : 15.555555555555555 }, "stressRatio" : 1, "leanAngle" : 0, "leanDirection" : 0, "clientItemVersion" : "611f76aa024d22a49fbcab865d4a3a5a", "clientItem" : { "species" : "Southern Pine", "classOfPole" : "2", "height" : { "unit" : "METRE", "value" : 18.288 } }, "externalId" : "5b216d8b8b721ed0b12182bb", "owner" : { "id" : "Acme Power", "industry" : "UTILITY", "externalId" : "43e130a1-3c21-41c9-b420-0e5b966eb2f2" } }, "wireEndPoints" : [ ], "spanPoints" : [ ], "anchors" : [ ], "notePoints" : [ ], "pointLoads" : [ ], "wirePointLoads" : [ ], "damages" : [ ], "wires" : [ ], "spanGuys" : [ ], "guys" : [ ], "equipments" : [ ], "guyAttachPoints" : [ ], "crossArms" : [ ], "insulators" : [ ], "pushBraces" : [ ], "sidewalkBraces" : [ ], "foundations" : [ ], "assemblies" : [ ] }, "mapLocation" : { "type" : "Point", "coordinates" : [ -83.01508247852325, 40.100811662473994 ] }, "analysis" : [ { "id" : "Medium", "results" : [ { "actual" : 5.37301105123481, "allowable" : 100, "unit" : "PERCENT", "analysisDate" :1528917746774, "component" : "Pole", "loadInfo" : "Medium", "passes" : true, "analysisType" : "STRESS" } ] }, { "id" : "NESC ZONE", "results" : [ ] } ], "version" : 5, "id" : "5b216ef1cff47e0001b9d9f4", "schema" : "/schema/spidacalc/calc/design.schema" }, "worstCaseAnalysisResults" : { "pole" : { "actual" : 5.37301105123481, "allowable" : 100, "unit" : "PERCENT", "analysisDate" : 1528917746774, "component" : "Pole", "loadInfo" : "Medium", "passes" : true, "analysisType" : "STRESS" } }, "user" : { "id" : "965", "email" : "amber.schmiesing@spidasoftware.com" }, "analysisSummary" : [ { "id" : "Medium", "results" : [ { "actual" : 5.37301105123481, "allowable" : 100, "unit" : "PERCENT", "analysisDate" : 1528917746774, "component" : "Pole", "loadInfo" : "Medium", "passes" : true, "analysisType" : "STRESS" } ] } ] }""")
            BasicDBObject designNoAnalysis = JSON.parse("""{  "dateModified" : 1528917745616, "id" : "5b216ef1cff47e0001b9d9f5", "locationLabel" : "Empty", "locationId" : "5b216ef1cff47e0001b9d9f6", "projectLabel" : "New Project_Empty Design Layer AS", "projectId" : "5b216ef1cff47e0001b9d9f7", "clientFile" : "Getting Started_v7.X.client", "clientFileVersion" : "f2a20ab17b9863d90884b0a65d2f6365", "calcDesign" : { "label" : "Theoretical Design", "layerType" : "Theoretical", "version" : 5, "id" : "5b216ef1cff47e0001b9d9f5", "schema" : "/schema/spidacalc/calc/design.schema" }, "user" : { "id" : "965", "email" : "amber.schmiesing@spidasoftware.com" } }""")
            database.getCollection("designs").insert(designWithAnalysis)
            database.getCollection("designs").insert(designNoAnalysis)
            DBCursor dbCursor = database.getCollection("designs").find(new BasicDBObject("locationId", "5b216ef1cff47e0001b9d9f6"))
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "analysis"))
            Query query = new Query("analysis", CQL.toFilter("locationId='5b216ef1cff47e0001b9d9f6'"), [])
            BasicDBObject mapping = jsonMapping.find { it.typeName == "analysis" }
            mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType, mapping)
            def mongoDBSubCollectionFeatureCollection = new MongoDBSubCollectionFeatureCollection(dbCursor, featureType, mapping, query, mongoDBFeatureSource)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.getAttribute("actual") == 5.37301105123481
    }

    void testGetPole() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("pole", "designs", CQL.toFilter("designType='Measured Design'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 16
            feature.getAttribute("designType") == "Measured Design"
            feature.getAttribute("locationLabel") == "684704E"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("clientFile") == "Acme Power.client"
            feature.getAttribute("clientFileVersion") == "6ee5fba14760878be22701e1b3b7c05b"
            feature.getAttribute("dateModified") == 1442498557079
            feature.getAttribute("glc") == 2.8990375130504664
            feature.getAttribute("glcUnit") == "FOOT"
            feature.getAttribute("agl") == 38.5
            feature.getAttribute("aglUnit") == "FOOT"
            feature.getAttribute("species") == "Southern Yellow Pine"
            feature.getAttribute("class") == "4"
            feature.getAttribute("length") == 45
            feature.getAttribute("lengthUnit") == "FOOT"
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c"
    }

    void testGetAnalysis() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("analysis", "designs", CQL.toFilter("actual=1.5677448671814123"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 25
            feature.getAttribute("designType") == "Measured Design"
            feature.getAttribute("loadInfo") == "CSA Heavy"
            feature.getAttribute("locationLabel") == "684704E"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("clientFile") == "Acme Power.client"
            feature.getAttribute("clientFileVersion") == "6ee5fba14760878be22701e1b3b7c05b"
            feature.getAttribute("dateModified") == 1442498557079
            feature.getAttribute("glc") == 2.8990375130504664
            feature.getAttribute("glcUnit") == "FOOT"
            feature.getAttribute("agl") == 38.5
            feature.getAttribute("aglUnit") == "FOOT"
            feature.getAttribute("species") == "Southern Yellow Pine"
            feature.getAttribute("class") == "4"
            feature.getAttribute("length") == 45
            feature.getAttribute("lengthUnit") == "FOOT"
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("actual") == 1.5677448671814123
            feature.getAttribute("allowable") == 100
            feature.getAttribute("unit") == "PERCENT"
            feature.getAttribute("analysisDate") == 1446037442824
            feature.getAttribute("component") ==  "Pole"
            feature.getAttribute("analysisType") ==  "BUCKLING"
            feature.getAttribute("passes") == true
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0_0"
    }

    void testGetAllPoles() {
        when:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("pole", "designs", Filter.INCLUDE)
        then:
            mongoDBSubCollectionFeatureCollection.size() == 1
    }

    void testGetAllAnalysis() {
        when:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("analysis", "designs", Filter.INCLUDE)
        then:
            mongoDBSubCollectionFeatureCollection.size() == 6
            mongoDBSubCollectionFeatureCollection.featuresList.get(0).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0_0"
            mongoDBSubCollectionFeatureCollection.featuresList.get(1).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0_1"
            mongoDBSubCollectionFeatureCollection.featuresList.get(2).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0_2"
            mongoDBSubCollectionFeatureCollection.featuresList.get(3).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_1_0"
            mongoDBSubCollectionFeatureCollection.featuresList.get(4).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_1_1"
            mongoDBSubCollectionFeatureCollection.featuresList.get(5).getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_1_2"
    }

    void testLimitPolePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("pole", "designs", CQL.toFilter("designType='Measured Design'"), ["id", "designType"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 16
            feature.getAttribute("designType") == "Measured Design"
            feature.getAttribute("locationLabel") == null
            feature.getAttribute("locationId") == null
            feature.getAttribute("clientFile") == null
            feature.getAttribute("clientFileVersion") == null
            feature.getAttribute("dateModified") == null
            feature.getAttribute("glc") == null
            feature.getAttribute("glcUnit") == null
            feature.getAttribute("agl") == null
            feature.getAttribute("aglUnit") == null
            feature.getAttribute("species") == null
            feature.getAttribute("class") == null
            feature.getAttribute("length") == null
            feature.getAttribute("lengthUnit") == null
            feature.getAttribute("owner") == null
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitAnalysisPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("analysis", "designs", CQL.toFilter("actual=1.5677448671814123"), ["id", "actual"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 25
            feature.getAttribute("assetType") == null
            feature.getAttribute("designType") == null
            feature.getAttribute("loadInfo") ==null
            feature.getAttribute("locationLabel") == null
            feature.getAttribute("locationId") == null
            feature.getAttribute("clientFile") == null
            feature.getAttribute("clientFileVersion") == null
            feature.getAttribute("dateModified") == null
            feature.getAttribute("glc") == null
            feature.getAttribute("glcUnit") == null
            feature.getAttribute("agl") == null
            feature.getAttribute("aglUnit") == null
            feature.getAttribute("species") == null
            feature.getAttribute("class") == null
            feature.getAttribute("length") == null
            feature.getAttribute("lengthUnit") == null
            feature.getAttribute("owner") == null
            feature.getAttribute("actual") == 1.5677448671814123
            feature.getAttribute("allowable") == null
            feature.getAttribute("unit") == null
            feature.getAttribute("analysisDate") == null
            feature.getAttribute("component") ==  null
            feature.getAttribute("analysisType") ==  null
            feature.getAttribute("passes") == null
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0_0"
    }

    void testGetForm() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("form", "locations", CQL.toFilter("title='HTA Form'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("title") == "HTA Form"
            feature.getAttribute("template") == "6ee5fba14760878be22701e1b3b7c05b-HTA Form"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("id") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form"
    }

    void testGetFormLimitPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("form", "locations", CQL.toFilter("title='HTA Form'"), ["title"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("title") == "HTA Form"
            feature.getAttribute("template") == null
            feature.getAttribute("locationId") == null
            feature.getAttribute("id") == null
    }

    void testGetFormField() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("formField", "locations", CQL.toFilter("name='HTA Pole'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("name") == "HTA Pole"
            feature.getAttribute("value") == "TesterValue123"
            feature.getAttribute("groupName") == null
            feature.getAttribute("formId") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form"
    }

    void testGetFormFieldGroupName() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("formField", "locations", CQL.toFilter("groupName='Group Name'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("name") == "Rework POA"
            feature.getAttribute("value") == "--"
            feature.getAttribute("groupName") == "Group Name"
            feature.getAttribute("formId") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-SAP"
    }

    void testGetFormFieldLimitPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("formField", "locations", CQL.toFilter("name='HTA Pole'"), ["name"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("name") == "HTA Pole"
            feature.getAttribute("value") == null
            feature.getAttribute("groupName") == null
            feature.getAttribute("formId") == null
    }

    void testRemedyFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("remedy", "locations",  CQL.toFilter("value='Duplicate pole from other Windstrean/KDL proposal, do not put on cover map'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 2
            feature.getAttribute("value") == "Duplicate pole from other Windstrean/KDL proposal, do not put on cover map"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
    }

    void testLimitRemedyPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("remedy", "locations", CQL.toFilter("value='Duplicate pole from other Windstrean/KDL proposal, do not put on cover map'"), ["value"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 2
            feature.getAttribute("value") == "Duplicate pole from other Windstrean/KDL proposal, do not put on cover map"
            feature.getAttribute("locationId") == null
    }

    void testGetSummaryNoteFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("summaryNote", "locations",  CQL.toFilter("value='Windstream/KDL install down guy for span to the W'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 2
            feature.getAttribute("value") == "Windstream/KDL install down guy for span to the W"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
    }

    void testLimitSummaryNotePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("summaryNote", "locations", CQL.toFilter("value='Windstream/KDL install down guy for span to the W'"), ["value"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 2
            feature.getAttribute("value") == "Windstream/KDL install down guy for span to the W"
            feature.getAttribute("locationId") == null
    }

    void testGetWireFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("wire", "designs", CQL.toFilter("owner='Acme Power'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 12
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == "1/0 AAAC"
            feature.getAttribute("coreStrands") == 4
            feature.getAttribute("conductorStrands") == 7
            feature.getAttribute("attachmentHeight") == 33.25
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("usageGroup") == "NEUTRAL"
            feature.getAttribute("tensionGroup") == "Full"
            feature.getAttribute("midspanHeight") == 27.5
            feature.getAttribute("midspanHeightUnit") == "FOOT"
            feature.getAttribute("tensionAdjustment") == 0.9
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitWirePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("wire", "designs", CQL.toFilter("owner='Acme Power'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 12
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == null
            feature.getAttribute("coreStrands") == null
            feature.getAttribute("conductorStrands") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("usageGroup") == null
            feature.getAttribute("tensionGroup") == null
            feature.getAttribute("midspanHeight") == null
            feature.getAttribute("midspanHeightUnit") == null
            feature.getAttribute("tensionAdjustment") == null
            feature.getAttribute("poleId") == null
    }

    void testGetSpanPointFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("spanPoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("distance") == 88
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("environment") == "STREET"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitSpanPointPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("spanPoint", "designs",CQL.toFilter("distance=88"), ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 4
            feature.getAttribute("distance") == 88
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("environment") == null
            feature.getAttribute("poleId") == null
    }

    void testGetSpanGuyFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("spanGuy", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 11
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == "3/8\" EHS"
            feature.getAttribute("coreStrands") == 1
            feature.getAttribute("conductorStrands") == 7
            feature.getAttribute("attachmentHeight") == 32.666666666666664
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("midspanHeight") == 28.916666666666668
            feature.getAttribute("midspanHeightUnit") == "FOOT"
            feature.getAttribute("height") == 27.25
            feature.getAttribute("heightUnit") == "FOOT"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitSpanGuyPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("spanGuy", "designs", CQL.toFilter("owner='Acme Power'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 11
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == null
            feature.getAttribute("coreStrands") == null
            feature.getAttribute("conductorStrands") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("midspanHeight") == null
            feature.getAttribute("midspanHeightUnit") == null
            feature.getAttribute("height") == null
            feature.getAttribute("heightUnit") == null
            feature.getAttribute("poleId") == null
    }

    void testGetGuyFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("guy", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 7
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == "3/8\" EHS"
            feature.getAttribute("coreStrands") == 1
            feature.getAttribute("conductorStrands") == 7
            feature.getAttribute("attachmentHeight") == 28.25
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitGuyPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("guy", "designs", CQL.toFilter("owner='Acme Power'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 7
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == null
            feature.getAttribute("coreStrands") == null
            feature.getAttribute("conductorStrands") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("poleId") == null
    }

    void testGetInsulatorFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("insulator", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 9
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("type") == "15kV Dead End Insulator"
            feature.getAttribute("attachmentHeight") == 27.083333333333332
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("offset") == 325
            feature.getAttribute("offsetUnit") == "INCH"
            feature.getAttribute("direction") == 95
            feature.getAttribute("doubleInsulator") == false
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitInsulatorPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("insulator", "designs", CQL.toFilter("owner='Acme Power'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 9
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("type") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("offset") == null
            feature.getAttribute("offsetUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("doubleInsulator") == null
            feature.getAttribute("poleId") == null
    }

    void testGetEquipmentFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("equipment", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 9
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("size") == "1 Cutout"
            feature.getAttribute("type") == "CUTOUT_ARRESTOR"
            feature.getAttribute("attachmentHeight") == 24.75
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("bottomHeight") == 24.75
            feature.getAttribute("bottomHeightUnit") == "FOOT"
            feature.getAttribute("direction") == 108
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitEquipmentPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("equipment", "designs", CQL.toFilter("size='1 Cutout'"), ["size"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 9
            feature.getAttribute("owner") == null
            feature.getAttribute("size") == "1 Cutout"
            feature.getAttribute("type") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("bottomHeight") == null
            feature.getAttribute("bottomHeightUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("poleId") == null
    }

    void testGetDamageFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("damage", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 25
            feature.getAttribute("attachHeight") == 3
            feature.getAttribute("attachHeightUnit") == "FOOT"
            feature.getAttribute("damageHeight") == 2.3
            feature.getAttribute("damageHeightUnit") == "FOOT"
            feature.getAttribute("type") == "SLICE"
            feature.getAttribute("direction") == 299
            feature.getAttribute("width") == 7
            feature.getAttribute("widthUnit") == "INCH"
            feature.getAttribute("arc") == null
            feature.getAttribute("arcUnit") == null
            feature.getAttribute("depth") == null
            feature.getAttribute("depthUnit") == null
            feature.getAttribute("circumference") == null
            feature.getAttribute("circumferenceUnit") == null
            feature.getAttribute("shellThickness") == null
            feature.getAttribute("shellThicknessUnit") == null
            feature.getAttribute("neckDepth") == null
            feature.getAttribute("neckDepthUnit") == null
            feature.getAttribute("neckDiameter") == null
            feature.getAttribute("neckDiameterUnit") == null
            feature.getAttribute("neckOffset") == null
            feature.getAttribute("neckOffsetUnit") == null
            feature.getAttribute("nestDepth") == null
            feature.getAttribute("nestDepthUnit") == null
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitDamagePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("damage", "designs", CQL.toFilter("attachHeight=3"), ["attachHeight"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 25
            feature.getAttribute("attachHeight") == 3
            feature.getAttribute("attachHeightUnit") == null
            feature.getAttribute("damageHeight") == null
            feature.getAttribute("damageHeightUnit") == null
            feature.getAttribute("type") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("width") == null
            feature.getAttribute("widthUnit") == null
            feature.getAttribute("arc") == null
            feature.getAttribute("arcUnit") == null
            feature.getAttribute("depth") == null
            feature.getAttribute("depthUnit") == null
            feature.getAttribute("circumference") == null
            feature.getAttribute("circumferenceUnit") == null
            feature.getAttribute("shellThickness") == null
            feature.getAttribute("shellThicknessUnit") == null
            feature.getAttribute("neckDepth") == null
            feature.getAttribute("neckDepthUnit") == null
            feature.getAttribute("neckDiameter") == null
            feature.getAttribute("neckDiameterUnit") == null
            feature.getAttribute("neckOffset") == null
            feature.getAttribute("neckOffsetUnit") == null
            feature.getAttribute("nestDepth") == null
            feature.getAttribute("nestDepthUnit") == null
            feature.getAttribute("poleId") == null
    }

    void testCrossArmGetFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("crossArm", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("type") == "8 Foot Cross Arm"
            feature.getAttribute("attachmentHeight") == 32.666666666666664
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("offset") == 48
            feature.getAttribute("offsetUnit") == "INCH"
            feature.getAttribute("direction") == 57
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitCrossArmPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("crossArm", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("type") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("offset") == null
            feature.getAttribute("offsetUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("poleId") == null
    }

    void testGetAnchorFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("anchor", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("distance") == 12
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("direction") == 122
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("height") == 0
            feature.getAttribute("heightUnit") == "FOOT"
            feature.getAttribute("type") == "Single"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitAnchorPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("anchor", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("distance") == null
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("directionUnit") == null
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("height") == null
            feature.getAttribute("type") == null
            feature.getAttribute("poleId") == null
    }

    void testGetWireEndPointFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("wireEndPoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("distance") == 62
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("direction") == 302
            feature.getAttribute("inclination") == 0
            feature.getAttribute("inclinationUnit") == "DEGREE_ANGLE"
            feature.getAttribute("type") == "PREVIOUS_POLE"
            feature.getAttribute("comments") == "someComments"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitWireEndPointPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("wireEndPoint", "designs", Filter.INCLUDE, ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 8
            feature.getAttribute("distance") == 62
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("inclination") == null
            feature.getAttribute("inclinationUnit") == null
            feature.getAttribute("type") == null
            feature.getAttribute("comments") == null
            feature.getAttribute("poleId") == null
    }

    void testGetNotePointFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("notePoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 7
            feature.getAttribute("distance") == 62
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("direction") == 302
            feature.getAttribute("note") == "Residential Driveway"
            feature.getAttribute("height") == 23
            feature.getAttribute("heightUnit") == "FOOT"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitNotePointPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("notePoint", "designs", Filter.INCLUDE, ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 7
            feature.getAttribute("distance") == 62
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("note") == null
            feature.getAttribute("height") == null
            feature.getAttribute("heightUnit") == null
            feature.getAttribute("poleId") == null
    }

    void testGetPointLoadFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("pointLoad", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.size() == 1
            feature.attributeCount == 26
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("elevation") == 0
            feature.getAttribute("elevationUnit") == "DEGREE_ANGLE"
            feature.getAttribute("attachmentHeight") == 32
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("rotation") == 0
            feature.getAttribute("rotationUnit") == "DEGREE_ANGLE"
            feature.getAttribute("x") == 0
            feature.getAttribute("xUnit") == "FOOT"
            feature.getAttribute("y") == 0
            feature.getAttribute("yUnit") == "FOOT"
            feature.getAttribute("z") == 32.666666666666664
            feature.getAttribute("zUnit") == "FOOT"
            feature.getAttribute("fx") == 73.92457179425563
            feature.getAttribute("fxUnit") == "POUND_FORCE"
            feature.getAttribute("fy") == 241.92570549706124
            feature.getAttribute("fyUnit") == "POUND_FORCE"
            feature.getAttribute("fz") == -7.177126069505941
            feature.getAttribute("fzUnit") == "POUND_FORCE"
            feature.getAttribute("mx") == 0
            feature.getAttribute("mxUnit") == "POUND_FORCE_FOOT"
            feature.getAttribute("my") == 0
            feature.getAttribute("myUnit") == "POUND_FORCE_FOOT"
            feature.getAttribute("mz") == 0
            feature.getAttribute("mzUnit") == "POUND_FORCE_FOOT"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitPointLoadPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollection = getFeatureIterator("pointLoad", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollection.featuresList.get(0)
        then:
            mongoDBSubCollectionFeatureCollection.featuresList.size() == 1
            feature.attributeCount == 26
            feature.getAttribute("owner") == "Acme Power"
            feature.getAttribute("elevation") == null
            feature.getAttribute("elevationUnit") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("rotation") == null
            feature.getAttribute("rotationUnit") == null
            feature.getAttribute("x") == null
            feature.getAttribute("xUnit") == null
            feature.getAttribute("y") == null
            feature.getAttribute("yUnit") == null
            feature.getAttribute("z") == null
            feature.getAttribute("zUnit") == null
            feature.getAttribute("fx") == null
            feature.getAttribute("fxUnit") == null
            feature.getAttribute("fy") == null
            feature.getAttribute("fyUnit") == null
            feature.getAttribute("fz") == null
            feature.getAttribute("fzUnit") == null
            feature.getAttribute("mx") == null
            feature.getAttribute("mxUnit") == null
            feature.getAttribute("my") == null
            feature.getAttribute("myUnit") == null
            feature.getAttribute("mz") == null
            feature.getAttribute("mzUnit") == null
            feature.getAttribute("poleId") == null
    }

    private MongoDBSubCollectionFeatureCollection getFeatureIterator(String typeName, String collectionName, Filter filter, String[] propertyNames = null) {
        DBCursor dbCursor = database.getCollection(collectionName).find(new BasicDBObject("id", collectionName == "designs" ? designJSON.get("id") : locationJSON.get("id")))
        FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, typeName))
        Query query = new Query(typeName, filter, propertyNames)
        BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
        mongoDBFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, featureType, mapping)
        return new MongoDBSubCollectionFeatureCollection(dbCursor, featureType, mapping, query, mongoDBFeatureSource)
    }
}
