package com.spidasoftware.mongodb.feature

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCursor
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.util.JSON
import com.spidasoftware.mongodb.data.SpidaDbDataAccess
import org.geotools.data.Query
import org.geotools.feature.NameImpl
import org.geotools.util.logging.Logging
import org.opengis.feature.Feature
import org.opengis.feature.type.FeatureType

import org.opengis.filter.Filter
import spock.lang.Shared
import spock.lang.Specification
import org.geotools.filter.text.cql2.CQL
import java.util.logging.Logger

class MongoDBSubCollectionFeatureCollectionIteratorSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBSubCollectionFeatureCollectionIteratorSpec.class.getPackage().getName())

    @Shared DB database
    @Shared BasicDBObject designJSON
    @Shared BasicDBObject locationJSON
    @Shared BasicDBList jsonMapping
    @Shared SpidaDbDataAccess spidaDbDataAccess
    @Shared String namespace = "http://spida/db"

    void setupSpec() {
        designJSON = JSON.parse(getClass().getResourceAsStream('/design.json').text)
        locationJSON = JSON.parse(getClass().getResourceAsStream('/location.json').text)

        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        spidaDbDataAccess = new SpidaDbDataAccess(namespace, System.getProperty("mongoHost"), System.getProperty("mongoPort"), System.getProperty("mongoDatabase"), null, null, jsonMapping)

        String host = System.getProperty("mongoHost")
        String port = System.getProperty("mongoPort")
        String databaseName = System.getProperty("mongoDatabase")
        def serverAddress = new ServerAddress(host, Integer.valueOf(port))
        MongoClient mongoClient = new MongoClient(serverAddress)
        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        spidaDbDataAccess = new SpidaDbDataAccess(namespace, host, port, databaseName, null, null, jsonMapping)
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

    void testGetPole() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("pole", "designs", CQL.toFilter("designType='Measured Design'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 16
            feature.getAttribute("designType") == "Measured Design"
            feature.getAttribute("locationLabel") == "684704E"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("clientFile") == "SCE.client"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("analysis", "designs", CQL.toFilter("actual=1.5677448671814123"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 24
            feature.getAttribute("designType") == "Measured Design"
            feature.getAttribute("loadInfo") == "CSA Heavy"
            feature.getAttribute("locationLabel") == "684704E"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("clientFile") == "SCE.client"
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
            feature.getAttribute("component") ==  "Pole-Buckling"
            feature.getAttribute("passes") == true
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0"
    }

    void testGetAllPoles() {
        when:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("pole", "designs", Filter.INCLUDE)
        then:
            mongoDBSubCollectionFeatureCollectionIterator.size() == 1
    }

    void testGetAllAnalysis() {
        when:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("analysis", "designs", Filter.INCLUDE)
        then:
            mongoDBSubCollectionFeatureCollectionIterator.size() == 3
    }

    void testLimitPolePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("pole", "designs", CQL.toFilter("designType='Measured Design'"), ["id", "designType"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("analysis", "designs", CQL.toFilter("actual=1.5677448671814123"), ["id", "actual"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 24
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
            feature.getAttribute("passes") == null
            feature.getAttribute("id") == "56e9b7137d84511d8dd0f13c_ANALYSIS_0"
    }

    void testGetForm() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("form", "locations", CQL.toFilter("title='HTA Form'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("title") == "HTA Form"
            feature.getAttribute("template") == "6ee5fba14760878be22701e1b3b7c05b-HTA Form"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
            feature.getAttribute("id") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form"
    }

    void testGetFormLimitPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("form", "locations", CQL.toFilter("title='HTA Form'"), ["title"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("title") == "HTA Form"
            feature.getAttribute("template") == null
            feature.getAttribute("locationId") == null
            feature.getAttribute("id") == null
    }

    void testGetFormField() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("formField", "locations", CQL.toFilter("name='HTA Pole'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("name") == "HTA Pole"
            feature.getAttribute("value") == "TesterValue123"
            feature.getAttribute("groupName") == null
            feature.getAttribute("formId") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-HTA Form"
    }

    void testGetFormFieldGroupName() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("formField", "locations", CQL.toFilter("groupName='Group Name'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("name") == "Rework POA"
            feature.getAttribute("value") == "--"
            feature.getAttribute("groupName") == "Group Name"
            feature.getAttribute("formId") == "55fac7fde4b0e7f2e3be342c_6ee5fba14760878be22701e1b3b7c05b-SAP"
    }

    void testGetFormFieldLimitPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("formField", "locations", CQL.toFilter("name='HTA Pole'"), ["name"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("name") == "HTA Pole"
            feature.getAttribute("value") == null
            feature.getAttribute("groupName") == null
            feature.getAttribute("formId") == null
    }

    void testRemedyFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("remedy", "locations",  CQL.toFilter("value='Duplicate pole from other Windstrean/KDL proposal, do not put on cover map'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 2
            feature.getAttribute("value") == "Duplicate pole from other Windstrean/KDL proposal, do not put on cover map"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
    }

    void testLimitRemedyPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("remedy", "locations", CQL.toFilter("value='Duplicate pole from other Windstrean/KDL proposal, do not put on cover map'"), ["value"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 2
            feature.getAttribute("value") == "Duplicate pole from other Windstrean/KDL proposal, do not put on cover map"
            feature.getAttribute("locationId") == null
    }

    void testGetSummaryNoteFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("summaryNote", "locations",  CQL.toFilter("value='Windstream/KDL install down guy for span to the W'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 2
            feature.getAttribute("value") == "Windstream/KDL install down guy for span to the W"
            feature.getAttribute("locationId") == "55fac7fde4b0e7f2e3be342c"
    }

    void testLimitSummaryNotePropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("summaryNote", "locations", CQL.toFilter("value='Windstream/KDL install down guy for span to the W'"), ["value"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 2
            feature.getAttribute("value") == "Windstream/KDL install down guy for span to the W"
            feature.getAttribute("locationId") == null
    }

    void testGetWireFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("wire", "designs", CQL.toFilter("owner='AEP'"))
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 12
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("wire", "designs", CQL.toFilter("owner='AEP'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 12
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("spanPoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("distance") == 88
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("environment") == "STREET"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitSpanPointPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("spanPoint", "designs",CQL.toFilter("distance=88"), ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 4
            feature.getAttribute("distance") == 88
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("environment") == null
            feature.getAttribute("poleId") == null
    }

    void testGetSpanGuyFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("spanGuy", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 11
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("spanGuy", "designs", CQL.toFilter("owner='AEP'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 11
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("guy", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 7
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("size") == "3/8\" EHS"
            feature.getAttribute("coreStrands") == 1
            feature.getAttribute("conductorStrands") == 7
            feature.getAttribute("attachmentHeight") == 28.25
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitGuyPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("guy", "designs", CQL.toFilter("owner='AEP'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 7
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("size") == null
            feature.getAttribute("coreStrands") == null
            feature.getAttribute("conductorStrands") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("poleId") == null
    }

    void testGetInsulatorFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("insulator", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("insulator", "designs", CQL.toFilter("owner='AEP'"), ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("equipment", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("owner") == "AEP"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("equipment", "designs", CQL.toFilter("size='1 Cutout'"), ["size"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("damage", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("damage", "designs", CQL.toFilter("attachHeight=3"), ["attachHeight"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("crossArm", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("type") == "8 Foot Cross Arm"
            feature.getAttribute("attachmentHeight") == 32.666666666666664
            feature.getAttribute("attachmentHeightUnit") == "FOOT"
            feature.getAttribute("offset") == 48
            feature.getAttribute("offsetUnit") == "INCH"
            feature.getAttribute("direction") == 57
            feature.getAttribute("associatedBacking") == "Other"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitCrossArmPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("crossArm", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("type") == null
            feature.getAttribute("attachmentHeight") == null
            feature.getAttribute("attachmentHeightUnit") == null
            feature.getAttribute("offset") == null
            feature.getAttribute("offsetUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("associatedBacking") == null
            feature.getAttribute("poleId") == null
    }

    void testGetAnchorFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("anchor", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("distance") == 12
            feature.getAttribute("distanceUnit") == "FOOT"
            feature.getAttribute("direction") == 122
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("height") == 0
            feature.getAttribute("heightUnit") == "FOOT"
            feature.getAttribute("supportType") == "Other"
            feature.getAttribute("type") == "Single"
            feature.getAttribute("poleId") == "56e9b7137d84511d8dd0f13c"
    }

    void testLimitAnchorPropertyNames() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("anchor", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 9
            feature.getAttribute("distance") == null
            feature.getAttribute("distanceUnit") == null
            feature.getAttribute("direction") == null
            feature.getAttribute("directionUnit") == null
            feature.getAttribute("owner") == "AEP"
            feature.getAttribute("height") == null
            feature.getAttribute("supportType") == null
            feature.getAttribute("type") == null
            feature.getAttribute("poleId") == null
    }

    void testGetWireEndPointFeature() {
        setup:
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("wireEndPoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("wireEndPoint", "designs", Filter.INCLUDE, ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("notePoint", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("notePoint", "designs", Filter.INCLUDE, ["distance"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("pointLoad", "designs", Filter.INCLUDE)
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 26
            feature.getAttribute("owner") == "SCE"
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
            def mongoDBSubCollectionFeatureCollectionIterator = getFeatureIterator("pointLoad", "designs", Filter.INCLUDE, ["owner"] as String[])
        when:
            Feature feature = mongoDBSubCollectionFeatureCollectionIterator.next()
        then:
            !mongoDBSubCollectionFeatureCollectionIterator.hasNext()
            feature.attributeCount == 26
            feature.getAttribute("owner") == "SCE"
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

    private MongoDBSubCollectionFeatureCollectionIterator getFeatureIterator(String typeName, String collectionName, Filter filter, String[] propertyNames = null) {
        DBCursor dbCursor = database.getCollection(collectionName).find(new BasicDBObject("id", collectionName == "designs" ? designJSON.get("id") : locationJSON.get("id")))
        FeatureType featureType = spidaDbDataAccess.getSchema(new NameImpl(namespace, typeName))
        Query query = new Query(typeName, filter, propertyNames)
        BasicDBObject mapping = jsonMapping.find { it.typeName == typeName }
        return new MongoDBSubCollectionFeatureCollectionIterator(dbCursor, featureType, mapping, query)
    }
}
