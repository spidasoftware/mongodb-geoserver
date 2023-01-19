package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.util.JSON
import org.geotools.feature.NameImpl
import org.geotools.referencing.CRS
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import org.opengis.feature.type.Name
import spock.lang.Specification

import java.util.logging.Logger

class MongoDBDataAccessSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBDataAccessSpec.class.getPackage().getName())

    MongoDBDataAccess mongoDBDataAccess
    String namespace = "http://spida/db"
    BasicDBList jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)

    void setup() {
        mongoDBDataAccess = new MongoDBDataAccess(namespace, System.getProperty("mongoHost"), System.getProperty("mongoPort"), System.getProperty("mongoDatabase"), null, null, null, jsonMapping)
    }

    void "test can get schema for every type"() {
        expect:
            mongoDBDataAccess.getNames().each { Name name ->
                assert mongoDBDataAccess.getSchema(name) != null // Can get the schema
            }
    }

    void "test location feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "location"))
        then:
            featureType.coordinateReferenceSystem == CRS.decode("urn:ogc:def:crs:EPSG:4326")
            featureType.getGeometryDescriptor().getName().getLocalPart() == "geographicCoordinate"
            featureType.getName().getLocalPart() == "location"
            featureType.getDescriptor("id").type.binding == String
            featureType.getDescriptor("name").type.binding == String
            featureType.getDescriptor("projectId").type.binding == String
            featureType.getDescriptor("projectName").type.binding == String
            featureType.getDescriptor("dateModified").type.binding == Long
            featureType.getDescriptor("clientFile").type.binding == String
            featureType.getDescriptor("clientFileVersion").type.binding == String
            featureType.getDescriptor("comments").type.binding == String
            featureType.getDescriptor("streetNumber").type.binding == String
            featureType.getDescriptor("street").type.binding == String
            featureType.getDescriptor("city").type.binding == String
            featureType.getDescriptor("state").type.binding == String
            featureType.getDescriptor("zipCode").type.binding == String
            featureType.getDescriptor("county").type.binding == String
            featureType.getDescriptor("technician").type.binding == String
            featureType.getDescriptor("latitude").type.binding == Double
            featureType.getDescriptor("longitude").type.binding == Double
    }

    void "test poleTag feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "poleTag"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "poleTag"
            featureType.getDescriptor("type").type.binding == String
            featureType.getDescriptor("value").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
    }

    void "test remedy feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "remedy"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "remedy"
            featureType.getDescriptor("value").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
    }

    void "test summaryNote feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "summaryNote"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "summaryNote"
            featureType.getDescriptor("value").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
    }

    void "test form feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "form"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "form"
            featureType.getDescriptor("title").type.binding == String
            featureType.getDescriptor("template").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
            featureType.getDescriptor("id").type.binding == String
    }

    void "test formField feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "formField"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "formField"
            featureType.getDescriptor("name").type.binding == String
            featureType.getDescriptor("value").type.binding == String
            featureType.getDescriptor("groupName").type.binding == String
            featureType.getDescriptor("formId").type.binding == String
    }

    void "test analysis feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "analysis"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "analysis"
            featureType.getDescriptor("designLayerName").type.binding == String
            featureType.getDescriptor("loadInfo").type.binding == String
            featureType.getDescriptor("locationName").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
            featureType.getDescriptor("clientFile").type.binding == String
            featureType.getDescriptor("clientFileVersion").type.binding == String
            featureType.getDescriptor("dateModified").type.binding == Long
            featureType.getDescriptor("actual").type.binding == Double
            featureType.getDescriptor("allowable").type.binding == Double
            featureType.getDescriptor("unit").type.binding == String
            featureType.getDescriptor("analysisDate").type.binding == Long
            featureType.getDescriptor("component").type.binding == String
            featureType.getDescriptor("analysisType").type.binding == String
            featureType.getDescriptor("passes").type.binding == Boolean
            featureType.getDescriptor("poleId").type.binding == String
            featureType.getDescriptor("id").type.binding == String
    }

    void "test pole feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "pole"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "pole"
            featureType.getDescriptor("designLayerName").type.binding == String
            featureType.getDescriptor("locationName").type.binding == String
            featureType.getDescriptor("locationId").type.binding == String
            featureType.getDescriptor("clientFile").type.binding == String
            featureType.getDescriptor("clientFileVersion").type.binding == String
            featureType.getDescriptor("dateModified").type.binding == Long
            featureType.getDescriptor("glc").type.binding == Double
            featureType.getDescriptor("glcUnit").type.binding == String
            featureType.getDescriptor("agl").type.binding == Double
            featureType.getDescriptor("aglUnit").type.binding == String
            featureType.getDescriptor("species").type.binding == String
            featureType.getDescriptor("class").type.binding == String
            featureType.getDescriptor("length").type.binding == Double
            featureType.getDescriptor("lengthUnit").type.binding == String
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("id").type.binding == String
    }

    void "test wire feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "wire"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "wire"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("coreStrands").type.binding == Long
            featureType.getDescriptor("conductorStrands").type.binding == Long
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("usageGroup").type.binding == String
            featureType.getDescriptor("tensionGroup").type.binding == String
            featureType.getDescriptor("midspanHeight").type.binding == Double
            featureType.getDescriptor("midspanHeightUnit").type.binding == String
            featureType.getDescriptor("tensionAdjustment").type.binding == Double
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test spanPoint feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "spanPoint"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "spanPoint"
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("environment").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test spanGuy feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "spanGuy"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "spanGuy"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("coreStrands").type.binding == Long
            featureType.getDescriptor("conductorStrands").type.binding == Long
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("midspanHeight").type.binding == Double
            featureType.getDescriptor("midspanHeightUnit").type.binding == String
            featureType.getDescriptor("height").type.binding == Double
            featureType.getDescriptor("heightUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }
    void "test guy feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "guy"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "guy"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("coreStrands").type.binding == Long
            featureType.getDescriptor("conductorStrands").type.binding == Long
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test insulator feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "equipment"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "equipment"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("type").type.binding == String
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("bottomHeight").type.binding == Double
            featureType.getDescriptor("bottomHeightUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test equipment feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "equipment"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "equipment"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("type").type.binding == String
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("bottomHeight").type.binding == Double
            featureType.getDescriptor("bottomHeightUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test damage feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "damage"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "damage"
            featureType.getDescriptor("attachHeight").type.binding == Double
            featureType.getDescriptor("attachHeightUnit").type.binding == String
            featureType.getDescriptor("damageHeight").type.binding == Double
            featureType.getDescriptor("damageHeightUnit").type.binding == String
            featureType.getDescriptor("type").type.binding == String
            featureType.getDescriptor("width").type.binding == Double
            featureType.getDescriptor("widthUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("arc").type.binding == Double
            featureType.getDescriptor("arcUnit").type.binding == String
            featureType.getDescriptor("depth").type.binding == Double
            featureType.getDescriptor("depthUnit").type.binding == String
            featureType.getDescriptor("circumference").type.binding == Double
            featureType.getDescriptor("circumferenceUnit").type.binding == String
            featureType.getDescriptor("shellThickness").type.binding == Double
            featureType.getDescriptor("shellThicknessUnit").type.binding == String
            featureType.getDescriptor("neckDepth").type.binding == Double
            featureType.getDescriptor("neckDepthUnit").type.binding == String
            featureType.getDescriptor("nestDepth").type.binding == Double
            featureType.getDescriptor("nestDepthUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test crossArm feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "crossArm"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "crossArm"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("offset").type.binding == Double
            featureType.getDescriptor("offsetUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test anchor feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "anchor"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "anchor"
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("height").type.binding == Double
            featureType.getDescriptor("heightUnit").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test wireEndPoint feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "wireEndPoint"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "wireEndPoint"
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("inclination").type.binding == Long
            featureType.getDescriptor("inclinationUnit").type.binding == String
            featureType.getDescriptor("type").type.binding == String
            featureType.getDescriptor("comments").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test notePoint feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "notePoint"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "notePoint"
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Long
            featureType.getDescriptor("note").type.binding == String
            featureType.getDescriptor("height").type.binding == Double
            featureType.getDescriptor("heightUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test pointLoad feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "pointLoad"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "pointLoad"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("attachHeight").type.binding == Double
            featureType.getDescriptor("attachHeightUnit").type.binding == String
            featureType.getDescriptor("XForce").type.binding == Double
            featureType.getDescriptor("XForceUnit").type.binding == String
            featureType.getDescriptor("YForce").type.binding == Double
            featureType.getDescriptor("YForceUnit").type.binding == String
            featureType.getDescriptor("ZForce").type.binding == Double
            featureType.getDescriptor("ZForceUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test wirePointLoad feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "wirePointLoad"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "wirePointLoad"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("wire").type.binding == String
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("XForce").type.binding == Double
            featureType.getDescriptor("XForceUnit").type.binding == String
            featureType.getDescriptor("YForce").type.binding == Double
            featureType.getDescriptor("YForceUnit").type.binding == String
            featureType.getDescriptor("ZForce").type.binding == Double
            featureType.getDescriptor("ZForceUnit").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test pushBrace feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "pushBrace"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "pushBrace"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("glc").type.binding == Double
            featureType.getDescriptor("glcUnit").type.binding == String
            featureType.getDescriptor("distance").type.binding == Double
            featureType.getDescriptor("distanceUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Double
            featureType.getDescriptor("species").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test sidewalkBrace feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "sidewalkBrace"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "sidewalkBrace"
            featureType.getDescriptor("owner").type.binding == String
            featureType.getDescriptor("attachmentHeight").type.binding == Double
            featureType.getDescriptor("attachmentHeightUnit").type.binding == String
            featureType.getDescriptor("size").type.binding == String
            featureType.getDescriptor("length").type.binding == Double
            featureType.getDescriptor("lengthUnit").type.binding == String
            featureType.getDescriptor("direction").type.binding == Double
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test foundation feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "foundation"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "foundation"
            featureType.getDescriptor("name").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }

    void "test assembly feature type"() {
        when:
            FeatureType featureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "assembly"))
        then:
            featureType.coordinateReferenceSystem == null
            featureType.getGeometryDescriptor() == null
            featureType.getName().getLocalPart() == "assembly"
            featureType.getDescriptor("code").type.binding == String
            featureType.getDescriptor("poleId").type.binding == String
    }
}
