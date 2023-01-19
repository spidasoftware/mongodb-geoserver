package com.spidasoftware.mongodb.data

import spock.lang.Specification
import spock.lang.Unroll
import spock.util.mop.ConfineMetaClassChanges

class MongoDBDataAccessFactorySpec extends Specification {

    MongoDBDataAccessFactory mongoDBDataAccessFactory

    void setup() {
        mongoDBDataAccessFactory = new MongoDBDataAccessFactory()
    }

    void "test isAvailable"() {
        expect:
            mongoDBDataAccessFactory.isAvailable()
    }

    @Unroll("test canProcess testNo=#testNo")
    @ConfineMetaClassChanges([File])
    void "test canProcess"() {
        setup:
            File featureTypeMappingFile = GroovyMock(File)
            featureTypeMappingFile.getText() >> jsonMapping
            File.metaClass.constructor = { String path -> return featureTypeMappingFile }
        when:
            boolean canProcess = mongoDBDataAccessFactory.canProcess(["host"                  : host,
                                                                      "port"                  : port,
                                                                      "databaseName"          : databaseName,
                                                                      "namespace"             : namespace,
                                                                      "uri"                   : uri,
                                                                      "featureTypeMappingFile": "/Users/test/test/jsonMapping.text"])
        then:
            canProcess == expectedCanProcess
        where:
            testNo | jsonMapping                                                                                                  | host        | port    | databaseName  | namespace         | uri                   | expectedCanProcess
            1      | getClass().getResourceAsStream('/mapping.json').text                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | null                  | true
            2      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | null                  | true
            3      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | null        | null    | "test-calcdb" | "http://spida/db" | "mongodb://localhost" | true
            4      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | null        | "27017" | "test-calcdb" | "http://spida/db" | null                  | false
            5      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | null    | "test-calcdb" | "http://spida/db" | null                  | false
            6      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | null          | "http://spida/db" | null                  | false
            7      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | "test-calcdb" | null              | null                  | false
            8      | '[]'                                                                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | null                  | false
            9      | null                                                                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | null                  | false
    }
}