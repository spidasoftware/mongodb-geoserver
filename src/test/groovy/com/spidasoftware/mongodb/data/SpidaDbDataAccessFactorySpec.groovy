package com.spidasoftware.mongodb.data

import spock.lang.Specification
import spock.lang.Unroll
import spock.util.mop.ConfineMetaClassChanges

class SpidaDbDataAccessFactorySpec extends Specification {

    SpidaDbDataAccessFactory spidaDbDataAccessFactory

    void setup() {
        spidaDbDataAccessFactory = new SpidaDbDataAccessFactory()
    }

    void "test isAvailable"() {
        expect:
            spidaDbDataAccessFactory.isAvailable()
    }

    @Unroll("test canProcess testNo=#testNo")
    @ConfineMetaClassChanges([File])
    void "test canProcess"() {
        setup:
            File featureTypeMappingFile = GroovyMock(File)
            featureTypeMappingFile.getText() >> jsonMapping
            File.metaClass.constructor = { String path -> return featureTypeMappingFile }
        when:
            boolean canProcess = spidaDbDataAccessFactory.canProcess(["host"                  : host,
                                                                      "port"                  : port,
                                                                      "databaseName"          : databaseName,
                                                                      "namespace"             : namespace,
                                                                      "featureTypeMappingFile": "/Users/test/test/jsonMapping.text"])
        then:
            canProcess == expectedCanProcess
        where:
            testNo | jsonMapping                                                                                                  | host        | port    | databaseName  | namespace         | expectedCanProcess
            1      | getClass().getResourceAsStream('/mapping.json').text                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | true
            2      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | true
            3      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | null        | "27017" | "test-calcdb" | "http://spida/db" | false
            4      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | null    | "test-calcdb" | "http://spida/db" | false
            5      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | null          | "http://spida/db" | false
            6      | '[{"typeName": "T", "collection": "T", "idAsAttribute": false, "attributes": [{"name": "T", "path": "T"}]}]' | "localhost" | "27017" | "test-calcdb" | null              | false
            7      | '[]'                                                                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | false
            8      | null                                                                                                         | "localhost" | "27017" | "test-calcdb" | "http://spida/db" | false

    }
}