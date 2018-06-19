# mongodb-geoserver
A [MongoDB](https://www.mongodb.com/) plugin for [Geoserver](http://geoserver.org/).

### To setup:
1. Download deployment from [here](https://dev.spidasoftware.com/artifactory/exposed-repo/com/spidasoftware/mongodb-geoserver/)
2. Unzip the deployment
3. Move the groovy jar, the mongo java driver jar and the mongodb-geoserver jar into  geoserver/WEB-INF/lib
4. Restart geoserver.

### To add a store
1. Create a mapping file that includes a mapping for each mongo collection that will be published.
2. Add the store in Geoserver, adding the MongoDB connection parameters and the path to the mapping file.

### Mapping file
The mapping file will map a feature collection to a mongo collection.  If there is multiple objects nested in a collection each nested object can be published as a feature.  Here is an example [mapping.json](src/test/resources/mapping.json) file.  There is also a [location.json](src/test/resources/location.json) and [design.json](src/test/resources/design.json) that the mapping file and tests use.

Top level fields in the mapping.json file:
* typeName: (Required) The typeName for the feature that geoserver will use
* collection: (Required) The MongoDB collection that the feature is located in.
* idAsAttribute: (Required) Whether or not to include the id of the object as an attribute for the features.
* geometry: (Not Required) The geometry for the collection, must include the name, crs and path.
* displayGeometry: (Not Required) Whether or not to add the geometry to the feature.
* attributes: (Not Required) Must add the name for the attribute and the path to the value,  can also add a class([Double](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L692), [Long](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L32) or [Boolean](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L311))
* subCollections: (Not Required)
     + subCollectionPath: (Required) path to the subcollection.
     + includeInDefaultQuery: (Required) If true the query for objects will include a criteria that the length of the array is greater than 0.
     + attributes: (Not Required) Similar to attributes for top level objects but more options instead of just the path value.
         - path: The path from the top level object, the main collection object.
         - subCollectionPath: The path from the subCollection for the value.
         - concatenate: Will concatenate these values together with an underscore [example](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L128)
         - useKey: Use the key of the object for the value of the attribute [example](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L128(.
         - useValue: Use the value of the object for the value of the attribute [example](https://github.com/spidasoftware/mongodb-geoserver/blob/mongodb-geoserver-plugin/src/test/resources/mapping.json#L171).

### License Report

The license report for this project will be generated in `build/reports/dependency-license` by running:
```
gradlew generateLicenseReport
```