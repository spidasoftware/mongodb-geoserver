## Overview
  This is a plugin for [Geoserver](http://geoserver.org/) that we use as a [Web Feature Service](https://en.wikipedia.org/wiki/Web_Feature_Service) for SPIDADB data.  
  
## Known Issues/limitations
  - This plugin only works when using WFS version 2.0.0 because it serves up [Complex Features](http://docs.geoserver.org/latest/en/user/data/app-schema/complex-features.html)
  - Specifying propertyNames for return values only works if you add the namespace twice, [see example in our WfsAssetService](https://github.com/spidasoftware/min/blob/b8cbfab2a9f48c6d3a63f9029c48f225c9aeaeb0/projectmanager/src/groovy/com/spidasoftware/projectmanager/gis/wfs/WfsAssetService.groovy#L259)
  
## To Run Locally
  1. [Install Geoserver](http://docs.geoserver.org/stable/en/user/installation/osx_installer.html)
  2. Copy the required jars into the Geoserver lib:
  
  ```
  cd min
  cp scripts/docker/tomcat/geoserver/*.jar /Applications/GeoServer.app/Contents/Java/webapps/geoserver/WEB-INF/lib/.
  ```

  3. Copy in the DB workspace:
  
  ```
  cd min
  cp -R scripts/docker/tomcat/geoserver/workspaces/ /Applications/GeoServer.app/Contents/Java/data_dir/workspaces
  ```
  4. Build and copy the db-geoserver plugin to  the Geoserver lib:
  
  ```
  cd min/db-geoserver
  gradlew clean build
  cp build/libs/db-geoserver*.jar /Applications/GeoServer.app/Contents/Java/webapps/geoserver/WEB-INF/lib/.
  ```

## To Setup QGIS
  1. [Install QGIS](https://www.qgis.org/en/site/forusers/download.html)
  2. Start QGIS
  3. Install QGIS WFS 2.0 Client (Plugins -> Manage and Install Plugins -> Search for 'WFS 2.0 Client')
  4. Add the local WFS service to QGIS using the QGIS WFS 2.0 client plugin (Web -> WFS 2.0 Client -> WFS 2.0 Client)
