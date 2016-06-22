#!/bin/sh

set -x

mvn install:install-file -Dfile=jars/iothub-service-sdk-1.0.7.jar -DgroupId=com.microsoft.azure.iothub.service.sdk -DartifactId=iothub-service-sdk -Dversion=1.0.7 -Dpackaging=jar -DgeneratePom=true
