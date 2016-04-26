#!/bin/sh 

set -x

mvn exec:java -Dexec.mainClass="com.arm.iot.event.hub.responder.IoTEventHubResponder"
