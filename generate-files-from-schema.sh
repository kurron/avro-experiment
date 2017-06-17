#!/bin/bash

AVRO_JAR=${1:-avro-tools-1.8.2.jar}

CMD="java -jar ${AVRO_JAR} compile schema src/test/resources/schemas/*.json src/test/java"
echo ${CMD}
${CMD}
