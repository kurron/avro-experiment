#!/bin/bash

AVRO_JAR=${1:-avro-tools-1.8.2.jar}

for schema in user-1.0.0.json user-1.1.0.json; do
  CMD="java -jar ${AVRO_JAR} compile schema src/test/resources/schemas/${schema} src/test/java"
  echo ${CMD}
  ${CMD}
done