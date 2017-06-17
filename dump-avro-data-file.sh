#!/bin/bash

DATA_FILE=${1:-build/written.bin}
AVRO_JAR=${2:-avro-tools-1.8.2.jar}

TOSCHEMA="java -jar ${AVRO_JAR} getschema ${DATA_FILE}"
echo ${TOSCHEMA}
${TOSCHEMA}

TOJSON="java -jar ${AVRO_JAR} tojson ${DATA_FILE}"
echo ${TOJSON}
${TOJSON}
