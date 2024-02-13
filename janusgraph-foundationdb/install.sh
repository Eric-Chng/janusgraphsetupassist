#!/bin/bash

if [ -z "$*" ]; then
    echo "Please provide the path to your JanusGraph install directory"
    exit 0
fi

JANUS_INSTALL_PATH=$1
JANUS_BIN_PATH=${JANUS_INSTALL_PATH}/bin
JANUS_CONF_PATH=${JANUS_INSTALL_PATH}/conf
JANUS_GREMLIN_SERVER_CONF=${JANUS_CONF_PATH}/gremlin-server
JANUS_LIB_PATH=${JANUS_INSTALL_PATH}/lib

if [ ! -d "$JANUS_INSTALL_PATH" ]; then
  echo "Directory ${JANUS_INSTALL_PATH} does not exist"
  exit 0
fi

cp target/janusgraph-foundationdb*.jar ${JANUS_LIB_PATH}
cp target/lib/fdb-java*.jar ${JANUS_LIB_PATH}
cp target/lib/simpleclient*.jar ${JANUS_LIB_PATH}
cp target/lib/nugraph-calltracing*.jar ${JANUS_LIB_PATH}

cp conf/gremlin-server-deploy-local.yaml ${JANUS_GREMLIN_SERVER_CONF}
cp conf/nugraph-fdb-deploy-local.properties ${JANUS_GREMLIN_SERVER_CONF}