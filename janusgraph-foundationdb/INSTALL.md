# Install janusgraph-foundationdb plugin to the janusgraph base code

### Download janusgraph code base

Download the corresponding `janusgraph-${VERSION}.zip` and unzip it.

### Install the plugin

```shell
./install.sh <janusgraph_base_path>
```

### Use Gremlin with janusgraph-foundationdb plugin

#### Start the server

```shell
cd <janusgraph_base_path>/bin
./gremlin-server.sh conf/gremlin-server/gremlin-server-deploy-local.yaml
```

#### Start the client

```shell
cd <janusgraph_base_path>/bin
./gremlin.sh

         \,,,/
         (o o)
-----oOOo-(3)-oOOo-----
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/Users/hieunguyen/Downloads/janusgraph-0.5.3/lib/slf4j-log4j12-1.7.12.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/Users/hieunguyen/Downloads/janusgraph-0.5.3/lib/logback-classic-1.1.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
plugin activated: tinkerpop.server
plugin activated: tinkerpop.tinkergraph
15:24:58 WARN  org.apache.hadoop.util.NativeCodeLoader  - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
plugin activated: tinkerpop.hadoop
plugin activated: tinkerpop.spark
plugin activated: tinkerpop.utilities
plugin activated: janusgraph.imports
gremlin> :remote connect tinkerpop.server conf/remote.yaml
==>Configured localhost/127.0.0.1:8182
gremlin> :remote console
==>All scripts will now be sent to Gremlin Server - [localhost/127.0.0.1:8182] - type ':remote console' to return to local mode
```

Now you can execute some graph traversals:

```shell
gremlin> g.V().count()
==>700
gremlin> g.V().limit(2)
==>v[4096]
==>v[8192]
```