# Setup
1. Clone https://github.com/JanusGraph/janusgraph
    1. Run `mvn clean install -DskipTests`
2. Inside NuGraphCallTracing
    1. Run `mvn clean install -DskipTests`
3. Inside janusgraph-foundationdb
    1. Run `mvn clean install -DskipTests`
    2. Run `./install.sh ../janusgraph-1.0.0`

# Running Server
1. While inside janusgraph-1.0.0 folder, run `./bin/janusgraph-server.sh conf/gremlin-server/gremlin-server-deploy-local.yaml`

# Running Client
1. While inside janusgraph-1.0.0 folder, run `./bin/gremlin.sh`
2. After console opens, run `:remote connect tinkerpop.server conf/remote.yaml`
3. Then run `:remote console` 
4. You can now run gremlin queries. The default graph object is called g (ex: g.V().count())

# Setting up Remote Connection
1. Open up the port with `sudo iptables -A INPUT -p tcp --dport 8182 -j ACCEPT`
2. Edit `janusgraph-1.0.0/conf/gremlin-server/gremlin-server-deploy-local.yaml` host to your ip address
3. Edit `janusgraph-1.0.0/conf/remote.yaml` hosts to include your ip address

# Potential Error Vectors
- `janusgraph-1.0.0/conf/gremlin-server/nugraph-fdb-deploy-local-properties` has to point to correct path of fdb.cluster in `storage.fdb.cluster-file-path`
- If `./gremlin.sh` throws a `java.awt.AWTError: Assistive Technology` error, refer to https://github.com/microsoft/vscode-arduino/issues/644 to fix
