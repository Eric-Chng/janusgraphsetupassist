package org.janusgraph.blueprints.jsr223;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineSuite;
import org.apache.tinkerpop.gremlin.jsr223.ScriptEngineToTest;
import org.janusgraph.blueprints.FoundationDBGraphProvider;
import org.janusgraph.core.JanusGraph;
import org.junit.runner.RunWith;

@RunWith(GremlinScriptEngineSuite.class)
@GraphProviderClass(provider = FoundationDBGraphProvider.class, graph = JanusGraph.class)
@ScriptEngineToTest(scriptEngineName = "gremlin-groovy")
public class FoundationDBGremlinScriptEngineTest {
}
