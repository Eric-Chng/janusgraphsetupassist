package org.janusgraph.blueprints.process;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
//import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest;
//import org.apache.tinkerpop.gremlin.process.computer.bulkdumping.BulkDumperVertexProgramTest;
//import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgramTest;
//import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class FoundationDBProcessComputerSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed as part of this suite.
     */
    private static final Class<?>[] allTests = new Class<?>[]{

            // computer, vertex program, and map/reduce semantics
            GraphComputerTest.class,

            // branch
            BranchTest.Traversals.class,
            ChooseTest.Traversals.class,
            OptionalTest.Traversals.class,
            LocalTest.Traversals.class,
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CoinTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class,
            SampleTest.Traversals.class,
            SimplePathTest.Traversals.class,
            TailTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            CoalesceTest.Traversals.class,
            ConstantTest.Traversals.class,
            CountTest.Traversals.class,
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            GraphTest.Traversals.class,
            LoopsTest.Traversals.class,
            MapTest.Traversals.class,
            MatchTest.CountMatchTraversals.class,
            MatchTest.GreedyMatchTraversals.class,
            MathTest.Traversals.class,
            MaxTest.Traversals.class,
            MeanTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            OrderTest.Traversals.class,
            PageRankTest.Traversals.class,
            PathTest.Traversals.class,
//            PeerPressureTest.Traversals.class,
//            ProfileTest.Traversals.class,
            ProjectTest.Traversals.class,
//            ProgramTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,
            VertexTest.Traversals.class,

            // sideEffect
            AddEdgeTest.Traversals.class,
            AggregateTest.Traversals.class,
            ExplainTest.Traversals.class,
            GroupTest.Traversals.class,
            GroupCountTest.Traversals.class,
            InjectTest.Traversals.class,
//            ProfileTest.Traversals.class,
            SackTest.Traversals.class,
            SideEffectCapTest.Traversals.class,
            SideEffectTest.Traversals.class,
            StoreTest.Traversals.class,
            SubgraphTest.Traversals.class,
            TreeTest.Traversals.class,

            // compliance
            ComplexTest.Traversals.class,
            TraversalInterruptionComputerTest.class,

            // algorithms
            PageRankVertexProgramTest.class,
//            PeerPressureVertexProgramTest.class,
//            BulkLoaderVertexProgramTest.class,
//            BulkDumperVertexProgramTest.class,

            // creations
            TranslationStrategyProcessTest.class,

            // decorations
            ReadOnlyStrategyProcessTest.class,
            SubgraphStrategyProcessTest.class,

            // optimizations
            IncidentToAdjacentStrategyProcessTest.class
    };

    public FoundationDBProcessComputerSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        super(klass, builder, allTests, null, true, TraversalEngine.Type.COMPUTER);
    }

    public FoundationDBProcessComputerSuite(final Class<?> classToTest, final RunnerBuilder builder) throws InitializationError {
        this(classToTest, builder, allTests);
    }
}
