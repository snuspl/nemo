/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.compiler.optimizer.examples;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.frontend.beam.transform.DoTransform;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static edu.snu.vortex.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * A sample MapReduce application.
 */
public final class MapReduce {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduce.class.getName());

  /**
   * Private constructor.
   */
  private MapReduce() {
  }

  /**
   * Main function of the example MR program.
   * @param args arguments.
   * @throws Exception Exceptions on the way.
   */
  public static void main(final String[] args) throws Exception {
    final IRVertex source =  new BoundedSourceVertex<>(new EmptyBoundedSource("Source"));
    final IRVertex map = new OperatorVertex(new EmptyTransform("MapVertex"));
    final IRVertex reduce = new OperatorVertex(new DoTransform(null, null));

    // Before
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);

    final IREdge edge1 = new IREdge(IREdge.Type.OneToOne, source, map, Coder.DUMMY_CODER);
    builder.connectVertices(edge1);

    final IREdge edge2 = new IREdge(IREdge.Type.ScatterGather, map, reduce, Coder.DUMMY_CODER);
    builder.connectVertices(edge2);

    final DAG dag = builder.build();
    LOG.info("Before Optimization");
    LOG.info(dag.toString());

    // Optimize
    final Optimizer optimizer = new Optimizer();
    final DAG optimizedDAG = optimizer.optimize(dag, Optimizer.PolicyType.Disaggregation, EMPTY_DAG_DIRECTORY);

    // After
    LOG.info("After Optimization");
    LOG.info(optimizedDAG.toString());
  }

  /**
   * An empty transform.
   */
  private static class EmptyTransform implements Transform {
    private final String name;

    /**
     * Default constructor.
     * @param name name of the empty transform.
     */
    EmptyTransform(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final Context context, final OutputCollector outputCollector) {
    }

    @Override
    public void onData(final Iterable<Element> data, final String srcVertexId) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * An empty bounded source.
   */
  public static final class EmptyBoundedSource extends BoundedSource {
    private final String name;

    public EmptyBoundedSource(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    public boolean producesSortedKeys(final PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    public BoundedReader createReader(final PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    @Override
    public List<? extends BoundedSource> split(final long l, final PipelineOptions pipelineOptions) throws Exception {
      return Arrays.asList(this);
    }

    public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
      return 1;
    }

    public List<? extends BoundedSource> splitIntoBundles(
        final long desiredBundleSizeBytes, final PipelineOptions options) throws Exception {
      return new ArrayList<>();
    }

    public void validate() {
    }

    public org.apache.beam.sdk.coders.Coder getDefaultOutputCoder() {
      throw new UnsupportedOperationException("Empty bounded source");
    }
  }
}
