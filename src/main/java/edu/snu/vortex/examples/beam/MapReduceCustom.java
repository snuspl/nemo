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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.frontend.beam.Runner;
import edu.snu.vortex.compiler.frontend.beam.VortexPipelineOptions;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.optimizer.passes.DefaultStagePartitioningPass;
import edu.snu.vortex.compiler.optimizer.passes.ScheduleGroupPass;
import edu.snu.vortex.compiler.optimizer.passes.StaticOptimizationPass;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Sample MapReduce application, running a custom pass.
 */
public final class MapReduceCustom {
  /**
   * Private Constructor.
   */
  private MapReduceCustom() {
  }

  /**
   * Main function for the MR BEAM program running a custom pass.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    // same as the original application
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(VortexPipelineOptions.class);
    options.setRunner(Runner.class);
    options.setJobName("MapReduce");

    // register a new pass to the Optimizer before running the application.
    Optimizer.registerPass("custom", new CustomDefaultPass());

    // run the application.
    final Pipeline p = Pipeline.create(options);
    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.<String, KV<String, Long>>via(new SimpleFunction<String, KV<String, Long>>() {
          @Override
          public KV<String, Long> apply(final String line) {
            final String[] words = line.split(" +");
            final String documentId = words[0];
            final Long count = Long.parseLong(words[2]);
            return KV.of(documentId, count);
          }
        }))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(Sum.ofLongs()))
        .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(final KV<String, Long> kv) {
            return kv.getKey() + ": " + kv.getValue();
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }

  /**
   * A custom pass that consists of two passes that stage-partitions and puts schedule group numbers on the DAG.
   * This is just a simple demonstration of a pass, but it can be flexibly modified for other uses.
   */
  private static final class CustomDefaultPass implements StaticOptimizationPass {
    @Override
    public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
      return new ScheduleGroupPass().process(new DefaultStagePartitioningPass().process(dag));
    }
  }
}
