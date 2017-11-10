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
package edu.snu.onyx.compiler;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.compiler.frontend.beam.BeamResult;
import edu.snu.onyx.compiler.frontend.beam.OnyxPipelineOptions;
import edu.snu.onyx.compiler.frontend.beam.Visitor;
import edu.snu.onyx.compiler.ir.*;
import edu.snu.onyx.compiler.optimizer.policy.*;
import edu.snu.onyx.examples.beam.*;
import edu.snu.onyx.common.dag.DAG;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Utility methods for tests.
 */
public final class CompilerTestUtil {
  public static final String rootDir = System.getProperty("user.dir");
  public static final String padoPolicy = PadoPolicy.class.getCanonicalName();
  public static final String sailfishDisaggPolicy = SailfishDisaggPolicy.class.getCanonicalName();
  public static final String defaultPolicy = DefaultPolicy.class.getCanonicalName();
  public static final String dataSkewPolicy = DataSkewPolicy.class.getCanonicalName();

  public static DAG<IRVertex, IREdge> compileMRDAG() throws Exception {
    final ArgBuilder mrArgBuilder = MapReduceITCase.builder;
    return BeamCompilerForTest.compile(mrArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSDAG() throws Exception {
    final ArgBuilder alsArgBuilder = AlternatingLeastSquareITCase.builder;
    return BeamCompilerForTest.compile(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSInefficientDAG() throws Exception {
    final String alsInefficient = "edu.snu.onyx.examples.beam.AlternatingLeastSquareInefficient";
    final String input = rootDir + "/src/main/resources/sample_input_als";
    final String numFeatures = "10";
    final String numIteration = "3";
    final String dagDirectory = "./dag";

    final ArgBuilder alsArgBuilder = new ArgBuilder()
        .addJobId(AlternatingLeastSquareInefficient.class.getSimpleName())
        .addUserMain(alsInefficient)
        .addUserArgs(input, numFeatures, numIteration)
        .addDAGDirectory(dagDirectory);
    return BeamCompilerForTest.compile(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileMLRDAG() throws Exception {
    final ArgBuilder mlrArgBuilder = MultinomialLogisticRegressionITCase.builder;
    return BeamCompilerForTest.compile(mlrArgBuilder.build());
  }

  private static final class BeamCompilerForTest {
    private static DAG dag;

    private static DAG<IRVertex, IREdge> compile(final String[] args) throws Exception {
      dag = null;

      final Configuration configuration = JobLauncher.getJobConf(args);
      final Injector injector = Tang.Factory.getTang().newInjector(configuration);
      final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
      final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");

      final PipelineOptions options = PipelineOptionsFactory.create();
      options.setRunner(BeamRunnerForTest.class);

      final Class userCode = Class.forName(className);
      final Method method;
      try {
        method = userCode.getMethod("createPipeline", String[].class, PipelineOptions.class);
      } catch (final NoSuchMethodException e) {
        throw new RuntimeException("For CompilerTestUtil to catch DAG for Beam application, " +
            "createPipeline method on application example is needed for " + className);
      }
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new RuntimeException("User Main Method not static");
      }
      if (!Modifier.isPublic(userCode.getModifiers())) {
        throw new RuntimeException("User Main Class not public");
      }
      final Pipeline pipeline = (Pipeline) method.invoke(null, arguments, options);
      pipeline.run();

      if (dag == null) {
        throw new RuntimeException("DAG not supplied by running pipeline");
      }
      return dag;
    }

    private static void supplyDAGFromRunner(final DAG suppliedDAG) {
      if (dag != null) {
        throw new RuntimeException("Cannot supply dag twice");
      }
      dag = suppliedDAG;
    }
  }

  /**
   * Fake Beam runner for obtaining DAG from client application.
   */
  private static final class BeamRunnerForTest extends PipelineRunner<BeamResult> {
    private final OnyxPipelineOptions onyxPipelineOptions;

    /**
     * Beam Pipeline Runner for testing.
     * @param onyxPipelineOptions PipelineOptions.
     */
    private BeamRunnerForTest(final OnyxPipelineOptions onyxPipelineOptions) {
      this.onyxPipelineOptions = onyxPipelineOptions;
    }

    /**
     * Static initializer for creating PipelineRunner with the given options.
     * @param options given PipelineOptions.
     * @return The created PipelineRunner.
     */
    public static PipelineRunner<BeamResult> fromOptions(final PipelineOptions options) {
      final OnyxPipelineOptions onyxOptions = PipelineOptionsValidator.validate(OnyxPipelineOptions.class, options);
      return new BeamRunnerForTest(onyxOptions);
    }

    /**
     * Method to run the Pipeline.
     * @param pipeline the Pipeline to run.
     * @return The result of the pipeline.
     */
    @Override
    public BeamResult run(Pipeline pipeline) {
      final DAGBuilder builder = new DAGBuilder<>();
      final Visitor visitor = new Visitor(builder, onyxPipelineOptions);
      pipeline.traverseTopologically(visitor);
      final DAG dag = builder.build();
      final BeamResult beamResult = new BeamResult();
      // Supply the dag.
      BeamCompilerForTest.supplyDAGFromRunner(dag);
      return beamResult;
    }
  }
}
