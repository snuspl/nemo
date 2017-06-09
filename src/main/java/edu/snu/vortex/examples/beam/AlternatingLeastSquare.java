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

import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;
import edu.snu.vortex.client.beam.LoopCompositeTransform;
import edu.snu.vortex.compiler.frontend.beam.Runner;
import edu.snu.vortex.compiler.frontend.beam.coder.PairCoder;
import edu.snu.vortex.utils.Pair;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang.ArrayUtils;
import org.netlib.util.intW;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample Alternating Least Square application.
 */
public final class AlternatingLeastSquare {
  private static final Logger LOG = Logger.getLogger(AlternatingLeastSquare.class.getName());

  /**
   * Private constructor.
   */
  private AlternatingLeastSquare() {
  }

  /**
   * Method for parsing the input line.
   */
  public static final class ParseLine extends DoFn<String, KV<Integer, Pair<Integer[], Float[]>>> {
    private final Boolean isUserData;

    /**
     * Constructor for Parseline DoFn class.
     * @param isUserData flag that distinguishes user data from item data.
     */
    public ParseLine(final Boolean isUserData) {
      this.isUserData = isUserData;
    }

    /**
     * ProcessElement method for BEAM.
     * @param c Process context.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final String text = c.element().trim();
      if (text.startsWith("#") || text.length() == 0) {
        // comments and empty lines
        return;
      }

      final String[] split = text.split("\\s+|:");
      final Integer userId = Integer.parseInt(split[0]);
      final Integer itemId = Integer.parseInt(split[1]);
      final Float rating = Float.parseFloat(split[2]);


      final Integer[] userAry = new Integer[1];
      userAry[0] = userId;

      final Integer[] itemAry = new Integer[1];
      itemAry[0] = itemId;

      final Float[] ratingAry = new Float[1];
      ratingAry[0] = rating;

      if (isUserData) {
        c.output(KV.of(userId, Pair.of(itemAry, ratingAry)));
      } else {
        c.output(KV.of(itemId, Pair.of(userAry, ratingAry)));
      }
    }
  }

  /**
   * Combiner for the training data.
   */
  public static final class TrainingDataCombiner extends
      Combine.CombineFn<Pair<Integer[], Float[]>, List<Pair<Integer[], Float[]>>, Pair<Integer[], Float[]>> {

    @Override
    public List<Pair<Integer[], Float[]>> createAccumulator() {
      return new LinkedList<>();
    }

    @Override
    public List<Pair<Integer[], Float[]>> addInput(final List<Pair<Integer[], Float[]>> accumulator,
                                               final Pair<Integer[], Float[]> value) {
      accumulator.add(value);
      return accumulator;
    }

    @Override
    public List<Pair<Integer[], Float[]>> mergeAccumulators(
        final Iterable<List<Pair<Integer[], Float[]>>> accumulators) {
      final List<Pair<Integer[], Float[]>> merged = new LinkedList<>();
      accumulators.forEach(merged::addAll);
      return merged;
    }

    @Override
    public Pair<Integer[], Float[]> extractOutput(final List<Pair<Integer[], Float[]>> accumulator) {
      Integer dimension = 0;
      for (final Pair<Integer[], Float[]> pair : accumulator) {
        dimension += pair.left().length;
      }

      final Integer[] intArr = new Integer[dimension];
      final Float[] floatArr = new Float[dimension];

      Integer itr = 0;
      for (final Pair<Integer[], Float[]> pair : accumulator) {
        final Integer[] ints = pair.left();
        final Float[] floats = pair.right();
        for (Integer i = 0; i < ints.length; i++) {
          intArr[itr] = ints[i];
          floatArr[itr] = floats[i];
          itr++;
        }
      }

      return Pair.of(intArr, floatArr);
    }
  }

  /**
   * DoFn for calculating next matrix at each iteration.
   */
  public static final class CalculateNextMatrix
      extends DoFn<KV<Integer, Pair<Integer[], Float[]>>, KV<Integer, Float[]>> {
    private static final LAPACK NETLIB_LAPACK = LAPACK.getInstance();
    private static final BLAS NETLIB_BLAS = BLAS.getInstance();

    private final List<KV<Integer, Float[]>> results;
    private final Double[] upperTriangularLeftMatrix;
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollectionView<Map<Integer, Float[]>> fixedMatrixView;

    /**
     * Constructor for CalculateNextMatrix DoFn class.
     * @param numFeatures number of features.
     * @param lambda lambda.
     * @param fixedMatrixView a PCollectionView of the fixed matrix (item / user matrix).
     */
    CalculateNextMatrix(final Integer numFeatures, final Double lambda,
                        final PCollectionView<Map<Integer, Float[]>> fixedMatrixView) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.fixedMatrixView = fixedMatrixView;
      this.results = new LinkedList<>();
      this.upperTriangularLeftMatrix = new Double[numFeatures * (numFeatures + 1) / 2];
    }

    /**
     * ProcessElement method for BEAM.
     * @param c ProcessContext.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      for (Integer j = 0; j < upperTriangularLeftMatrix.length; j++) {
        upperTriangularLeftMatrix[j] = 0.0;
      }

      final Map<Integer, Float[]> fixedMatrix = c.sideInput(fixedMatrixView);

      final Integer[] indexArr = c.element().getValue().left();
      final Float[] ratingArr = c.element().getValue().right();

      final Integer size = indexArr.length;

      final Float[] vector = new Float[numFeatures];
      final double[] rightSideVector = new double[numFeatures];
      final Double[] tmp = new Double[numFeatures];
      for (Integer i = 0; i < size; i++) {
        final Integer ratingIndex = indexArr[i];
        final Float rating = ratingArr[i];
        for (Integer j = 0; j < numFeatures; j++) {
//          LOG.log(Level.INFO, "Rating index " + ratingIndex);
          tmp[j] = fixedMatrix.get(ratingIndex)[j].doubleValue();
        }


        NETLIB_BLAS.dspr("U", numFeatures, 1.0, ArrayUtils.toPrimitive(tmp), 1,
            ArrayUtils.toPrimitive(upperTriangularLeftMatrix));
        if (rating != 0.0) {
          NETLIB_BLAS.daxpy(numFeatures, rating, ArrayUtils.toPrimitive(tmp), 1, rightSideVector, 1);
        }
      }

      final Double regParam = lambda * size;
      Integer a = 0;
      Integer b = 2;
      while (a < upperTriangularLeftMatrix.length) {
        upperTriangularLeftMatrix[a] += regParam;
        a += b;
        b += 1;
      }

      final intW info = new intW(0);

      NETLIB_LAPACK.dppsv("U", numFeatures, 1, ArrayUtils.toPrimitive(upperTriangularLeftMatrix),
          rightSideVector, numFeatures, info);
      if (info.val != 0) {
        throw new RuntimeException("returned info value : " + info.val);
      }

      for (Integer i = 0; i < vector.length; i++) {
        vector[i] = (float) rightSideVector[i];
      }

      results.add(KV.of(c.element().getKey(), vector));
    }

    /**
     * FinishBundle method for BEAM.
     * @param c Context.
     */
    @FinishBundle
    public void finishBundle(final FinishBundleContext c) {
      results.forEach(r -> c.output(r, null, null));
    }
  }

  /**
   * Composite transform that wraps the transforms inside the loop.
   * The loop updates the user matrix and the item matrix in each iteration.
   */
  public static final class UpdateUserAndItemMatrix extends LoopCompositeTransform<
      PCollection<KV<Integer, Float[]>>, PCollection<KV<Integer, Float[]>>> {
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedUserData;
    private final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedItemData;

    /**
     * Constructor of UpdateUserAndItemMatrix CompositeTransform.
     * @param numFeatures number of features.
     * @param lambda lambda.
     * @param parsedUserData PCollection of parsed user data.
     * @param parsedItemData PCollection of parsed item data.
     */
    UpdateUserAndItemMatrix(final Integer numFeatures, final Double lambda,
                            final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedUserData,
                            final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.parsedUserData = parsedUserData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, Float[]>> expand(final PCollection<KV<Integer, Float[]>> itemMatrix) {
      // Make Item Matrix view.
      final PCollectionView<Map<Integer, Float[]>> itemMatrixView = itemMatrix.apply(View.asMap());
      // Get new User Matrix
      final PCollectionView<Map<Integer, Float[]>> userMatrixView = parsedUserData
          .apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, itemMatrixView)).withSideInputs(itemMatrixView))
          .apply(View.asMap());
      // return new Item Matrix
      return parsedItemData.apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, userMatrixView))
          .withSideInputs(userMatrixView));
    }
  }

  /**
   * Main function for the ALS BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final Long start = System.currentTimeMillis();
    LOG.log(Level.INFO, Arrays.toString(args));
    final String inputFilePath = args[0];
    final Integer numFeatures = Integer.parseInt(args[1]);
    final Integer numItr = Integer.parseInt(args[2]);
    final Double lambda;
    if (args.length > 4) {
      lambda = Double.parseDouble(args[3]);
    } else {
      lambda = 0.05;
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);
    options.setJobName("ALS");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(Pair.class, PairCoder.class));

    // Read raw data
    final PCollection<String> rawData = GenericSourceSink.read(p, inputFilePath);

    // Parse data for item
    final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedItemData = rawData
        .apply(ParDo.of(new ParseLine(false)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

    // Parse data for user
    final PCollection<KV<Integer, Pair<Integer[], Float[]>>> parsedUserData = rawData
        .apply(ParDo.of(new ParseLine(true)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

    // Create Initial Item Matrix
    PCollection<KV<Integer, Float[]>> itemMatrix = parsedItemData
        .apply(ParDo.of(new DoFn<KV<Integer, Pair<Integer[], Float[]>>, KV<Integer, Float[]>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            final Float[] result = new Float[numFeatures];
            result[0] = 0f;

            final KV<Integer, Pair<Integer[], Float[]>> element = c.element();
            final Float[] ratings = element.getValue().right();
            for (Integer i = 0; i < ratings.length; i++) {
              result[0] += ratings[i];
            }

            result[0] /= ratings.length;
            for (Integer i = 1; i < result.length; i++) {
              result[i] = (float) (Math.random() * 0.01);
            }
            c.output(KV.of(element.getKey(), result));
          }
        }));


    // Iterations to update Item Matrix.
    for (Integer i = 0; i < numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      itemMatrix = itemMatrix.apply(new UpdateUserAndItemMatrix(numFeatures, lambda, parsedUserData, parsedItemData));
    }

    p.run();
    LOG.log(Level.INFO, "JCT " + (System.currentTimeMillis() - start));
  }
}
