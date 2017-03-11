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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Transform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DoFn operator implementation.
 */
public final class DoFn implements Transform {
  private final org.apache.beam.sdk.transforms.DoFn doFn;
  private final PipelineOptions options;
  private OutputCollector outputCollector;

  public DoFn(final org.apache.beam.sdk.transforms.DoFn doFn, final PipelineOptions options) {
    this.doFn = doFn;
    this.options = options;
  }

  @Override
  public void prepare(final OutputCollector oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final List data, final int from) {
    // Beam-specific processing
    final DoFnInvoker invoker = DoFnInvokers.invokerFor(doFn);
    final ArrayList outputList = new ArrayList<>();
    final ProcessContext beamContext = new ProcessContext<>(doFn, outputList, null, options);
    invoker.invokeSetup();
    invoker.invokeStartBundle(beamContext);
    data.forEach(element -> { // No need to check for input index, since it is always 0 for DoFn
      beamContext.setElement(element);
      invoker.invokeProcessElement(beamContext);
    });
    invoker.invokeFinishBundle(beamContext);
    invoker.invokeTeardown();

    // Output
    outputCollector.emit(0, outputList);
  }

  @Override
  public void close() {
    // do nothing
  }


  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", doFn: ");
    sb.append(doFn);
    return sb.toString();
  }

  /**
   * ProcessContext class. Reference: SimpleDoFnRunner.DoFnProcessContext in BEAM.
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class ProcessContext<I, O> extends org.apache.beam.sdk.transforms.DoFn<I, O>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<I, O> {
    private I inputElement;
    private final Map<PCollectionView, Object> sideInputs;
    private final List<O> outputs;
    private final PipelineOptions options;

    ProcessContext(final org.apache.beam.sdk.transforms.DoFn<I, O> fn,
                   final List<O> outputs,
                   final Map<PCollectionView, Object> sideInputs,
                   final PipelineOptions options) {
      fn.super();
      this.outputs = outputs;
      this.sideInputs = sideInputs;
      this.options = options;
    }

    void setElement(final I element) {
      this.inputElement = element;
    }

    @Override
    public I element() {
      return this.inputElement;
    }

    @Override
    public <T> T sideInput(final PCollectionView<T> view) {
      return (T) sideInputs.get(view);
    }

    @Override
    public Instant timestamp() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PaneInfo pane() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(final O output) {
      outputs.add(output);
    }

    @Override
    public void outputWithTimestamp(final O output, final Instant timestamp) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void sideOutput(final TupleTag<T> tag, final T output) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void sideOutputWithTimestamp(final TupleTag<T> tag, final T output, final Instant timestamp) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        final String name, final Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.beam.sdk.transforms.DoFn.Context context(final org.apache.beam.sdk.transforms.DoFn<I, O> doFn) {
      return this;
    }

    @Override
    public org.apache.beam.sdk.transforms.DoFn.ProcessContext
        processContext(final org.apache.beam.sdk.transforms.DoFn<I, O> doFn) {
      return this;
    }

    @Override
    public org.apache.beam.sdk.transforms.DoFn.OnTimerContext
        onTimerContext(final org.apache.beam.sdk.transforms.DoFn<I, O> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.beam.sdk.transforms.DoFn.InputProvider<I> inputProvider() {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.beam.sdk.transforms.DoFn.OutputReceiver<O> outputReceiver() {
      throw new UnsupportedOperationException();
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
      throw new UnsupportedOperationException();
    }

    @Override
    public State state(final String stateId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timer timer(final String timerId) {
      throw new UnsupportedOperationException();
    }
  }
}

