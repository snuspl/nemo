/*
 * Copyright (C) 2016 Seoul National University
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
package beam;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
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
import java.util.Map;

public class ProcessContext<I, O> extends DoFn<I, O>.ProcessContext implements DoFnInvoker.ArgumentProvider<I, O> {
  private I element;
  private final Map<PCollectionView, Object> broadcasted;
  private final ArrayList<O> outputs;

  public ProcessContext(final DoFn<I, O> fn,
                        final ArrayList<O> outputs,
                        final Map<PCollectionView, Object> broadcasted) {
    fn.super();
    this.outputs = outputs;
    this.broadcasted = broadcasted;
  }

  public void setElement(final I element) {
    this.element = element;
  }

  @Override
  public I element() {
    return this.element;
  }

  @Override
  public <T> T sideInput(final PCollectionView<T> view) {
    return (T)broadcasted.get(view);
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
    throw new UnsupportedOperationException();
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
  protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(final String name, final Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BoundedWindow window() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DoFn<I, O>.Context context(DoFn<I, O> doFn) {
    return this;
  }

  @Override
  public DoFn<I, O>.ProcessContext processContext(DoFn<I, O> doFn) {
    return this;
  }

  @Override
  public DoFn<I, O>.OnTimerContext onTimerContext(DoFn<I, O> doFn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DoFn.InputProvider<I> inputProvider() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DoFn.OutputReceiver<O> outputReceiver() {
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
  public State state(String stateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timer timer(String timerId) {
    throw new UnsupportedOperationException();
  }
}
