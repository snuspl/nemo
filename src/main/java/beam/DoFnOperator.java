package beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;

import java.util.ArrayList;

class DoFnOperator<I, O> implements dag.Operator<I, O> {
  private final DoFn<I, O> doFn;

  DoFnOperator(final DoFn<I, O> doFn) {
    this.doFn = doFn;
  }

  @Override
  public String toString() {
    return doFn.toString();
  }

  @Override
  public Iterable<O> compute(final Iterable<I> input) {
    final DoFnInvoker<I, O> invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(doFn);
    final ArrayList<O> outputList = new ArrayList<>();
    final ProcessContext<I, O> context = new ProcessContext<>(doFn, outputList);
    invoker.invokeSetup();
    invoker.invokeStartBundle(context);
    input.forEach(element -> {
      context.setElement(element);
      invoker.invokeProcessElement(context, null);
    });
    invoker.invokeFinishBundle(context);
    invoker.invokeTeardown();
    return outputList;
  }
}
