package beam.node;

import beam.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;

import java.util.ArrayList;

public class BeamDo<I, O> extends dag.node.Do<I, O> {
  private final DoFn doFn;

  public BeamDo(final DoFn doFn) {
    this.doFn = doFn;
  }

  @Override
  public Iterable<O> compute(Iterable<I> input) {
    final DoFnInvoker<I, O> invoker = DoFnInvokers.invokerFor(doFn);
    final ArrayList<O> outputList = new ArrayList<>();
    final ProcessContext<I, O> context = new ProcessContext<>(doFn, outputList);

    invoker.invokeSetup();
    invoker.invokeStartBundle(context);
    input.forEach(element -> {
      context.setElement(element);
      invoker.invokeProcessElement(context);
    });
    invoker.invokeFinishBundle(context);
    invoker.invokeTeardown();
    return outputList;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", doFn: ");
    sb.append(doFn);
    return sb.toString();
  }
}

