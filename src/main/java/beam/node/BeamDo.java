package beam.node;

import beam.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Map;

public class BeamDo<I, O> extends dag.node.Do<I, O, PCollectionView> {
  private final DoFn doFn;

  public BeamDo(final DoFn doFn) {
    this.doFn = doFn;
  }

  @Override
  public Iterable<O> compute(final Iterable<I> input, final Map<PCollectionView, Object> broadcasted) {
    final DoFnInvoker<I, O> invoker = DoFnInvokers.invokerFor(doFn);
    final ArrayList<O> outputList = new ArrayList<>();
    final ProcessContext<I, O> context = new ProcessContext<>(doFn, outputList, broadcasted);
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

