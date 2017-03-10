package edu.snu.vortex.compiler.frontend.beam.udf;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.UserDefinedFunction;

import java.util.List;

public class MergeKV extends UserDefinedFunction {
  @Override
  public void prepare(final OutputCollector outputCollector) {
  }

  @Override
  public void onData(final List data, final int from) {
  }

  @Override
  public void close() {
  }
}
