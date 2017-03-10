package edu.snu.vortex.compiler.ir;

import java.util.List;

public interface Operator {
  void prepare(final OutputCollector outputCollector);

  void onData(final List data, final int from);

  void close();

}
