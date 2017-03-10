package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Operator;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;

public class MergeKV implements Operator {
  private final Map<Object, List> keyToValues;
  private OutputCollector outputCollector;

  public MergeKV() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(final List data, final int from) {
    data.forEach(element -> {
      final KV kv = (KV)element;
      final List valueList = keyToValues.get(kv.getKey());
      if (valueList == null) {
        final List newValueList = new ArrayList();
        newValueList.add(kv.getValue());
        keyToValues.put(kv.getKey(), newValueList);
      } else {
        valueList.add(kv.getValue());
      }
    });
  }

  @Override
  public void close() {
    final List<KV<Object, List>> grouped = keyToValues.entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
    outputCollector.emit(0, grouped);
  }
}

