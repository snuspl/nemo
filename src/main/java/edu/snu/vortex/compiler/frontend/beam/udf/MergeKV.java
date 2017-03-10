package edu.snu.vortex.compiler.frontend.beam.udf;

import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.UserDefinedFunction;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import java.util.*;
import java.util.stream.Collectors;

public class MergeKV implements UserDefinedFunction {
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
      final KV kv = ((WindowedValue<KV>)element).getValue();
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
    final List<WindowedValue<KV<Object, List>>> grouped = keyToValues.entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .map(kv -> WindowedValue.of(kv, Instant.now(), new ArrayList<>(), PaneInfo.ON_TIME_AND_ONLY_FIRING))
        .collect(Collectors.toList());
    outputCollector.emit(0, grouped);
  }
}

