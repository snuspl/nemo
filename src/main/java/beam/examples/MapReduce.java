package beam.examples;

import beam.Runner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public final class MapReduce {
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);

    final Pipeline p = Pipeline.create(options);
    p.apply(TextIO.Read.from(inputFilePath))
        .apply(MapElements.via((String line) -> {
          final String[] words = line.split(" +");
          final String documentId = words[0];
          final Long count = Long.parseLong(words[2]);
          return KV.of(documentId, count);
        }).withOutputType(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(new Sum.SumLongFn()))
        .apply(MapElements.via((KV<String, Long> kv) -> kv.getKey() + ": " + kv.getValue())
            .withOutputType(TypeDescriptors.strings()));
        // .apply(TextIO.Write.to(outputFilePath));
    p.run();
  }
}
