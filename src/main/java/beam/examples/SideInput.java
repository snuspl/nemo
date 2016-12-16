package beam.examples;

import beam.Runner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Optional;
import java.util.stream.StreamSupport;

public final class SideInput {
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> elemCollection = p.apply(TextIO.Read.from(inputFilePath));
    final PCollectionView<Iterable<String>> allCollection = elemCollection.apply(View.<String>asIterable());

    elemCollection.apply(ParDo.withSideInputs(allCollection)
        .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            final String line = c.element();
            final Iterable<String> all = c.sideInput(allCollection);
            final Optional<String> appended = StreamSupport.stream(all.spliterator(), false)
                .reduce((l, r) -> l + '%' + r);
            if (appended.isPresent()) {
              c.output("line: " + line + ", all: " + appended.get());
            } else {
              c.output("error");
            }
          }}));

    p.run();
  }

  /*
  public static void main(final String[] args) {

    final int wordLengthCutOff = 10;
    // Create tags to use for the main and side outputs.
    final TupleTag<String> wordsBelowCutOffTag = new TupleTag<String>(){};
    final TupleTag<Integer> wordLengthsAboveCutOffTag = new TupleTag<Integer>(){};
    final TupleTag<String> markedWordsTag = new TupleTag<String>(){};
    PCollectionTuple results = words.apply(
        ParDo.withOutputTags(wordsBelowCutOffTag, TupleTagList.of(wordLengthsAboveCutOffTag).and(markedWordsTag))
            .of(new DoFn<String, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                String word = c.element();
                if (word.length() <= wordLengthCutOff) {
                  // Emit this short word to the main output.
                  c.output(word);
                } else {
                  // Emit this long word's length to a side output.
                  c.sideOutput(wordLengthsAboveCutOffTag, word.length());
                }
                if (word.startsWith("MARKER")) {
                  // Emit this word to a different side output.
                  c.sideOutput(markedWordsTag, word);
                }
              }}));
    // Extract the PCollection results, by tag.
    final PCollection<String> wordsBelowCutOff = results.get(wordsBelowCutOffTag);
    final PCollection<Integer> wordLengthsAboveCutOff = results.get(wordLengthsAboveCutOffTag);
    final PCollection<String> markedWords = results.get(markedWordsTag);


  }
*/
}



