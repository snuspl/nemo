package beam.examples;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;

public final class ComplexWorkload {
  /*
  public static void main(final String[] args) {
    final PCollection<Integer> maxWordLengthCutOff = ...; // Singleton PCollection
    final PCollectionView<Integer> maxWordLengthCutOffView = maxWordLengthCutOff.apply(View.<Integer>asSingleton());
    final PCollection<String> wordsBelowCutOff = words.apply(ParDo.withSideInputs(maxWordLengthCutOffView)
        .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String word = c.element();
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              c.output(word);
            }
          }}));

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



