/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for handling source/sink in a generic way.
 * Assumes String-type PCollections.
 */
final class GenericSourceSink {
  private static final Logger LOG = LoggerFactory.getLogger(GenericSourceSink.class.getName());

  private GenericSourceSink() {
  }

  public static PCollection<String> read(final Pipeline pipeline,
                                         final String path) {
    if (path.startsWith("hdfs://")) {
      final Configuration hadoopConf = new Configuration(false);
      hadoopConf.set("mapreduce.input.fileinputformat.inputdir", path);
      hadoopConf.setClass("mapreduce.job.inputformat.class", TextInputFormat.class, InputFormat.class);
      hadoopConf.setClass("key.class", LongWritable.class, Object.class);
      hadoopConf.setClass("value.class", Text.class, Object.class);

      // Without translations, Beam internally does some weird cloning
      final HadoopInputFormatIO.Read<Long, String> read = HadoopInputFormatIO.<Long, String>read()
          .withConfiguration(hadoopConf)
          .withKeyTranslation(new SimpleFunction<LongWritable, Long>() {
            @Override
            public Long apply(final LongWritable longWritable) {
              return longWritable.get();
            }
          })
          .withValueTranslation(new SimpleFunction<Text, String>() {
            @Override
            public String apply(final Text text) {
              return text.toString();
            }
          });
      return pipeline.apply(read).apply(MapElements.into(TypeDescriptor.of(String.class)).via(KV::getValue));
    } else {
      return pipeline.apply(TextIO.read().from(path));
    }
  }

  /**
   * Write HDFS according to the parallelism.
  */
  public static class HDFSWrite extends DoFn<String, Void> {

    private final String dir;
    private BufferedWriter writer;

    HDFSWrite(final String dir) {

      this.dir = dir;
    }

    @StartBundle
    public void startBundle(final StartBundleContext c) {
      // Reset state in case of reuse. We need to make sure that each bundle gets unique writers.
      writer = null;
    }

    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow boundedWindow) throws Exception {
      // Cache a single writer for the bundle.
      if (writer == null) {
        // Make unique file path
        Random random = new Random();
        SimpleDateFormat dateFormatTime = new SimpleDateFormat("yyyy:mm:dd:hh:mm:ss");
        String dateFormatTimeStr = dateFormatTime.format(new Date(System.currentTimeMillis()));
        String filePath = dir + dateFormatTimeStr + "-" + random.nextGaussian();
        LOG.info("Bundle created and processing");
        writer = new BufferedWriter(new FileWriter(filePath, true));
      }
      writer.write(c.element() + "\n");
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext c) throws Exception {
      if (writer == null) {
        LOG.info("finishBundle: writer is null, returning");
        return;
      }
      writer.close();
    }
  }

  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {

    if (path.startsWith("hdfs://")) {
      dataToWrite.apply(ParDo.of(new HDFSWrite(path)));
      return PDone.in(dataToWrite.getPipeline());
    } else {
      return dataToWrite.apply(TextIO.write().to(path));
    }
  }
}

/**
 * Default filename policy for Vortex.
 */
final class VortexDefaultFilenamePolicy {

  //private final Logger LOG = LoggerFactory.getLogger(VortexDefaultFilenamePolicy.class);
  public final String defaultShardTemplate = ShardNameTemplate.INDEX_OF_MAX;
  private final Pattern shardFormatRe = Pattern.compile("(S+|N+)");

  private final String prefix;
  private final String shardTemplate;
  private final String suffix;

  VortexDefaultFilenamePolicy(final String prefix) {
    this.prefix = prefix;
    shardTemplate = defaultShardTemplate;
    suffix = "";
  }

  /**
   * Constructs a fully qualified name from components.
   *
   * <p>The name is built from a prefix, shard template (with shard numbers
   * applied), and a suffix.  All components are required, but may be empty
   * strings.
   *
   * <p>Within a shard template, repeating sequences of the letters "S" or "N"
   * are replaced with the shard number, or number of shards respectively.  The
   * numbers are formatted with leading zeros to match the length of the
   * repeated sequence of letters.
   *
   * <p>For example, if prefix = "output", shardTemplate = "-SSS-of-NNN", and
   * suffix = ".txt", with shardNum = 1 and numShards = 100, the following is
   * produced:  "output-001-of-100.txt".
   *
   * @param prefix1 prefix.
   * @param shardNum shardNum.
   * @return String.
   */
  public String constructName(final String prefix1,
                              final int shardNum) {
    // Matcher API works with StringBuffer, rather than StringBuilder.
    StringBuffer sb = new StringBuffer();
    sb.append(prefix1);

    //LOG.info("prefix: {} shardTemplate: {} shardNum: {} numShards: {}", prefix1, shardTemplate, shardNum);

    Matcher m = shardFormatRe.matcher(shardTemplate);
    while (m.find()) {
      boolean isShardNum = (m.group(1).charAt(0) == 'S');

      char[] zeros = new char[m.end() - m.start()];
      Arrays.fill(zeros, '0');
      DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
      String formatted = df.format(isShardNum ? shardNum : shardNum);
      m.appendReplacement(sb, formatted);
    }
    m.appendTail(sb);

    sb.append(suffix);
    return sb.toString();
  }
}
