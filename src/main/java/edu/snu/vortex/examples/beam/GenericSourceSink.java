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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.Date;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for handling source/sink in a generic way.
 * Assumes String-type Pcollections.
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
      return pipeline.apply(TextIO.read().from(path));¶
    }
  }

  /**
   * Write each string element to HDFS.
   */
  public static class HDFSWrite extends DoFn<String, Void> {

    private final String path;

    HDFSWrite(final String path) {
      this.path = path;
    }

    @ProcessElement
    public void processElement(final ProcessElement c) {
      SimpleDateFormat dateFormatTime = new SimpleDateFormat("yyyy:mm:dd:hh:mm:ss");
      String dateFormatTimeStr = dateFormatTime.format(new Date(System.currentTimeMillis()));

      String data = c.toString();
      try (final BufferedWriter writer = new BufferedWriter(
          new FileWriter(path + dateFormatTimeStr))) {

        writer.write(data + "\n");

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {
    if (path.startsWith("hdfs://")) {
      // TODO #268 Import beam-sdks-java-io-hadoop-file-system
      //throw new UnsupportedOperationException("Writing to HDFS is not yet supported");

      LOG.info("HDFS write start: " + System.nanoTime());

      dataToWrite.apply(ParDo.of(new HDFSWrite(path)));

      LOG.info("HDFS write end: " + System.nanoTime());

      return PDone.in(dataToWrite.getPipeline());
    } else {
      return dataToWrite.apply(TextIO.write().to(path));
    }
  }
}
