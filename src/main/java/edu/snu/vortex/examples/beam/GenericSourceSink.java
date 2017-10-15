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
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

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
   * Write output to HDFS according to the parallelism.
  */
  public static class HDFSWrite extends DoFn<String, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSWrite.class.getName());

    private final String path;
    private static Path fileName;
    private static FileSystem hdfsFileSystem;
    private static FSDataOutputStream outputStream;

    HDFSWrite(final String path) {
      this.path = path;
      this.fileName = null;
      this.hdfsFileSystem = null;
    }

    private static <T> void writeOrClose(final String t) throws Exception {
      try {
          outputStream.writeChars(t + "\n");
          LOG.info("Written {}", t);
      } catch (Exception e) {
        try {
          LOG.debug("Closes and delete file");
          outputStream.close();
          hdfsFileSystem.delete(fileName, true);
          hdfsFileSystem.close();
        } catch (Exception closeException) {
          if (closeException instanceof InterruptedException) {
            // Do not silently ignore interrupted state.
            Thread.currentThread().interrupt();
          }
          // Do not mask the exception that caused the write to fail.
          e.addSuppressed(closeException);
        }
        throw e;
      }
    }

    // The number of output files are determined according to the parallelism.
    // i.e. if parallelism is 2, then there are total 2 output files.
    // Each output file is written as a bundle.
    @StartBundle
    public void startBundle(final StartBundleContext c) {
      fileName = new Path(path + UUID.randomUUID().toString());
      try {
        hdfsFileSystem = fileName.getFileSystem(new JobConf());
        outputStream = hdfsFileSystem.create(fileName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      LOG.info("startBundle");
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      writeOrClose(c.element());
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext c) throws Exception {
      outputStream.close();
      hdfsFileSystem.close();
    }
  }

  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {
    if (path.startsWith("hdfs://")) {
      LOG.info("HDFS writing start");
      dataToWrite.apply(ParDo.of(new HDFSWrite(path)));
      LOG.info("HDFS writing end");
      return PDone.in(dataToWrite.getPipeline());
    } else {
      return dataToWrite.apply(TextIO.write().to(path));
    }
  }
}
