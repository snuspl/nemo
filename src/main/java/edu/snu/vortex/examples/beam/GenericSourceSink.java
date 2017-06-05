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
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Helper class for handling source/sink in a generic way.
 * Assumes String-type Pcollections.
 */
final class GenericSourceSink {
  private GenericSourceSink() {
  }

  public static PCollection<String> read(final Pipeline pipeline,
                                         final String path) {
    if (path.startsWith("hdfs://")) {
      final Configuration hadoopConf = new Configuration(false);
      hadoopConf.setClass("mapreduce.job.inputformat.class", TextInputFormat.class, InputFormat.class);
      hadoopConf.setClass("key.class", LongWritable.class, Object.class);
      hadoopConf.setClass("value.class", Text.class, Object.class);
      return pipeline.apply("read", HadoopInputFormatIO.<LongWritable, Text>read().withConfiguration(hadoopConf))
          .apply(MapElements.into(TypeDescriptor.of(String.class)).via(kv -> kv.getValue().toString()));
    } else {
      return pipeline.apply(TextIO.read().from(path));
    }
  }

  public static PDone write(final PCollection<String> dataToWrite,
                            final String path) {
    if (path.startsWith("hdfs://")) {
      final ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(path);
      /*
      if (!resource.isDirectory()) {
        prefix = verifyNotNull(
            resource.getFilename(),
            "A non-directory resource should have a non-null filename: %s",
            resource);
      }
      */
      System.out.println(resource.getScheme());
      return dataToWrite.apply(TextIO.write().to(resource.getCurrentDirectory()));
    } else {
      return dataToWrite.apply(TextIO.write().to(path));
    }
  }
}
