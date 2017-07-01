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
package edu.snu.vortex.runtime.executor.partition;

import com.google.protobuf.ByteString;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store partitions in file.
 * It conducts asynchronous write and synchronous read.
 */
final class FileStore implements PartitionStore {

  private final String fileDirectory;
  private final Map<String, UnitFilePartition> partitionIdToData;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  @Inject
  FileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
            final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.fileDirectory = fileDirectory;
    this.partitionIdToData = new ConcurrentHashMap<>();
    this.partitionManagerWorker = partitionManagerWorker;
  }

  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    // Deserialize the target data in the corresponding file and pass it as a local partition.
    final UnitFilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      return Optional.of(new LocalPartition(partition.asIterable()));
    }
  }

  @Override
  public Optional<Integer> putPartition(final String partitionId, final Iterable<Element> data) {
    final UnitFilePartition partition = new UnitFilePartition();
    final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
    if (previousPartition != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    // Serialize the given data
    final PartitionManagerWorker worker = partitionManagerWorker.get();
    final String runtimeEdgeId = partitionId.split("-")[1];
    final Coder coder = worker.getCoder(runtimeEdgeId);
    final ControlMessage.SerializedPartitionMsg.Builder replyBuilder =
        ControlMessage.SerializedPartitionMsg.newBuilder();
    for (final Element element : data) {
      // Memory leak if we don't do try-with-resources here
      try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        coder.encode(element, stream);
        replyBuilder.addData(ByteString.copyFrom(stream.toByteArray()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    final byte[] serialized = replyBuilder.build().toByteArray();
    // Asynchronously write the serialized data to file
    partition.writeData(serialized, coder, fileDirectory + "/" + partitionId);

    return Optional.of(serialized.length);
  }

  @Override
  public Optional<Partition> removePartition(final String partitionId) {
    final UnitFilePartition serializedPartition = partitionIdToData.remove(partitionId);
    if (serializedPartition == null) {
      return Optional.empty();
    }

    // Deserialize the target data, delete the file and pass it as a local partition.
    final Optional<Partition> result = Optional.of(new LocalPartition(serializedPartition.asIterable()));
    serializedPartition.deleteFile();
    return result;
  }

  /**
   * This class represents a {@link Partition} which is stored in {@link FileStore}
   * and not divided in multiple blocks.
   * It does not contain any actual data.
   */
  private final class UnitFilePartition implements Partition {

    private Coder coder;
    private Path filePath;
    private int size;
    private final CompletableFuture<Integer> writeFuture;

    /**
     * Construct a file partition.
     * For the synchronicity of the partition map, it does not write data at the construction time.
     */
    private UnitFilePartition() {
      this.writeFuture = new CompletableFuture<>();
    }

    private void writeData(final byte[] serializedData,
                           final Coder coderToSet,
                           final String filePathToSet) {
      this.coder = coderToSet;
      this.filePath = Paths.get(filePathToSet);
      this.size = serializedData.length;
      // Wrap the given serialized data (but not copy it)
      final ByteBuffer buf = ByteBuffer.wrap(serializedData);

      // Write asynchronously
      try (final AsynchronousFileChannel asyncFileChannel = AsynchronousFileChannel.open(filePath,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        asyncFileChannel.write(buf, 0, writeFuture, new WriteCompletionHandler());
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    private void deleteFile() {
      try {
        writeFuture.get();
        Files.delete(filePath);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Iterable<Element> asIterable() {
      // Read file synchronously
      final ControlMessage.SerializedPartitionMsg reply;
      try (
          final FileInputStream fileStream = new FileInputStream(filePath.toString());
          final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileStream)
      ) {
        writeFuture.get();
        reply = ControlMessage.SerializedPartitionMsg.parseFrom(bufferedInputStream);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

      // Deserialize the data
      final ArrayList<Element> deserializedData = new ArrayList<>(reply.getDataCount());
      for (int i = 0; i < reply.getDataCount(); i++) {
        // Memory leak if we don't do try-with-resources here
        try (final InputStream inputStream = reply.getData(i).newInput()) {
          deserializedData.add(coder.decode(inputStream));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      
      return deserializedData;
    }
  }

  /**
   * A {@link CompletionHandler} for write.
   */
  private final class WriteCompletionHandler implements CompletionHandler<Integer, CompletableFuture<Integer>> {

    @Override
    public void completed(Integer result, CompletableFuture<Integer> completableFuture) {
      completableFuture.complete(result);
    }

    @Override
    public void failed(Throwable exc, CompletableFuture future) {
      throw new RuntimeException(exc);
    }
  }
}
