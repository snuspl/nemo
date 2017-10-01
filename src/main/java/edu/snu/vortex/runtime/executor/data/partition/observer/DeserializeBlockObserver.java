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
package edu.snu.vortex.runtime.executor.data.partition.observer;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.ClosableBlockingIterable;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;
import io.reactivex.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * The {@link io.reactivex.Observer} handling block subscription.
 * If a committed block in the file is in a specific {@link HashRange}, it will be deserialized into {@link Element}s.
 * This observer is intended to run on a scheduler which is designed to serve blocking tasks,
 * such as {@link io.reactivex.schedulers.Schedulers#io()}.
 * However, deserialization needs quite lots of computation, which is not recommended to do on this kind of schedulers.
 * Therefore, it uses a separate executor service for deserialization.
 */
public final class DeserializeBlockObserver extends CommittedBlockParseObserver<BlockMetadata, Element> {
  private static final Logger LOG = LoggerFactory.getLogger(DeserializeBlockObserver.class.getName());
  private final FileInputStream fileStream;
  private final Coder coder;
  private final ExecutorService executorService; // Executor service for deserialization.

  public DeserializeBlockObserver(final HashRange hashRange,
                                  final CompletableFuture<Iterable<Element>> iterableFuture,
                                  final String filePath,
                                  final Coder coder,
                                  final ExecutorService executorService) throws IOException {
    super(hashRange, iterableFuture);
    this.fileStream = new FileInputStream(filePath);
    this.coder = coder;
    this.executorService = executorService;
  }

  @Override
  public synchronized void onNext(@NonNull final BlockMetadata blockMetadata) {
    // Check if the committed block is included in the target key range.
    final HashRange hashRange = getHashRange();
    try {
      if (hashRange.includes(blockMetadata.getHashValue())) {
        final ClosableBlockingIterable<Element> elementsInRange = getResultInRange();
        // Deserialize the
        final Supplier<Iterable<Element>> supplier = () -> deserializeBlock(blockMetadata, fileStream);
        final Iterable<Element> deserializedElements =
            CompletableFuture.supplyAsync(supplier, executorService).get();
        deserializedElements.forEach(elementsInRange::add);
      } else {
        // Have to skip this block.
        final long bytesToSkip = blockMetadata.getBlockSize();
        final long skippedBytes = fileStream.skip(bytesToSkip);
        if (skippedBytes != bytesToSkip) {
          throw new IOException("The file stream failed to skip to the next block.");
        }
      }
    } catch (final IOException | InterruptedException | ExecutionException e) {
      this.onError(e);
    }
  }

  @Override
  public synchronized void onError(@NonNull final Throwable throwable) {
    LOG.error(throwable.toString());
    stopSubscription();
  }

  @Override
  public synchronized void onComplete() {
    stopSubscription();
  }

  private void stopSubscription() {
    getResultInRange().close();
    try {
      fileStream.close();
    } catch (final IOException e) {
      LOG.error(e.toString());
    }
  }

  /**
   * Reads and deserializes a block.
   *
   * @param blockMetadata   the block metadata.
   * @param fileInputStream the stream contains the actual data.
   * @return the block consist of the deserialized elements.
   */
  private Iterable<Element> deserializeBlock(final BlockMetadata blockMetadata,
                                             final FileInputStream fileInputStream) {
    final int size = blockMetadata.getBlockSize();
    final long numElements = blockMetadata.getElementsTotal();
    final List<Element> deserializedData = new ArrayList<>();
    if (size != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, size);
      for (int i = 0; i < numElements; i++) {
        deserializedData.add(coder.decode(bufferedInputStream));
      }
    }
    return deserializedData;
  }
}
