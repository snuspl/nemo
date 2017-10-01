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

import edu.snu.vortex.runtime.executor.data.FileArea;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;
import io.reactivex.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The {@link io.reactivex.Observer} handling block subscription.
 * If a committed block in the file is in a specific {@link HashRange},
 * it's metadata will be parsed into a {@link FileArea}.
 * This observer is intended to run on a scheduler which is designed to do some blocking tasks,
 * such as {@link io.reactivex.schedulers.Schedulers#io()}.
 */
public final class FileAreaObserver extends CommittedBlockParseObserver<BlockMetadata, FileArea> {
  private static final Logger LOG = LoggerFactory.getLogger(FileAreaObserver.class.getName());
  private final String filePath;

  public FileAreaObserver(final HashRange hashRange,
                          final CompletableFuture<Iterable<FileArea>> iterableFuture,
                          final String filePath) {
    super(hashRange, iterableFuture);
    this.filePath = filePath;
  }

  @Override
  public synchronized void onNext(@NonNull final BlockMetadata blockMetadata) {
    // Check if the committed block is included in the target key range.
    final HashRange hashRange = getHashRange();
    if (hashRange.includes(blockMetadata.getHashValue())) {
      getResultInRange().add(new FileArea(filePath, blockMetadata.getOffset(), blockMetadata.getBlockSize()));
    }
  }

  @Override
  public synchronized void onError(@NonNull final Throwable throwable) {
    LOG.error(throwable.toString());
    getResultInRange().close();
  }

  @Override
  public synchronized void onComplete() {
    getResultInRange().close();
  }
}
