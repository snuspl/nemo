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

import edu.snu.vortex.runtime.common.ClosableBlockingIterable;
import edu.snu.vortex.runtime.executor.data.HashRange;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;

import java.util.concurrent.CompletableFuture;

/**
 * This abstract class represents an {@link io.reactivex.Observer} handling block subscription.
 * If a committed block information is in a specific {@link HashRange}, it will be parsed.
 * This observer is intended to run on a scheduler which is designed to do some blocking tasks,
 * such as {@link io.reactivex.schedulers.Schedulers#io()}.
 *
 * @param <T> the type of the published information to parse.
 * @param <K> the type of the parsed result.
 */
abstract class CommittedBlockParseObserver<T, K> implements io.reactivex.Observer<T> {
  private final HashRange hashRange;
  private final ClosableBlockingIterable<K> resultInRange;
  private final CompletableFuture<Iterable<K>> iterableFuture;


  protected CommittedBlockParseObserver(final HashRange hashRange,
                                        final CompletableFuture<Iterable<K>> iterableFuture) {
    this.hashRange = hashRange;
    this.resultInRange = new ClosableBlockingIterable<>();
    this.iterableFuture = iterableFuture;
  }

  @Override
  public synchronized void onSubscribe(@NonNull final Disposable disposable) {
    iterableFuture.complete(resultInRange);
  }

  /**
   * @return the hash range.
   */
  protected HashRange getHashRange() {
    return hashRange;
  }

  /**
   * @return the iterable which contains results.
   */
  public ClosableBlockingIterable<K> getResultInRange() {
    return resultInRange;
  }
}
