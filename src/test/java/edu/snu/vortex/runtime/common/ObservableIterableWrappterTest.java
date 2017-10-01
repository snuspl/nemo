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
package edu.snu.vortex.runtime.common;

import io.reactivex.schedulers.Schedulers;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ObservableIterableWrapper}
 */
public final class ObservableIterableWrappterTest {
  private static final Logger LOG = LoggerFactory.getLogger(ObservableIterableWrappterTest.class.getName());
  private List<Integer> resultIterable;
  private AtomicBoolean error;
  private AtomicBoolean complete;

  @Before
  public void setUp() {
    resultIterable = new ArrayList<>();
    error = new AtomicBoolean(false);
    complete = new AtomicBoolean(false);
  }

  @Test(timeout = 1000)
  public void testSubscription() {
    final List<Integer> inputList = new ArrayList<>();
    IntStream.range(0, 10).forEach(inputList::add);
    final ObservableIterableWrapper<Integer> observableIterable = new ObservableIterableWrapper<>(inputList);
    observableIterable.subscribe(
        // onNext
        resultIterable::add,
        // onError
        throwable -> {
          error.set(true);
          LOG.error(throwable.toString());
        },
        // onComplete
        () -> complete.set(true));

    // Check whether the subscription is successfully completed.
    assertEquals(inputList, resultIterable);
    assertFalse(error.get());
    assertTrue(complete.get());
  }

  @Test(timeout = 1000)
  public void testBlockingSubscription() throws InterruptedException {
    final CountDownLatch completeLatch = new CountDownLatch(1);
    final ClosableBlockingIterable<Integer> inputList = new ClosableBlockingIterable<>();
    IntStream.range(0, 10).forEach(inputList::add);
    final ObservableIterableWrapper<Integer> observableIterable = new ObservableIterableWrapper<>(inputList);
    observableIterable.subscribeOn(Schedulers.io()).subscribe( // asynchronous subscription.
        // onNext
        resultIterable::add,
        // onError
        throwable -> {
          error.set(true);
          LOG.error(throwable.toString());
        },
        // onComplete
        () -> {
          completeLatch.countDown();
          complete.set(true);
        });

    // Because the blocking iterable is not closed, the subscription have to not finished yet.
    assertFalse(error.get());
    assertFalse(complete.get());

    IntStream.range(10, 20).forEach(inputList::add);
    inputList.close();
    completeLatch.await();

    // Check whether the subscription is successfully completed.
    final Iterator inputIterator = inputList.iterator();
    final Iterator resultIterator = resultIterable.iterator();
    while (inputIterator.hasNext()) {
      assertEquals(inputIterator.next(), resultIterator.next());
    }
    assertFalse(resultIterator.hasNext());
    assertFalse(error.get());
    assertTrue(complete.get());
  }
}
