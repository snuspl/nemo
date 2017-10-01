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

import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;

import java.util.Iterator;

/**
 * Observable wrapper of an iterable.
 * When this observable is subscribed, all elements of the iterable will be passed by {@link Observer#onNext(Object)}.
 * If the wrapped iterable is a blocking iterable such as {@link ClosableBlockingIterable},
 * the subscriber can receive the data until the iterable is closed.
 * For the further information, check {@link Observable}.
 *
 * @param <T> the type of element of the wrapping iterable.
 */
public final class ObservableIterableWrapper<T> extends Observable<T> {

  private final Iterable<T> iterableToWrap;

  public ObservableIterableWrapper(final Iterable<T> iterableToWrap) {
    this.iterableToWrap = iterableToWrap;
  }

  /**
   * Conducts the actual subscription process and return the {@link IterDisposable}.
   * When this observable is subscribed, all elements of the iterable will be passed by {@link Observer#onNext(Object)}.
   *
   * @param observer the observer who subscribes the iterable.
   * @see Observable#subscribeActual(Observer).
   */
  @Override
  protected void subscribeActual(final Observer<? super T> observer) {
    final IterDisposable<T> disposable = new IterDisposable<>(observer, iterableToWrap);
    observer.onSubscribe(disposable);
    disposable.start();
  }

  /**
   * {@link Disposable} which will be returned by {@link ObservableIterableWrapper} when it is subscribed.
   * The subscriber can get the iterable or cease the subscription.
   *
   * @param <K> the type of element of the iterable.
   */
  public final class IterDisposable<K> implements Disposable {

    private final Observer<? super K> observer;
    private final Iterable<K> iterable;
    private volatile boolean disposed;

    private IterDisposable(final Observer<? super K> observer,
                           final Iterable<K> iterable) {
      this.observer = observer;
      this.iterable = iterable;
    }

    /**
     * Start the subscription.
     * All elements of the iterable will be passed by {@link Observer#onNext(Object)}.
     */
    private void start() {
      final Iterator<? extends K> iterator = iterable.iterator();

      while (iterator.hasNext() && !isDisposed()) {
        observer.onNext(iterator.next());
      }

      if (!isDisposed()) {
        observer.onComplete();
      }
    }

    /**
     * @return the wrapped iterable.
     */
    public Iterable getIterable() {
      return iterable;
    }

    /**
     * Dispose the resource.
     *
     * @see Disposable#dispose().
     */
    @Override
    public void dispose() {
      disposed = true;
    }

    /**
     * @return true if this resource has been disposed.
     */
    @Override
    public boolean isDisposed() {
      return disposed;
    }
  }
}
