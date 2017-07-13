package edu.snu.vortex.runtime.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrate message sender and receiver using {@link CompletableFuture} for asynchronous request-reply communication.
 */
public final class ReplyFutureMap<T> {

  private final ConcurrentHashMap<Long, CompletableFuture<T>> requestIdToFuture;

  public ReplyFutureMap() {
    requestIdToFuture = new ConcurrentHashMap<>();
  }

  /**
   * Called by message sender, whenever a new request is sent.
   * @param id the request id
   * @return a {@link CompletableFuture} for the reply
   */
  public CompletableFuture<T> onRequest(final long id) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    requestIdToFuture.put(id, future);
    return future;
  }

  /**
   * Called by message receiver, for a successful reply message.
   * @param id the request id
   * @param successMessage the reply message
   */
  public void onSuccessMessage(final long id, final T successMessage) {
    requestIdToFuture.remove(id).complete(successMessage);
  }

  /**
   * Called for a failure in request-reply communication.
   * @param id the request id
   * @param ex throwable exception
   */
  public void onFailure(final long id, final Throwable ex) {
    requestIdToFuture.remove(id).completeExceptionally(ex);
  }
}
