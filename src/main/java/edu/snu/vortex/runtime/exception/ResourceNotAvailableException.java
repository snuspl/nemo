package edu.snu.vortex.runtime.exception;

/**
 * Occurs when a required resource is not available.
 */
public final class ResourceNotAvailableException extends RuntimeException {
  /**
   * ResourceNotAvailableException.
   * @param message message
   */
  public ResourceNotAvailableException(final String message) {
    super(message);
  }
}
