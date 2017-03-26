package edu.snu.vortex.runtime.common.message;

import java.util.Objects;

/**
 * A physical address of an endpoint.
 */
public final class EndpointAddress {
  private final String host;
  private final int port;

  public EndpointAddress(final String host, final int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.host, this.port);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof EndpointAddress)) {
      return false;
    }

    final EndpointAddress otherAddress = (EndpointAddress) obj;
    return this.port == otherAddress.port && this.host.equals(otherAddress.host);
  }
}
