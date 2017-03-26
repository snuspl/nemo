package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * An address for vortex communication following general 'uri' form like below.
 * "vortex://{{message_name}}@{{host}}:{{port}}"
 */
public final class MessageAddress implements Serializable {
  private EndpointAddress endpointAddress;
  private final String name;

  public MessageAddress(final String uriString) {
    try {
      final URI uri = new URI(uriString);
      this.endpointAddress = new EndpointAddress(uri.getHost(), uri.getPort());
      this.name = uri.getUserInfo();
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public MessageAddress(final String host, final int port, final String name) {
    this.endpointAddress = new EndpointAddress(host, port);
    this.name = name;
  }

  public String getHost() {
    return endpointAddress.getHost();
  }

  public int getPort() {
    return endpointAddress.getPort();
  }

  public EndpointAddress getEndpointAddress() {
    return endpointAddress;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "vortex://" + name + "@" + endpointAddress.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.endpointAddress, this.name);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof MessageAddress)) {
      return false;
    }

    final MessageAddress otherAddress = (MessageAddress) obj;
    return this.name.equals(otherAddress.name) && this.endpointAddress.equals(otherAddress.endpointAddress);
  }
}
