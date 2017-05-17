package edu.snu.vortex.runtime.common.message.ncs;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public final class NcsParameters {

  @NamedParameter
  public static final class SenderId implements Name<String> {
  }
}
