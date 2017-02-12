package edu.snu.vortex.runtime.common.comm;

import java.io.Serializable;

public final class RtExchangeable implements Serializable {
  private String senderId;
  private String receiverId;
  private Type type;
  private Serializable message;

  public RtExchangeable(String executorId, String receiverId, Type type, Serializable message) {
    this.senderId = executorId;
    this.receiverId = receiverId;
    this.type = type;
    this.message = message;
  }

  public Type getType() {
    return type;
  }

  public Serializable getMessage() {
    return message;
  }

  public enum Type {
    ScheduleTaskGroup,
    TaskStateChanged
  }
}
