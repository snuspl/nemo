package edu.snu.vortex.runtime.common.comm;

import java.io.Serializable;

public final class RtControllable implements Serializable {
  private String senderId;
  private String receiverId;
  private Type type;
  private Serializable message;

  public RtControllable(String senderId, String receiverId, Type type, Serializable message) {
    this.senderId = senderId;
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
