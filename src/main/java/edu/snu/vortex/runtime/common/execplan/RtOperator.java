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
package edu.snu.vortex.runtime.common.execplan;

import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.task.Task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Runtime Operator.
 * @param <I>
 * @param <O>
 */
public abstract class RtOperator<I, O> implements Serializable {
  private final String rtOpId;
  private final Map<RuntimeAttributes.OperatorAttribute, Object> rtOpAttr;

  /**
   * Map of <ID, {@link RtOpLink}> connecting previous {@link RtOperator} to this {@link RtOperator}.
   */
  private Map<String, RtOpLink> inputLinks;

  /**
   * Map of <ID, {@link RtOpLink}> connecting this {@link RtOperator} to the next {@link RtOperator}.
   */
  private Map<String, RtOpLink> outputLinks;

  private final List<Task> taskList;

  public RtOperator(final String irOpId, final Map<RuntimeAttributes.OperatorAttribute, Object> rtOpAttr) {
    this.rtOpId = IdGenerator.generateRtOpId(irOpId);
    this.rtOpAttr = rtOpAttr;
    this.inputLinks = new HashMap<>();
    this.outputLinks = new HashMap<>();
    this.taskList = new ArrayList<>();
  }

  public String getId() {
    return rtOpId;
  }

  public void addAttrbute(final RuntimeAttributes.OperatorAttribute key, final Object value) {
    rtOpAttr.put(key, value);
  }

  public void removeAttrbute(final RuntimeAttributes.OperatorAttribute key) {
    rtOpAttr.remove(key);
  }

  public Map<RuntimeAttributes.OperatorAttribute, Object> getRtOpAttr() {
    return rtOpAttr;
  }

  public void addOutputLink(final RtOpLink rtOpLink) {
    if (outputLinks.containsKey(rtOpLink.getRtOpLinkId())) {
      throw new RuntimeException("the given link is already in the output link list");
    }
    outputLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

  public void addInputLink(final RtOpLink rtOpLink) {
    if (inputLinks.containsKey(rtOpLink.getRtOpLinkId())) {
      throw new RuntimeException("the given link is already in the input link list");
    }
    inputLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

  public Map<String, RtOpLink> getInputLinks() {
    return inputLinks;
  }

  public Map<String, RtOpLink> getOutputLinks() {
    return outputLinks;
  }

  public void addTask(final Task task) {
    this.taskList.add(task);
  }

  public List<Task> getTaskList() {
    return taskList;
  }

  @Override
  public String toString() {
    return "RtOperator{" +
        "rOpId='" + rtOpId + '\'' +
        ", rtOpAttr=" + rtOpAttr +
        ", inputLinks=" + inputLinks +
        ", outputLinks=" + outputLinks +
        '}';
  }
}
