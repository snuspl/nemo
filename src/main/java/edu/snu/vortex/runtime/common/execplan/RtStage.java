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

import com.google.api.client.util.ArrayMap;
import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.task.TaskGroup;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Runtime Stage.
 */
public final class RtStage {
  private final String rtStageId;
  private final Map<RuntimeAttributes.StageAttribute, Object> rtStageAttr;

  /**
   * Map of <ID, {@link RtOperator}> contained in this {@link RtStage}.
   */
  private final Map<String, RtOperator> rtOps;

  /**
   * Map of <ID, {@link RtOperator}> contained in this {@link RtStage}.
   */
  private final List<RtOperator> rtOperatorList;

  /**
   * Map of <ID, {@link RtOpLink}> connecting the {@link RtStage#rtOps} contained in this {@link RtStage}.
   */
  private final Map<String, RtOpLink> rtOpLinks;

  /**
   * Map of <ID, {@link RtStageLink}> connecting previous {@link RtStage} to this {@link RtStage}.
   */
  private final Map<String, RtStageLink> inputLinks;

  /**
   * Map of <ID, {@link RtStageLink}> connecting this {@link RtStage} to next {@link RtStage}.
   */
  private final Map<String, RtStageLink> outputLinks;

  private List<TaskGroup> taskGroups;

  /**
   * Represents a stage containing operators to be executed in Vortex runtime.
   * @param rtStageAttr attributes that can be given and applied to this {@link RtStage}
   */
  public RtStage(final Map<RuntimeAttributes.StageAttribute, Object> rtStageAttr) {
    this.rtStageId = IdGenerator.generateRtStageId();
    this.rtOps = new ArrayMap<>();
    this.rtOperatorList = new LinkedList<>();
    this.rtOpLinks = new ArrayMap<>();
    this.inputLinks = new HashMap<>();
    this.outputLinks = new HashMap<>();
    this.rtStageAttr = rtStageAttr;
    this.taskGroups = new LinkedList<>();
  }

  public String getId() {
    return rtStageId;
  }

  public RtOperator getRtOpById(final String rtOpId) {
    return rtOps.get(rtOpId);
  }


  public boolean contains(final String rtOpId) {
    return rtOps.containsKey(rtOpId);
  }

  public void addRtOp(final RtOperator rtOp) {
    if (rtOps.containsKey(rtOp.getId())) {
      throw new RuntimeException("the given rtOp has been already added");
    }
    rtOps.put(rtOp.getId(), rtOp);

    if (rtOperatorList.contains(rtOp)) {
      throw new RuntimeException("the given rtOp has been already added");
    }
    rtOperatorList.add(rtOp);
  }

  public void connectRtOps(final RtOperator srcRtOp,
                           final RtOperator dstRtOp,
                           final RtOpLink rtOpLink) {
    if (srcRtOp == null || dstRtOp == null) {
      throw new RuntimeException("one of given rtOps is not in the stage");
    }

    srcRtOp.addOutputLink(rtOpLink);
    dstRtOp.addInputLink(rtOpLink);
    rtOpLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

//  public void connectRtOps(final String srcRtOpId,
//                           final String dstRtOpId,
//                           final RtOpLink rtOpLink) {
//    final RtOperator srcRtOp = rtOps.get(srcRtOpId);
//    final RtOperator dstRtOp = rtOps.get(dstRtOpId);
//    if (srcRtOp == null || dstRtOp == null) {
//      throw new RuntimeException("one of given rtOps is not in the stage");
//    }
//
//    srcRtOp.addOutputLink(rtOpLink);
//    dstRtOp.addInputLink(rtOpLink);
//    rtOpLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
//  }

  public void addInputRtStageLink(final RtStageLink rtStageLink) {
    if (inputLinks.containsKey(rtStageLink.getId())) {
      throw new RuntimeException("the given stage rtStageLink is already in the input link list");
    }
    inputLinks.put(rtStageLink.getId(), rtStageLink);
  }

  public void addOutputRtStageLink(final RtStageLink rtStageLink) {
    if (outputLinks.containsKey(rtStageLink.getId())) {
      throw new RuntimeException("the given stage link is already in the output link list");
    }
    outputLinks.put(rtStageLink.getId(), rtStageLink);
  }

  public String getRtStageId() {
    return rtStageId;
  }

  public Map<String, RtStageLink> getInputLinks() {
    return inputLinks;
  }

  public Map<String, RtStageLink> getOutputLinks() {
    return outputLinks;
  }

  public Map<RuntimeAttributes.StageAttribute, Object> getRtStageAttr() {
    return rtStageAttr;
  }

  public Map<String, RtOperator> getRtOps() {
    return rtOps;
  }

  public List<RtOperator> getRtOperatorList() {
    return rtOperatorList;
  }

  public Map<String, RtOpLink> getRtOpLinks() {
    return rtOpLinks;
  }

  public List<TaskGroup> getTaskGroups() {
    return taskGroups;
  }

  public void addTaskGroup(final TaskGroup taskGroup) {
    this.taskGroups.add(taskGroup);
  }

  @Override
  public String toString() {
    return "RtStage{" +
        "rtStageId='" + rtStageId + '\'' +
        ", rtStageAttr=" + rtStageAttr +
        ", rtOps=" + rtOps +
        ", rtOpLinks=" + rtOpLinks +
        ", inputLinks=" + inputLinks +
        ", outputLinks=" + outputLinks +
        '}';
  }
}
