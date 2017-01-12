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

import com.google.api.client.util.ArrayMap;

import java.util.HashMap;
import java.util.Map;

public class RtStage {
  private final String rtStageId;
  private final Map<String, RtOperator> rtOps;
  private final Map<String, RtOpLink> rtOpLinks;
  private final Map<String, RtStageLink> inputLinks;
  private final Map<String, RtStageLink> outputLinks;
  private final Map<RtAttributes.RtStageAttribute, Object> rtStageAttr;

  public RtStage(final Map<RtAttributes.RtStageAttribute, Object> rtStageAttr) {
    this.rtStageId = IdGenerator.generateRtStageId();
    this.rtOps = new ArrayMap<>();
    this.rtOpLinks = new ArrayMap<>();
    this.inputLinks = new HashMap<>();
    this.outputLinks = new HashMap<>();
    this.rtStageAttr = rtStageAttr;
  }

  public String getId() {
    return rtStageId;
  }

  public RtOperator findOperator(final String rtOpId) {
    return rtOps.get(rtOpId);
  }

  public boolean contains(final String rtOpId) {
    return rtOps.containsKey(rtOpId);
  }

  public void addOperator(final RtOperator rtOp) {
    if (rtOps.containsKey(rtOp.getId()))
      throw new RuntimeException("the given rtOp has been already added");
    rtOps.put(rtOp.getId(), rtOp);
  }

  public void connectOperators(final String srcRtOpId,
                               final String dstRtOpId,
                               final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkAttr) {
    final RtOperator srcRtOp = rtOps.get(srcRtOpId);
    final RtOperator dstRtOp = rtOps.get(dstRtOpId);
    if (srcRtOp == null || dstRtOp == null) {
      throw new RuntimeException("one of given rtOps is not in the stage");
    }

    final RtOpLink rtOpLink = new RtOpLink(srcRtOp, dstRtOp, rtOpLinkAttr);
    srcRtOp.addOutputLink(rtOpLink);
    dstRtOp.addInputLink(rtOpLink);
    rtOpLinks.put(rtOpLink.getRtOpLinkId(), rtOpLink);
  }

  public void addInputLink(final RtStageLink rtStageLink) {
    if (inputLinks.containsKey(rtStageLink.getId())) {
      throw new RuntimeException("the given stage rtStageLink is already in the input link list");
    }
    inputLinks.put(rtStageLink.getId(), rtStageLink);
  }

  public void addOutputLink(final RtStageLink rtStageLink) {
    if (outputLinks.containsKey(rtStageLink.getId())) {
      throw new RuntimeException("the given stage link is already in the output link list");
    }
    outputLinks.put(rtStageLink.getId(), rtStageLink);
  }
}
