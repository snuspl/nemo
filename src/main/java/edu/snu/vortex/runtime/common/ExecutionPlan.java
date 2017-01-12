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

import edu.snu.vortex.runtime.exception.NoSuchRStageException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ExecutionPlan {
  private final List<RtStage> rtStages;
  private final Map<String, RtStageLink> rtStageLinks;

  public ExecutionPlan() {
    this.rtStages = new LinkedList<>();
    this.rtStageLinks = new HashMap<>();
  }
  public void addRStage(final RtStage rtStage) {
    rtStages.add(rtStage);
  }

  public void connectRStages(final RtStage srcRtStage,
                             final RtStage dstRtStage,
                             final RtOpLink rtOpLink) throws NoSuchRStageException {
    if (!rtStages.contains(srcRtStage) || !rtStages.contains(dstRtStage)) {
      throw new NoSuchRStageException("The requested RtStage does not exist in this ExecutionPlan");
    }

    final String rStageLinkId = IdGenerator.generateRtStageLinkId(srcRtStage.getId(), dstRtStage.getId());
    RtStageLink rtStageLink = rtStageLinks.get(rStageLinkId);

    if (rtStageLink == null) {
      rtStageLink = new RtStageLink(rStageLinkId, srcRtStage, dstRtStage);
      rtStageLinks.put(rStageLinkId, rtStageLink);
    }
    rtStageLink.addROpLink(rtOpLink);

    srcRtStage.addOutputLink(rtStageLink);
    dstRtStage.addInputLink(rtStageLink);
  }

  public List<RtStage> getRtStages() {
    return rtStages;
  }

  public Map<String, RtStageLink> getRtStageLinks() {
    return rtStageLinks;
  }

  public void print() {
    //TODO: print components of this execution grach in DFS gragh traversal.
    System.out.println("To be implemented.");
  }
}
