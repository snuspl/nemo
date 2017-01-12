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
  private final List<RStage> rStages;
  private final Map<String, RStageLink> rStageLinks;

  public ExecutionPlan() {
    this.rStages = new LinkedList<>();
    this.rStageLinks = new HashMap<>();
  }
  public void addRStage(final RStage rStage) {
    rStages.add(rStage);
  }

  public void connectRStages(final RStage srcRStage, final RStage dstRStage, final ROpLink rOpLink) throws NoSuchRStageException {
    if (!rStages.contains(srcRStage) || !rStages.contains(dstRStage)) {
      throw new NoSuchRStageException("The requested RStage does not exist in this ExecutionPlan");
    }

    final String rStageLinkId = IdGenerator.generateRStageLinkId(srcRStage.getId(), dstRStage.getId());
    final RStageLink rStageLink = rStageLinks.get(rStageLinkId);

    if (rStageLink == null) {
      rStageLinks.put(rStageLinkId, new RStageLink(rStageLinkId, srcRStage, dstRStage));
    }
    rStageLink.addROpLink(rOpLink);

    srcRStage.addOutputLink(rStageLink);
    dstRStage.addInputLink(rStageLink);
  }

  public List<RStage> getrStages() {
    return rStages;
  }

  public Map<String, RStageLink> getrStageLinks() {
    return rStageLinks;
  }

  public void print() {
    //TODO: print components of this execution grach in DFS gragh traversal.
  }
}
