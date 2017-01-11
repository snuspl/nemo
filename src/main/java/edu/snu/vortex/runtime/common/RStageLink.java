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

import java.util.HashSet;
import java.util.Set;

public class RStageLink {
  private String stageLinkId;
  private RStage srcStage;
  private RStage dstStage;
  private Set<ROpLink> rOpLinkSet;

  RStageLink(final String rStageLinkId, RStage srcStage, RStage dstStage) {
    this.stageLinkId = rStageLinkId;
    this.srcStage = srcStage;
    this.dstStage = dstStage;
    this.rOpLinkSet = new HashSet<>();
  }

  public void addROpLink(ROpLink rOpLink) {
    rOpLinkSet.add(rOpLink);
  }

  public String getId() {
    return stageLinkId;
  }

  public RStage getSrcStage() {
    return srcStage;
  }

  public RStage getDstStage() {
    return dstStage;
  }
}
