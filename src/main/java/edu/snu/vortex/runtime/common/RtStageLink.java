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

public class RtStageLink {
  private String rtStageLinkId;
  private RtStage srcStage;
  private RtStage dstStage;
  private Set<RtOpLink> rtOpLinkSet;

  RtStageLink(final String rtStageLinkId, RtStage srcStage, RtStage dstStage) {
    this.rtStageLinkId = rtStageLinkId;
    this.srcStage = srcStage;
    this.dstStage = dstStage;
    this.rtOpLinkSet = new HashSet<>();
  }

  public void addROpLink(RtOpLink rtOpLink) {
    rtOpLinkSet.add(rtOpLink);
  }

  public String getId() {
    return rtStageLinkId;
  }

  public RtStage getSrcStage() {
    return srcStage;
  }

  public RtStage getDstStage() {
    return dstStage;
  }
}
