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

import java.util.Map;

public class ROpLink {
  private String rOpLinkId;
  private ROperator srcROper;
  private ROperator destROper;
  private Map<RAttributes.ROpLinkAttribute, Object> attributes;

  public ROpLink(ROperator srcROper, ROperator destROper,
                 Map<RAttributes.ROpLinkAttribute, Object> attributes) {
    this.rOpLinkId = IdGenerator.generateComponentId();
    this.srcROper = srcROper;
    this.destROper = destROper;
    this.attributes = attributes;
  }

  public String getId() {
    return rOpLinkId;
  }

  public ROperator getSrcROperator() {
    return srcROper;
  }

  public ROperator getDestROperator() {
    return destROper;
  }

  public Map<RAttributes.ROpLinkAttribute, Object> getAttributes() {
    return attributes;
  }
}
