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

import java.util.Map;

public class RStage {
  private final String stageId;
  private Map<String, ROperator> operators;
  private Map<String, ROpLink> operLinks;
  private Map<String, RStageLink> inputLinks;
  private Map<String, RStageLink> outputLinks;

  public RStage() {
    this.stageId = IdGenerator.generateComponentId();
    this.operators = new ArrayMap<>();
    this.operLinks = new ArrayMap<>();
  }

  public String getId() {
    return stageId;
  }

  public void addOperator(ROperator operator) {
    if (operators.containsKey(operator.getId()))
      throw new RuntimeException("the given operator has been already added");
    operators.put(operator.getId(), operator);
  }

  public void connectOperators(String srcOperId, String destOperId, RAttributes opLinkAttributs) {
    ROperator srcOper = operators.get(srcOperId);
    ROperator destOper = operators.get(destOperId);
    if (srcOper == null || destOper == null)
      throw new RuntimeException("one of given operators is not in the stage");

    ROpLink operLink = new ROpLink(srcOper, destOper, opLinkAttributs);
    srcOper.addOutputLink(operLink);
    destOper.addInputLink(operLink);
    operLinks.put(operLink.getId(), operLink);
  }

  public void addInputLink(RStageLink link) {
    if (inputLinks.containsKey(link.getId()))
      throw new RuntimeException("the given stage link is already in the input link list");
    inputLinks.put(link.getId(), link);
  }

  public void addOutputLink(RStageLink link) {
    if (outputLinks.containsKey(link.getId()))
      throw new RuntimeException("the given stage link is already in the output link list");
    outputLinks.put(link.getId(), link);
  }
}
