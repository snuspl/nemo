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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.IdGenerator;
import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.*;

import java.util.*;

import static edu.snu.vortex.runtime.common.RtAttributes.RtOpLinkAttribute.COMM_PATTERN;

public final class VortexBackend implements Backend {
  private final Set<VirtualStage> vStages;
  private final Set<RtStage> rtStages;
  private final Map<String, String> vStageToRtStage;

  public VortexBackend() {
    this.vStages = new HashSet<>();
    this.rtStages = new HashSet<>();
    this.vStageToRtStage = new HashMap<>();
  }

  private final class VirtualStage {
    private String vStageId;
    private Map<String, Operator> operators;

    public VirtualStage() {
      vStageId = IdGenerator.newVStageId();
      operators = new HashMap<>();
    }

    public String getvStageId() {
      return vStageId;
    }

    public List<Operator> getOperatorList() {
      final List<Operator> operatorList = new ArrayList<>();
      operators.forEach((id, op) -> { operatorList.add(op); });
      return operatorList;
    }

    public void addOperator(Operator operator) {
      operators.put(operator.getId(), operator);
    }

    public void addMultipleOperators(List<Operator> operators) {
      operators.forEach(op -> this.operators.put(op.getId(), op));
    }

    public boolean contains(String operatorId) {
      return operators.containsKey(operatorId);
    }

    public Operator find(String operatorId) {
      return operators.get(operatorId);
    }
  }

  public ExecutionPlan compile(DAG dag) {
    final ExecutionPlan execPlan = new ExecutionPlan();
    final OperatorConverter compiler = new OperatorConverter();
    final List<Operator> operators = dag.getOperators();
    final List<Edge> edges = new ArrayList<>();

    // create initial virtual stages for each operator
    operators.forEach(op -> {
      final VirtualStage vStage = new VirtualStage();
      vStage.addOperator(op);
      vStages.add(vStage);

      if (dag.getInEdgesOf(op).isPresent()) {
        edges.addAll(dag.getInEdgesOf(op).get());
      }
    });

    edges.stream()
        .filter(edge -> isMemChannelType(edge))
        .forEach(edge -> {
          final VirtualStage srcVStage = findVStageOf(edge.getSrc().getId());
          final VirtualStage dstVStage = findVStageOf(edge.getDst().getId());

          if (srcVStage.getvStageId().compareTo(dstVStage.getvStageId()) != 0) {
            srcVStage.addMultipleOperators(dstVStage.getOperatorList());
            vStages.remove(dstVStage);
          }
        });

    vStages.forEach(vStage -> {
      final List<Operator> operatorsInStage = vStage.getOperatorList();
      final Map<RtAttributes.RtStageAttribute, Object> rtStageAttr = new HashMap<>();
      rtStageAttr.put(RtAttributes.RtStageAttribute.PARALLELISM,
          operatorsInStage.get(0).getAttrByKey(Attributes.Key.Parallelism));
      final RtStage rtStage = new RtStage(rtStageAttr);
      operatorsInStage.forEach(op -> rtStage.addRtOp(compiler.convert(op)));

      execPlan.addRtStage(rtStage);
      rtStages.add(rtStage);
      vStageToRtStage.put(vStage.getvStageId(), rtStage.getId());
    });

    edges.stream()
        .filter(edge -> isMemChannelType(edge))
        .forEach(edge -> {
          final VirtualStage vStage = findVStageOf(edge.getSrc().getId());
          final RtStage rtStage = findRtStageById(vStageToRtStage.get(vStage.getvStageId()));
          final String srcRtOperId = compiler.convertId(edge.getSrc().getId());
          final String dstRtOperId = compiler.convertId(edge.getDst().getId());

          final Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = generateRtOpLinkAttributes(edge);
          RtOpLink rtOpLink = new RtOpLink(rtStage.getRtOpById(srcRtOperId),
              rtStage.getRtOpById(dstRtOperId),
              rOpLinkAttr);
          rtStage.connectRtOps(srcRtOperId, dstRtOperId, rtOpLink);
        });

    edges.stream()
        .filter(edge -> !isMemChannelType(edge))
        .forEach(edge -> {
          final String srcOperId = edge.getSrc().getId();
          final String dstOperId = edge.getDst().getId();
          final RtStage srcRtStage = findRtStageById(vStageToRtStage.get(findVStageOf(srcOperId).getvStageId()));
          final RtStage dstRtStage = findRtStageById(vStageToRtStage.get(findVStageOf(dstOperId).getvStageId()));

          final String srcRtOperId = compiler.convertId(srcOperId);
          final String dstRtOperId = compiler.convertId(dstOperId);
          final Map<RtAttributes.RtOpLinkAttribute, Object> rOpLinkAttr = generateRtOpLinkAttributes(edge);
          RtOpLink rtOpLink = new RtOpLink(srcRtStage.getRtOpById(srcRtOperId),
              dstRtStage.getRtOpById(dstRtOperId),
              rOpLinkAttr);
          execPlan.connectRtStages(srcRtStage, dstRtStage, rtOpLink);
        });

    return execPlan;
  }

  public Map<RtAttributes.RtOpLinkAttribute, Object> generateRtOpLinkAttributes(final Edge irEdge) {
    final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkAttributes = new HashMap<>();

    switch (irEdge.getType()) {
      case O2O:
        rtOpLinkAttributes.put(COMM_PATTERN, RtAttributes.CommPattern.ONE_TO_ONE);
        break;
      case O2M:
        rtOpLinkAttributes.put(COMM_PATTERN, RtAttributes.CommPattern.BROADCAST);
        break;
      case M2M:
        rtOpLinkAttributes.put(COMM_PATTERN, RtAttributes.CommPattern.SCATTER_GATHER);
        break;
      default:
        throw new RuntimeException("no such edge type");
    }

    final Map<Attributes.Key, Attributes.Val> irEdgeAttributes = irEdge.getAttributes();
    irEdgeAttributes.forEach((key, val) -> {
      if (key == Attributes.Key.EdgeChannel) {
        switch((Attributes.EdgeChannel) val) {
          case File:
            rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.FILE);
            break;
          case Memory:
            rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.LOCAL_MEM);
            break;
          case TCPPipe:
            rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.TCP);
            break;
          case DistributedStorage:
            rtOpLinkAttributes.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.DISTR_STORAGE);
            break;
        }
      }
    });

    return rtOpLinkAttributes;
  }

  private RtStage findRtStageById(String rtStageId) {
    return rtStages.stream().filter(rtStage -> rtStage.getId().compareTo(rtStageId) == 0).findFirst().get();
  }

  private boolean isMemChannelType(Edge edge) {
    return edge.getAttr(Attributes.Key.EdgeChannel) == Attributes.EdgeChannel.Memory;
  }

  private VirtualStage findVStageOf(String operatorId) {
    Iterator<VirtualStage> iterator = vStages.iterator();
    while (iterator.hasNext()) {
      VirtualStage vStage = iterator.next();
      if (vStage.contains(operatorId))
        return vStage;
    }

    return null;
  }
}

