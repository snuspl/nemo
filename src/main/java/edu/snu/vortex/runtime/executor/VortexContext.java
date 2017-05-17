/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.runtime.executor;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@EvaluatorSide
@Unit
public final class VortexContext {

  private static final Logger LOG = Logger.getLogger(VortexContext.class.getName());

  private final MessageChannel messageChannel;
  private final AggregatorReportSender reportSender;
  private final VortexMessageCodec messageCodec;
  private final Map<Integer, TaskAggregator> stageIdToTaskAggregator;
  private final StageInfoRepository stageInfoRepository;
  private final ExecutorService executorService;

  @Inject
  private VortexContext(final MessageChannel messageChannel,
                        final AggregatorReportSender reportSender,
                        final VortexMessageCodec messageCodec,
                        @Parameter(VortexAggregatorConf.NumOfThreads.class) final int numOfThreads,
                        final StageInfoRepository stageInfoRepository) {
    this.messageChannel = messageChannel;
    this.reportSender = reportSender;
    this.messageCodec = messageCodec;
    this.stageIdToTaskAggregator = new ConcurrentHashMap<>();
    this.stageInfoRepository = stageInfoRepository;
    this.executorService = Executors.newFixedThreadPool(numOfThreads);
    messageChannel.registerHandlerForAggregator(new MessageHandler());
  }

  // VortexMessages from VortexMaster
  public final class ContextMessageHandler implements org.apache.reef.evaluator.context.ContextMessageHandler {
    @Override
    public void onNext(final byte[] messageFromDriver) {
      executorService.execute(() -> {
          final VortexMessage vortexMessage = messageCodec.decode(messageFromDriver);
          switch (vortexMessage.getType()) {
          case RegisterStageToAggregator:
            final RegisterStageToAggregator registerStageToAggregator = (RegisterStageToAggregator) vortexMessage;
            final VortexStage vortexStage = registerStageToAggregator.getVortexStage();
            stageInfoRepository.putStageInfo(vortexStage);
            stageIdToTaskAggregator.put(vortexStage.getStageId(),
                new TaskAggregator<>(vortexStage, messageChannel, reportSender));
            reportSender.sendToMaster(new EmptyAck(registerStageToAggregator.getBroadcastId()));
            break;
          case CommitTasks:
            final CommitTasks commitTasks = (CommitTasks) vortexMessage;
            stageIdToTaskAggregator.get(commitTasks.getStageId())
                .commitTasks(commitTasks);
            break;
          case CommitStage:
            final CommitStage commitStage = (CommitStage) vortexMessage;
            stageIdToTaskAggregator.get(commitStage.getStageId())
                .commitStage(commitStage);
            break;
          case FetchStageResult:
            final FetchStageResult fetchStageResult = (FetchStageResult) vortexMessage;
            stageIdToTaskAggregator.get(fetchStageResult.getStageId()).fetchStageResult(fetchStageResult);
            break;
          case RemoveStage:
            final RemoveStage removeStage = (RemoveStage) vortexMessage;
            final int stageId = removeStage.getStageId();
            stageInfoRepository.removeStageInfo(stageId);
            final TaskAggregator removedAggregator = stageIdToTaskAggregator.remove(stageId);
            final boolean removed = removedAggregator != null;
            if (removed) {
              stageInfoRepository.removeStageInfo(stageId);
            }
            LOG.log(Level.INFO, "Remove Stage({0}), result : {1}", new Object[]{stageId, removed});
            reportSender.sendToMaster(new EmptyAck(removeStage.getBroadcastId()));
            break;
          case CreateStage:
            final CreateStage createStage = (CreateStage) vortexMessage;
            final Map createData = new HashMap<>(createStage.getInputList().size());
            createStage.getInputList().forEach(k -> createData.put(k, createStage.getCreator().create(k)));
            stageInfoRepository.putStageInfo(createStage.getStage());
            stageIdToTaskAggregator.put(createStage.getStage().getStageId(),
                new TaskAggregator<>(createData, createStage.getStage(), messageChannel, reportSender));
            reportSender.sendToMaster(new EmptyAck(createStage.getBroadcastId()));
            break;
          case JoinStage:
            final JoinStage joinStage = (JoinStage) vortexMessage;
            final Map joinData = getJoinData(joinStage.getJoin(), joinStage.getStageIds());
            stageInfoRepository.putStageInfo(joinStage.getStage());
            stageIdToTaskAggregator.put(joinStage.getStage().getStageId(),
                new TaskAggregator<>(joinData, joinStage.getStage(), messageChannel, reportSender));
            reportSender.sendToMaster(new EmptyAck(joinStage.getBroadcastId()));
            break;
          default:
            throw new RuntimeException("Unknown message from master to aggregator " + vortexMessage.getType());
          }
        }
      );
    }
  }

  // VortexMessages from VortexExecutors
  public final class MessageHandler implements VortexMessageHandler {
    @Override
    public void onMessage(final VortexMessage vortexMessage, final Identifier identifier) {
      final String executorId = identifier.toString();
      executorService.execute(() -> {
          switch (vortexMessage.getType()) {
          case PushTasksOutput:
            final PushTasksOutput pushTasksOutput = (PushTasksOutput) vortexMessage;
            final TaskAggregator taskAggregator = stageIdToTaskAggregator.get(pushTasksOutput.getStageId());
            taskAggregator.push(executorId, pushTasksOutput);
            break;
          case PullPreviouslyAggregatedData:
            final PullPreviouslyAggregatedData pullPreviouslyAggregatedData =
                (PullPreviouslyAggregatedData) vortexMessage;
            stageIdToTaskAggregator.get(pullPreviouslyAggregatedData.getStageId())
                    .pull(executorId, pullPreviouslyAggregatedData);
            break;
          default:
            throw new RuntimeException("Unknown message from executor to aggregator " + vortexMessage.getType());
          }
        });
    }
  }
}
