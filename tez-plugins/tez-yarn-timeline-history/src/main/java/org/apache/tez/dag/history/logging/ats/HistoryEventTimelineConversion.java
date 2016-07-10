/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.history.logging.ats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.web.AMWebController;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.history.events.AMLaunchedEvent;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.history.events.AppLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;
import org.apache.tez.dag.history.events.ContainerStoppedEvent;
import org.apache.tez.dag.history.events.DAGFinishedEvent;
import org.apache.tez.dag.history.events.DAGInitializedEvent;
import org.apache.tez.dag.history.events.DAGRecoveredEvent;
import org.apache.tez.dag.history.events.DAGStartedEvent;
import org.apache.tez.dag.history.events.DAGSubmittedEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.history.events.TaskFinishedEvent;
import org.apache.tez.dag.history.events.TaskStartedEvent;
import org.apache.tez.dag.history.events.VertexFinishedEvent;
import org.apache.tez.dag.history.events.VertexInitializedEvent;
import org.apache.tez.dag.history.events.VertexParallelismUpdatedEvent;
import org.apache.tez.dag.history.events.VertexStartedEvent;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.dag.history.utils.DAGUtils;
import org.apache.tez.dag.records.TezVertexID;

public class HistoryEventTimelineConversion {

  public static TimelineEntity convertToTimelineEntity(HistoryEvent historyEvent) {
    if (!historyEvent.isHistoryEvent()) {
      throw new UnsupportedOperationException("Invalid Event, does not support history"
          + ", eventType=" + historyEvent.getEventType());
    }
    TimelineEntity timelineEntity;
    switch (historyEvent.getEventType()) {
      case APP_LAUNCHED:
        timelineEntity = convertAppLaunchedEvent((AppLaunchedEvent) historyEvent);
        break;
      case AM_LAUNCHED:
        timelineEntity = convertAMLaunchedEvent((AMLaunchedEvent) historyEvent);
        break;
      case AM_STARTED:
        timelineEntity = convertAMStartedEvent((AMStartedEvent) historyEvent);
        break;
      case CONTAINER_LAUNCHED:
        timelineEntity = convertContainerLaunchedEvent((ContainerLaunchedEvent) historyEvent);
        break;
      case CONTAINER_STOPPED:
        timelineEntity = convertContainerStoppedEvent((ContainerStoppedEvent) historyEvent);
        break;
      case DAG_SUBMITTED:
        timelineEntity = convertDAGSubmittedEvent((DAGSubmittedEvent) historyEvent);
        break;
      case DAG_INITIALIZED:
        timelineEntity = convertDAGInitializedEvent((DAGInitializedEvent) historyEvent);
        break;
      case DAG_STARTED:
        timelineEntity = convertDAGStartedEvent((DAGStartedEvent) historyEvent);
        break;
      case DAG_FINISHED:
        timelineEntity = convertDAGFinishedEvent((DAGFinishedEvent) historyEvent);
        break;
      case VERTEX_INITIALIZED:
        timelineEntity = convertVertexInitializedEvent((VertexInitializedEvent) historyEvent);
        break;
      case VERTEX_STARTED:
        timelineEntity = convertVertexStartedEvent((VertexStartedEvent) historyEvent);
        break;
      case VERTEX_FINISHED:
        timelineEntity = convertVertexFinishedEvent((VertexFinishedEvent) historyEvent);
      break;
      case TASK_STARTED:
        timelineEntity = convertTaskStartedEvent((TaskStartedEvent) historyEvent);
        break;
      case TASK_FINISHED:
        timelineEntity = convertTaskFinishedEvent((TaskFinishedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_STARTED:
        timelineEntity = convertTaskAttemptStartedEvent((TaskAttemptStartedEvent) historyEvent);
        break;
      case TASK_ATTEMPT_FINISHED:
        timelineEntity = convertTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) historyEvent);
        break;
      case VERTEX_PARALLELISM_UPDATED:
        timelineEntity = convertVertexParallelismUpdatedEvent(
            (VertexParallelismUpdatedEvent) historyEvent);
        break;
      case DAG_RECOVERED:
        timelineEntity = convertDAGRecoveredEvent(
            (DAGRecoveredEvent) historyEvent);
        break;
      case VERTEX_DATA_MOVEMENT_EVENTS_GENERATED:
      case VERTEX_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_STARTED:
      case VERTEX_GROUP_COMMIT_FINISHED:
      case DAG_COMMIT_STARTED:
        throw new UnsupportedOperationException("Invalid Event, does not support history"
            + ", eventType=" + historyEvent.getEventType());
      default:
        throw new UnsupportedOperationException("Unhandled Event"
            + ", eventType=" + historyEvent.getEventType());
    }
    return timelineEntity;
  }

  private static TimelineEntity convertDAGRecoveredEvent(DAGRecoveredEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent recoverEvt = new TimelineEvent();
    recoverEvt.setEventType(HistoryEventType.DAG_RECOVERED.name());
    recoverEvt.setTimestamp(event.getRecoveredTime());
    recoverEvt.addEventInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    if (event.getRecoveredDagState() != null) {
      recoverEvt.addEventInfo(ATSConstants.DAG_STATE, event.getRecoveredDagState().name());
    }
    if (event.getRecoveryFailureReason() != null) {
      recoverEvt.addEventInfo(ATSConstants.RECOVERY_FAILURE_REASON,
          event.getRecoveryFailureReason());
    }

    atsEntity.addEvent(recoverEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    return atsEntity;
  }

  private static TimelineEntity convertAppLaunchedEvent(AppLaunchedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getApplicationId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_APPLICATION.name());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());

    atsEntity.addOtherInfo(ATSConstants.CONFIG,
        DAGUtils.convertConfigurationToATSMap(event.getConf()));
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ID,
            event.getApplicationId().toString());
    atsEntity.addOtherInfo(ATSConstants.USER, event.getUser());

    atsEntity.setStartTime(event.getLaunchTime());

    if (event.getVersion() != null) {
      atsEntity.addOtherInfo(ATSConstants.TEZ_VERSION,
          DAGUtils.convertTezVersionToATSMap(event.getVersion()));
    }
    atsEntity.addOtherInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);

    return atsEntity;
  }

  private static TimelineEntity convertAMLaunchedEvent(AMLaunchedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getApplicationAttemptId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());

    atsEntity.setStartTime(event.getLaunchTime());

    TimelineEvent launchEvt = new TimelineEvent();
    launchEvt.setEventType(HistoryEventType.AM_LAUNCHED.name());
    launchEvt.setTimestamp(event.getLaunchTime());
    atsEntity.addEvent(launchEvt);

    atsEntity.addOtherInfo(ATSConstants.APP_SUBMIT_TIME, event.getAppSubmitTime());
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());
    atsEntity.addOtherInfo(ATSConstants.USER, event.getUser());

    return atsEntity;
  }

  private static TimelineEntity convertAMStartedEvent(AMStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getApplicationAttemptId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_APPLICATION_ATTEMPT.name());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.AM_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    return atsEntity;
  }

  private static TimelineEntity convertContainerLaunchedEvent(ContainerLaunchedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getContainerId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_CONTAINER_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());

    atsEntity.addOtherInfo(ATSConstants.CONTAINER_ID,
        event.getContainerId().toString());
    atsEntity.setStartTime(event.getLaunchTime());

    TimelineEvent launchEvt = new TimelineEvent();
    launchEvt.setEventType(HistoryEventType.CONTAINER_LAUNCHED.name());
    launchEvt.setTimestamp(event.getLaunchTime());
    atsEntity.addEvent(launchEvt);

    return atsEntity;
  }

  private static TimelineEntity convertContainerStoppedEvent(ContainerStoppedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId("tez_"
        + event.getContainerId().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_CONTAINER_ID.name());

    // In case, a container is stopped in a different attempt
    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());

    TimelineEvent stoppedEvt = new TimelineEvent();
    stoppedEvt.setEventType(HistoryEventType.CONTAINER_STOPPED.name());
    stoppedEvt.setTimestamp(event.getStoppedTime());
    atsEntity.addEvent(stoppedEvt);

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(ATSConstants.EXIT_STATUS, event.getExitStatus());

    atsEntity.addOtherInfo(ATSConstants.EXIT_STATUS, event.getExitStatus());
    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getStoppedTime());

    return atsEntity;
  }

  private static TimelineEntity convertDAGFinishedEvent(DAGFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.DAG_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getDagID().getApplicationId().toString());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());
    atsEntity.addPrimaryFilter(ATSConstants.STATUS, event.getState().name());
    if (event.getDAGPlan().hasCallerContext()
        && event.getDAGPlan().getCallerContext().hasCallerId()) {
      atsEntity.addPrimaryFilter(ATSConstants.CALLER_CONTEXT_ID,
          event.getDAGPlan().getCallerContext().getCallerId());
    }

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());
    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    atsEntity.addOtherInfo(ATSConstants.COMPLETION_APPLICATION_ATTEMPT_ID,
        event.getApplicationAttemptId().toString());

    final Map<String, Integer> dagTaskStats = event.getDagTaskStats();
    if (dagTaskStats != null) {
      for(Entry<String, Integer> entry : dagTaskStats.entrySet()) {
        atsEntity.addOtherInfo(entry.getKey(), entry.getValue());
      }
    }

    return atsEntity;
  }

  private static TimelineEntity convertDAGInitializedEvent(DAGInitializedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent initEvt = new TimelineEvent();
    initEvt.setEventType(HistoryEventType.DAG_INITIALIZED.name());
    initEvt.setTimestamp(event.getInitTime());
    atsEntity.addEvent(initEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getDagID().getApplicationId().toString());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    atsEntity.addOtherInfo(ATSConstants.INIT_TIME, event.getInitTime());

    if (event.getVertexNameIDMap() != null) {
      Map<String, String> nameIdStrMap = new TreeMap<String, String>();
      for (Entry<String, TezVertexID> entry : event.getVertexNameIDMap().entrySet()) {
        nameIdStrMap.put(entry.getKey(), entry.getValue().toString());
      }
      atsEntity.addOtherInfo(ATSConstants.VERTEX_NAME_ID_MAPPING, nameIdStrMap);
    }

    return atsEntity;
  }

  private static TimelineEntity convertDAGStartedEvent(DAGStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.DAG_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getDagID().getApplicationId().toString());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDagName());

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getDagState().toString());

    return atsEntity;
  }

  private static TimelineEntity convertDAGSubmittedEvent(DAGSubmittedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getDagID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_DAG_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION.name(),
        "tez_" + event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addRelatedEntity(EntityTypes.TEZ_APPLICATION_ATTEMPT.name(),
        "tez_" + event.getApplicationAttemptId().toString());

    TimelineEvent submitEvt = new TimelineEvent();
    submitEvt.setEventType(HistoryEventType.DAG_SUBMITTED.name());
    submitEvt.setTimestamp(event.getSubmitTime());
    atsEntity.addEvent(submitEvt);

    atsEntity.setStartTime(event.getSubmitTime());

    atsEntity.addPrimaryFilter(ATSConstants.USER, event.getUser());
    atsEntity.addPrimaryFilter(ATSConstants.DAG_NAME, event.getDAGName());
    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getDagID().getApplicationId().toString());

    if (event.getDAGPlan().hasCallerContext()
        && event.getDAGPlan().getCallerContext().hasCallerId()) {
      atsEntity.addPrimaryFilter(ATSConstants.CALLER_CONTEXT_ID,
          event.getDAGPlan().getCallerContext().getCallerId());
    }

    try {
      atsEntity.addOtherInfo(ATSConstants.DAG_PLAN,
          DAGUtils.convertDAGPlanToATSMap(event.getDAGPlan()));
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ID,
        event.getApplicationAttemptId().getApplicationId().toString());
    atsEntity.addOtherInfo(ATSConstants.APPLICATION_ATTEMPT_ID,
            event.getApplicationAttemptId().toString());
    atsEntity.addOtherInfo(ATSConstants.USER, event.getUser());
    atsEntity.addOtherInfo(ATSConstants.DAG_AM_WEB_SERVICE_VERSION, AMWebController.VERSION);
    if (event.getDAGPlan().hasCallerContext()
        && event.getDAGPlan().getCallerContext().hasCallerId()
        && event.getDAGPlan().getCallerContext().hasCallerType()) {
      atsEntity.addOtherInfo(ATSConstants.CALLER_CONTEXT_ID,
          event.getDAGPlan().getCallerContext().getCallerId());
      atsEntity.addOtherInfo(ATSConstants.CALLER_CONTEXT_TYPE,
          event.getDAGPlan().getCallerContext().getCallerType());
    }

    return atsEntity;
  }

  private static TimelineEntity convertTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskAttemptID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.TASK_ATTEMPT_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.CREATION_TIME, event.getCreationTime());
    atsEntity.addOtherInfo(ATSConstants.ALLOCATION_TIME, event.getAllocationTime());
    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    if (event.getCreationCausalTA() != null) {
      atsEntity.addOtherInfo(ATSConstants.CREATION_CAUSAL_ATTEMPT,
          event.getCreationCausalTA().toString());
    }
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());
    if (event.getTaskAttemptError() != null) {
      atsEntity.addOtherInfo(ATSConstants.TASK_ATTEMPT_ERROR_ENUM, event.getTaskAttemptError().name());
    }
    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getCounters()));
    if (event.getDataEvents() != null && !event.getDataEvents().isEmpty()) {
      atsEntity.addOtherInfo(ATSConstants.LAST_DATA_EVENTS, 
          DAGUtils.convertDataEventDependecyInfoToATS(event.getDataEvents()));
    }
    return atsEntity;
  }

  private static TimelineEntity convertTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskAttemptID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ATTEMPT_ID.name());

    atsEntity.setStartTime(event.getStartTime());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskAttemptID().getTaskID().getVertexID().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_TASK_ID.name(),
        event.getTaskAttemptID().getTaskID().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.TASK_ATTEMPT_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.IN_PROGRESS_LOGS_URL, event.getInProgressLogsUrl());
    atsEntity.addOtherInfo(ATSConstants.COMPLETED_LOGS_URL, event.getCompletedLogsUrl());
    atsEntity.addOtherInfo(ATSConstants.NODE_ID, event.getNodeId().toString());
    atsEntity.addOtherInfo(ATSConstants.NODE_HTTP_ADDRESS, event.getNodeHttpAddress());
    atsEntity.addOtherInfo(ATSConstants.CONTAINER_ID, event.getContainerId().toString());
    atsEntity.addOtherInfo(ATSConstants.STATUS, TaskAttemptState.RUNNING.name());

    return atsEntity;
  }

  private static TimelineEntity convertTaskFinishedEvent(TaskFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ID.name());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getTaskID().getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.TASK_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());
    atsEntity.addOtherInfo(ATSConstants.NUM_FAILED_TASKS_ATTEMPTS, event.getNumFailedAttempts());
    if (event.getSuccessfulAttemptID() != null) {
      atsEntity.addOtherInfo(ATSConstants.SUCCESSFUL_ATTEMPT_ID,
          event.getSuccessfulAttemptID().toString());
    }

    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));

    return atsEntity;
  }

  private static TimelineEntity convertTaskStartedEvent(TaskStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getTaskID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_TASK_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getTaskID().getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getTaskID().getVertexID().getDAGId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_VERTEX_ID.name(),
        event.getTaskID().getVertexID().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.TASK_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.setStartTime(event.getStartTime());

    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.SCHEDULED_TIME, event.getScheduledTime());
    atsEntity.addOtherInfo(ATSConstants.STATUS, TaskState.SCHEDULED.name());

    return atsEntity;
  }

  private static TimelineEntity convertVertexFinishedEvent(VertexFinishedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent finishEvt = new TimelineEvent();
    finishEvt.setEventType(HistoryEventType.VERTEX_FINISHED.name());
    finishEvt.setTimestamp(event.getFinishTime());
    atsEntity.addEvent(finishEvt);

    atsEntity.addPrimaryFilter(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.FINISH_TIME, event.getFinishTime());
    atsEntity.addOtherInfo(ATSConstants.TIME_TAKEN, (event.getFinishTime() - event.getStartTime()));
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getState().name());

    atsEntity.addOtherInfo(ATSConstants.DIAGNOSTICS, event.getDiagnostics());
    atsEntity.addOtherInfo(ATSConstants.COUNTERS,
        DAGUtils.convertCountersToATSMap(event.getTezCounters()));
    atsEntity.addOtherInfo(ATSConstants.STATS,
        DAGUtils.convertVertexStatsToATSMap(event.getVertexStats()));

    final Map<String, Integer> vertexTaskStats = event.getVertexTaskStats();
    if (vertexTaskStats != null) {
      for(Entry<String, Integer> entry : vertexTaskStats.entrySet()) {
        atsEntity.addOtherInfo(entry.getKey(), entry.getValue());
      }
    }

    return atsEntity;
  }

  private static TimelineEntity convertVertexInitializedEvent(VertexInitializedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addRelatedEntity(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent initEvt = new TimelineEvent();
    initEvt.setEventType(HistoryEventType.VERTEX_INITIALIZED.name());
    initEvt.setTimestamp(event.getInitedTime());
    atsEntity.addEvent(initEvt);

    atsEntity.setStartTime(event.getInitedTime());

    atsEntity.addOtherInfo(ATSConstants.VERTEX_NAME, event.getVertexName());
    atsEntity.addOtherInfo(ATSConstants.INIT_REQUESTED_TIME, event.getInitRequestedTime());
    atsEntity.addOtherInfo(ATSConstants.INIT_TIME, event.getInitedTime());
    atsEntity.addOtherInfo(ATSConstants.NUM_TASKS, event.getNumTasks());
    atsEntity.addOtherInfo(ATSConstants.PROCESSOR_CLASS_NAME, event.getProcessorName());

    return atsEntity;
  }

  private static TimelineEntity convertVertexStartedEvent(VertexStartedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent startEvt = new TimelineEvent();
    startEvt.setEventType(HistoryEventType.VERTEX_STARTED.name());
    startEvt.setTimestamp(event.getStartTime());
    atsEntity.addEvent(startEvt);

    atsEntity.addOtherInfo(ATSConstants.START_REQUESTED_TIME, event.getStartRequestedTime());
    atsEntity.addOtherInfo(ATSConstants.START_TIME, event.getStartTime());
    atsEntity.addOtherInfo(ATSConstants.STATUS, event.getVertexState().toString());

    return atsEntity;
  }

  private static TimelineEntity convertVertexParallelismUpdatedEvent(
      VertexParallelismUpdatedEvent event) {
    TimelineEntity atsEntity = new TimelineEntity();
    atsEntity.setEntityId(event.getVertexID().toString());
    atsEntity.setEntityType(EntityTypes.TEZ_VERTEX_ID.name());

    atsEntity.addPrimaryFilter(ATSConstants.APPLICATION_ID,
        event.getVertexID().getDAGId().getApplicationId().toString());
    atsEntity.addPrimaryFilter(EntityTypes.TEZ_DAG_ID.name(),
        event.getVertexID().getDAGId().toString());

    TimelineEvent updateEvt = new TimelineEvent();
    updateEvt.setEventType(HistoryEventType.VERTEX_PARALLELISM_UPDATED.name());
    updateEvt.setTimestamp(event.getUpdateTime());

    Map<String,Object> eventInfo = new HashMap<String, Object>();
    if (event.getSourceEdgeProperties() != null && !event.getSourceEdgeProperties().isEmpty()) {
      Map<String, Object> updatedEdgeManagers = new HashMap<String, Object>();
      for (Entry<String, EdgeProperty> entry :
          event.getSourceEdgeProperties().entrySet()) {
        updatedEdgeManagers.put(entry.getKey(),
            DAGUtils.convertEdgeProperty(entry.getValue()));
      }
      eventInfo.put(ATSConstants.UPDATED_EDGE_MANAGERS, updatedEdgeManagers);
    }
    eventInfo.put(ATSConstants.NUM_TASKS, event.getNumTasks());
    eventInfo.put(ATSConstants.OLD_NUM_TASKS, event.getOldNumTasks());
    updateEvt.setEventInfo(eventInfo);
    atsEntity.addEvent(updateEvt);

    atsEntity.addOtherInfo(ATSConstants.NUM_TASKS, event.getNumTasks());

    return atsEntity;
  }

}
