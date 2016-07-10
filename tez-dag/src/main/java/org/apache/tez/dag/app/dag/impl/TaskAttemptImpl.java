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

package org.apache.tez.dag.app.dag.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.oldrecords.TaskAttemptReport;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.TaskAttemptStateInternal;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventCounterUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.DiagnosableEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventAttemptFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventDiagnosticsUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventTerminationCauseEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventOutputFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventSchedule;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStartedRemotely;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventStatusUpdate;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEventTAUpdate;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.dag.app.dag.event.SpeculatorEventTaskAttemptStatusUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventTAEnded;
import org.apache.tez.dag.app.rm.AMSchedulerEventTALaunchRequest;
import org.apache.tez.dag.app.rm.container.AMContainer;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.events.TaskAttemptFinishedEvent;
import org.apache.tez.dag.history.events.TaskAttemptStartedEvent;
import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.DataEventDependencyInfoProto;
import org.apache.tez.dag.utils.TezBuilderUtils;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.TaskStatusUpdateEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TaskStatistics;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TaskAttemptImpl implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {

  // TODO Ensure MAPREDUCE-4457 is factored in. Also MAPREDUCE-4068.
  // TODO Consider TAL registration in the TaskAttempt instead of the container.

  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptImpl.class);
  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");
  
  public static class DataEventDependencyInfo {
    long timestamp;
    TezTaskAttemptID taId;
    public DataEventDependencyInfo(long time, TezTaskAttemptID id) {
      this.timestamp = time;
      this.taId = id;
    }
    public long getTimestamp() {
      return timestamp;
    }
    public TezTaskAttemptID getTaskAttemptId() {
      return taId;
    }
    public static DataEventDependencyInfoProto toProto(DataEventDependencyInfo info) {
      DataEventDependencyInfoProto.Builder builder = DataEventDependencyInfoProto.newBuilder();
      builder.setTimestamp(info.timestamp);
      if (info.taId != null) {
        builder.setTaskAttemptId(info.taId.toString());
      }
      return builder.build();
    }
    
    public static DataEventDependencyInfo fromProto(DataEventDependencyInfoProto proto) {
      TezTaskAttemptID taId = null;
      if(proto.hasTaskAttemptId()) {
        taId = TezTaskAttemptID.fromString(proto.getTaskAttemptId());
      }
      return new DataEventDependencyInfo(proto.getTimestamp(), taId);
    }
  }

  static final TezCounters EMPTY_COUNTERS = new TezCounters();

  protected final Configuration conf;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  private final TezTaskAttemptID attemptId;
  private final Clock clock;
  private TaskAttemptTerminationCause terminationCause = TaskAttemptTerminationCause.UNKNOWN_ERROR;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  protected final AppContext appContext;
  private final TaskHeartbeatHandler taskHeartbeatHandler;
  private long launchTime = 0;
  private long finishTime = 0;
  private String trackerName;
  private int httpPort;

  // TODO Can these be replaced by the container object TEZ-1037
  private Container container;
  private long allocationTime;
  private ContainerId containerId;
  private NodeId containerNodeId;
  private String nodeHttpAddress;
  private String nodeRackName;
  
  private final Task task;
  private final Vertex vertex;

  @VisibleForTesting
  boolean appendNextDataEvent = true;
  ArrayList<DataEventDependencyInfo> lastDataEvents = Lists.newArrayList();
  
  @VisibleForTesting
  TaskAttemptStatus reportedStatus;
  private DAGCounter localityCounter;
  
  org.apache.tez.runtime.api.impl.TaskStatistics statistics;

  // Used to store locality information when
  Set<String> taskHosts = new HashSet<String>();
  Set<String> taskRacks = new HashSet<String>();

  private Map<TezTaskAttemptID, Long> uniquefailedOutputReports = Maps.newHashMap();
  private static double MAX_ALLOWED_OUTPUT_FAILURES_FRACTION;
  private static int MAX_ALLOWED_OUTPUT_FAILURES;
  private static int MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC;

  protected final boolean isRescheduled;
  private final Resource taskResource;
  private final ContainerContext containerContext;
  private final boolean leafVertex;
  
  private TezTaskAttemptID creationCausalTA;
  private long creationTime;

  protected static final FailedTransitionHelper FAILED_HELPER =
      new FailedTransitionHelper();

  protected static final KilledTransitionHelper KILLED_HELPER =
      new KilledTransitionHelper();

  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION =
          new DiagnosticInformationUpdater();
  
  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      TERMINATED_AFTER_SUCCESS_HELPER = new TerminatedAfterSuccessHelper(KILLED_HELPER);

  private static SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent>
      STATUS_UPDATER = new StatusUpdaterTransition();

  private final StateMachine<TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent> stateMachine;

  private static StateMachineFactory
  <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
  stateMachineFactory
            = new StateMachineFactory
            <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
            (TaskAttemptStateInternal.NEW)

      .addTransition(TaskAttemptStateInternal.NEW,
          EnumSet.of(TaskAttemptStateInternal.START_WAIT, TaskAttemptStateInternal.FAILED),
          TaskAttemptEventType.TA_SCHEDULE, new ScheduleTaskattemptTransition())
      .addTransition(TaskAttemptStateInternal.NEW,
          TaskAttemptStateInternal.NEW,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(TaskAttemptStateInternal.NEW,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminateTransition(KILLED_HELPER))

      .addTransition(TaskAttemptStateInternal.NEW,
          EnumSet.of(TaskAttemptStateInternal.NEW,
              TaskAttemptStateInternal.RUNNING,
              TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.FAILED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_RECOVER, new RecoverTransition())

      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_STARTED_REMOTELY, new StartedTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.START_WAIT,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedBeforeRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_NODE_FAILED,
          new NodeFailedBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_CONTAINER_TERMINATING,
          new ContainerTerminatingBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedBeforeRunningTransition())
      .addTransition(TaskAttemptStateInternal.START_WAIT,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
          new ContainerCompletedBeforeRunningTransition(KILLED_HELPER))

      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_STATUS_UPDATE, STATUS_UPDATER)
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.RUNNING,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      // Optional, may not come in for all tasks.
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.SUCCEEDED, TaskAttemptEventType.TA_DONE,
          new SucceededTransition())
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_FAILED,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_TIMED_OUT,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_NODE_FAILED,
          new TerminatedWhileRunningTransition(KILLED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_CONTAINER_TERMINATING,
          new TerminatedWhileRunningTransition(FAILED_HELPER))
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileRunningTransition())
      .addTransition(TaskAttemptStateInternal.RUNNING,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
          new ContainerCompletedWhileRunningTransition(KILLED_HELPER))
      .addTransition(
          TaskAttemptStateInternal.RUNNING,
          EnumSet.of(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
              TaskAttemptStateInternal.RUNNING),
          TaskAttemptEventType.TA_OUTPUT_FAILED,
          new OutputReportedFailedTransition())

      .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileTerminating())
      .addTransition(TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          TaskAttemptStateInternal.KILL_IN_PROGRESS,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_CONTAINER_TERMINATED,
          new ContainerCompletedWhileTerminating())
      .addTransition(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(TaskAttemptStateInternal.KILLED,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(
          TaskAttemptStateInternal.KILLED,
          TaskAttemptStateInternal.KILLED,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      .addTransition(TaskAttemptStateInternal.FAILED,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(
          TaskAttemptStateInternal.FAILED,
          TaskAttemptStateInternal.FAILED,
          EnumSet.of(TaskAttemptEventType.TA_STARTED_REMOTELY,
              TaskAttemptEventType.TA_SCHEDULE,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM,
              TaskAttemptEventType.TA_STATUS_UPDATE,
              TaskAttemptEventType.TA_DONE, TaskAttemptEventType.TA_FAILED,
              TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_KILL_REQUEST,
              TaskAttemptEventType.TA_NODE_FAILED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_OUTPUT_FAILED))

      // How will duplicate history events be handled ?
      // TODO Maybe consider not failing REDUCE tasks in this case. Also,
      // MAP_TASKS in case there's only one phase in the job.
      .addTransition(TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
          DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_KILL_REQUEST,
          new TerminatedAfterSuccessTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.KILLED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_NODE_FAILED,
          new TerminatedAfterSuccessTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptStateInternal.FAILED,
              TaskAttemptStateInternal.SUCCEEDED),
          TaskAttemptEventType.TA_OUTPUT_FAILED,
          new OutputReportedFailedTransition())
      .addTransition(
          TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptStateInternal.SUCCEEDED,
          EnumSet.of(TaskAttemptEventType.TA_TIMED_OUT,
              TaskAttemptEventType.TA_CONTAINER_TERMINATING,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED,
              TaskAttemptEventType.TA_CONTAINER_TERMINATED_BY_SYSTEM))

        .installTopology();

  private TaskAttemptState recoveredState = TaskAttemptState.NEW;
  private boolean recoveryStartEventSeen = false;

  @SuppressWarnings("rawtypes")
  public TaskAttemptImpl(TezTaskID taskId, int attemptNumber, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener, Configuration conf, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
      boolean isRescheduled,
      Resource resource, ContainerContext containerContext, boolean leafVertex,
      Task task) {
    this(taskId, attemptNumber, eventHandler, taskAttemptListener, conf, clock,
        taskHeartbeatHandler, appContext, isRescheduled, resource, containerContext, leafVertex,
        task, null);
  }
  public TaskAttemptImpl(TezTaskID taskId, int attemptNumber, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener, Configuration conf, Clock clock,
      TaskHeartbeatHandler taskHeartbeatHandler, AppContext appContext,
      boolean isRescheduled,
      Resource resource, ContainerContext containerContext, boolean leafVertex,
      Task task, TezTaskAttemptID schedulingCausalTA) {

    MAX_ALLOWED_OUTPUT_FAILURES = conf.getInt(TezConfiguration
        .TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES, TezConfiguration
        .TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES_DEFAULT);

    MAX_ALLOWED_OUTPUT_FAILURES_FRACTION = conf.getDouble(TezConfiguration
        .TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES_FRACTION, TezConfiguration
        .TEZ_TASK_MAX_ALLOWED_OUTPUT_FAILURES_FRACTION_DEFAULT);
    
    MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC = conf.getInt(
        TezConfiguration.TEZ_AM_MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC,
        TezConfiguration.TEZ_AM_MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC_DEFAULT);
    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.attemptId = TezBuilderUtils.newTaskAttemptId(taskId, attemptNumber);
    this.eventHandler = eventHandler;
    //Reported status
    this.conf = conf;
    this.clock = clock;
    this.taskHeartbeatHandler = taskHeartbeatHandler;
    this.appContext = appContext;
    this.task = task;
    this.vertex = this.task.getVertex();
    this.creationCausalTA = schedulingCausalTA;
    this.creationTime = clock.getTime();

    this.reportedStatus = new TaskAttemptStatus(this.attemptId);
    initTaskAttemptStatus(reportedStatus);
    RackResolver.init(conf);
    this.stateMachine = stateMachineFactory.make(this);
    this.isRescheduled = isRescheduled;
    this.taskResource = resource;
    this.containerContext = containerContext;
    this.leafVertex = leafVertex;
  }


  @Override
  public TezTaskAttemptID getID() {
    return attemptId;
  }

  @Override
  public TezTaskID getTaskID() {
    return attemptId.getTaskID();
  }

  @Override
  public TezVertexID getVertexID() {
    return attemptId.getTaskID().getVertexID();
  }

  @Override
  public TezDAGID getDAGID() {
    return getVertexID().getDAGId();
  }
  
  public TezTaskAttemptID getSchedulingCausalTA() {
    return creationCausalTA;
  }

  TaskSpec createRemoteTaskSpec() throws AMUserCodeException {
    TaskSpec baseTaskSpec = task.getBaseTaskSpec();
    if (baseTaskSpec == null) {
      // since recovery does not follow normal transitions, TaskEventScheduleTask
      // is not being honored by the recovery code path. Using this to workaround 
      // until recovery is fixed. Calling the non-locking internal method of the vertex
      // to get the taskSpec directly. Since everything happens on the central dispatcher 
      // during recovery this is deadlock free for now. TEZ-1019 should remove the need for this.
      baseTaskSpec = ((VertexImpl) vertex).createRemoteTaskSpec(getID().getTaskID().getId());
    }
    return new TaskSpec(getID(),
        baseTaskSpec.getDAGName(), baseTaskSpec.getVertexName(),
        baseTaskSpec.getVertexParallelism(), baseTaskSpec.getProcessorDescriptor(),
        baseTaskSpec.getInputs(), baseTaskSpec.getOutputs(), baseTaskSpec.getGroupInputs());
  }

  @Override
  public TaskAttemptReport getReport() {
    TaskAttemptReport result = Records.newRecord(TaskAttemptReport.class);
    readLock.lock();
    try {
      result.setTaskAttemptId(attemptId);
      //take the LOCAL state of attempt
      //DO NOT take from reportedStatus

      result.setTaskAttemptState(getState());
      result.setProgress(reportedStatus.progress);
      result.setStartTime(launchTime);
      result.setFinishTime(finishTime);
      //result.setShuffleFinishTime(this.reportedStatus.shuffleFinishTime);
      result.setDiagnosticInfo(StringUtils.join(getDiagnostics(), LINE_SEPARATOR));
      //result.setPhase(reportedStatus.phase);
      //result.setStateString(reportedStatus.statef);
      result.setCounters(getCounters());
      result.setContainerId(this.getAssignedContainerID());
      result.setNodeManagerHost(trackerName);
      result.setNodeManagerHttpPort(httpPort);
      if (this.containerNodeId != null) {
        result.setNodeManagerPort(this.containerNodeId.getPort());
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    List<String> result = new ArrayList<String>();
    readLock.lock();
    try {
      result.addAll(diagnostics);
      return result;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public TaskAttemptTerminationCause getTerminationCause() {
    return terminationCause;
  }

  @Override
  public TezCounters getCounters() {
    readLock.lock();
    try {
      reportedStatus.setLocalityCounter(this.localityCounter);
      TezCounters counters = reportedStatus.counters;
      if (counters == null) {
        counters = EMPTY_COUNTERS;
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }
  
  TaskStatistics getStatistics() {
    return this.statistics;
  }

  @Override
  public float getProgress() {
    readLock.lock();
    try {
      return reportedStatus.progress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return getStateNoLock();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getStateNoLock() {
    return getExternalState(stateMachine.getCurrentState());
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
      return (EnumSet.of(TaskAttemptStateInternal.SUCCEEDED,
          TaskAttemptStateInternal.FAILED,
          TaskAttemptStateInternal.FAIL_IN_PROGRESS,
          TaskAttemptStateInternal.KILLED,
          TaskAttemptStateInternal.KILL_IN_PROGRESS)
          .contains(getInternalState()));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return containerId;
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public Container getAssignedContainer() {
    readLock.lock();
    try {
      return container;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    readLock.lock();
    try {
      return containerNodeId.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public NodeId getNodeId() {
    readLock.lock();
    try {
      return containerNodeId;
    } finally {
      readLock.unlock();
    }
  }

  /**If container Assigned then return the node's address, otherwise null.
   */
  @Override
  public String getNodeHttpAddress() {
    readLock.lock();
    try {
      return nodeHttpAddress;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * If container Assigned then return the node's rackname, otherwise null.
   */
  @Override
  public String getNodeRackName() {
    this.readLock.lock();
    try {
      return this.nodeRackName;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getLaunchTime() {
    readLock.lock();
    try {
      return launchTime;
    } finally {
      readLock.unlock();
    }
  }

  public long getCreationTime() {
    readLock.lock();
    try {
      return creationTime;
    } finally {
      readLock.unlock();
    }
  }
  
  public TezTaskAttemptID getCreationCausalAttempt() {
    readLock.lock();
    try {
      return creationCausalTA;
    } finally {
      readLock.unlock();
    }
  }

  public long getAllocationTime() {
    readLock.lock();
    try {
      return allocationTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }
  
  Vertex getVertex() {
    return vertex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing TaskAttemptEvent " + event.getTaskAttemptID()
          + " of type " + event.getType() + " while in state "
          + getInternalState() + ". Event: " + event);
    }
    writeLock.lock();
    try {
      final TaskAttemptStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new DAGEventDiagnosticsUpdate(
            this.attemptId.getTaskID().getVertexID().getDAGId(),
            "Invalid event " + event.getType() +
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(
            new DAGEvent(
                this.attemptId.getTaskID().getVertexID().getDAGId(),
                DAGEventType.INTERNAL_ERROR)
            );
      } catch (RuntimeException e) {
        LOG.error("Uncaught exception when handling event " + event.getType()
            + " at current state " + oldState + " for "
            + this.attemptId, e);
        eventHandler.handle(new DAGEventDiagnosticsUpdate(
            this.attemptId.getTaskID().getVertexID().getDAGId(),
            "Uncaught exception when handling event " + event.getType()
                + " on TaskAttempt " + this.attemptId
                + " at state " + oldState + ", error=" + e.getMessage()));
        eventHandler.handle(
            new DAGEvent(
                this.attemptId.getTaskID().getVertexID().getDAGId(),
                DAGEventType.INTERNAL_ERROR)
        );
      }
      if (oldState != getInternalState()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(attemptId + " TaskAttempt Transitioned from "
              + oldState + " to "
              + getInternalState() + " due to event "
              + event.getType());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @VisibleForTesting
  public TaskAttemptStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static TaskAttemptState getExternalState(
      TaskAttemptStateInternal smState) {
    switch (smState) {
    case NEW:
    case START_WAIT:
      return TaskAttemptState.STARTING;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case FAILED:
    case FAIL_IN_PROGRESS:
      return TaskAttemptState.FAILED;
    case KILLED:
    case KILL_IN_PROGRESS:
      return TaskAttemptState.KILLED;
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    default:
      throw new TezUncheckedException("Attempt to convert invalid "
          + "stateMachineTaskAttemptState to externalTaskAttemptState: "
          + smState);
    }
  }

  @Override
  public TaskAttemptState restoreFromEvent(HistoryEvent historyEvent) {
    writeLock.lock();
    try {
      switch (historyEvent.getEventType()) {
        case TASK_ATTEMPT_STARTED:
        {
          TaskAttemptStartedEvent tEvent = (TaskAttemptStartedEvent) historyEvent;
          this.launchTime = tEvent.getStartTime();
          recoveryStartEventSeen = true;
          recoveredState = TaskAttemptState.RUNNING;
          this.containerId = tEvent.getContainerId();
          sendEvent(createDAGCounterUpdateEventTALaunched(this));
          return recoveredState;
        }
        case TASK_ATTEMPT_FINISHED:
        {
          TaskAttemptFinishedEvent tEvent = (TaskAttemptFinishedEvent) historyEvent;
          this.creationTime = tEvent.getCreationTime();
          this.allocationTime = tEvent.getAllocationTime();
          this.launchTime = tEvent.getStartTime();
          this.finishTime = tEvent.getFinishTime();
          this.creationCausalTA = tEvent.getCreationCausalTA();
          this.reportedStatus.counters = tEvent.getCounters();
          this.reportedStatus.progress = 1f;
          this.reportedStatus.state = tEvent.getState();
          this.terminationCause = tEvent.getTaskAttemptError() != null ? tEvent.getTaskAttemptError()
              : TaskAttemptTerminationCause.UNKNOWN_ERROR;
          this.diagnostics.add(tEvent.getDiagnostics());
          this.recoveredState = tEvent.getState();
          if (tEvent.getDataEvents() != null) {
            this.lastDataEvents.addAll(tEvent.getDataEvents());
          }
          sendEvent(createDAGCounterUpdateEventTAFinished(this, tEvent.getState()));
          return recoveredState;
        }
        default:
          throw new RuntimeException("Unexpected event received for restoring"
              + " state, eventType=" + historyEvent.getEventType());
  
      }
    } finally {
      writeLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    this.eventHandler.handle(event);
  }

  // always called in write lock
  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0 && finishTime == 0) {
      finishTime = clock.getTime();
    }
  }

  // TOOD Merge some of these JobCounter events.
  private static DAGEventCounterUpdate createDAGCounterUpdateEventTALaunched(
      TaskAttemptImpl ta) {
    DAGEventCounterUpdate dagCounterEvent =
        new DAGEventCounterUpdate(
            ta.getDAGID()
            );
    dagCounterEvent.addCounterUpdate(DAGCounter.TOTAL_LAUNCHED_TASKS, 1);
    return dagCounterEvent;
  }

  private static DAGEventCounterUpdate createDAGCounterUpdateEventTAFinished(
      TaskAttemptImpl taskAttempt, TaskAttemptState taState) {
    DAGEventCounterUpdate jce =
        new DAGEventCounterUpdate(taskAttempt.getDAGID());

    if (taState == TaskAttemptState.FAILED) {
      jce.addCounterUpdate(DAGCounter.NUM_FAILED_TASKS, 1);
    } else if (taState == TaskAttemptState.KILLED) {
      jce.addCounterUpdate(DAGCounter.NUM_KILLED_TASKS, 1);
    } else if (taState == TaskAttemptState.SUCCEEDED ) {
      jce.addCounterUpdate(DAGCounter.NUM_SUCCEEDED_TASKS, 1);
    }

    return jce;
  }

//  private static long computeSlotMillis(TaskAttemptImpl taskAttempt) {
//    int slotMemoryReq =
//        taskAttempt.taskResource.getMemory();
//
//    int minSlotMemSize =
//        taskAttempt.appContext.getClusterInfo().getMinContainerCapability()
//            .getMemory();
//
//    int simSlotsRequired =
//        minSlotMemSize == 0 ? 0 : (int) Math.ceil((float) slotMemoryReq
//            / minSlotMemSize);
//
//    long slotMillisIncrement =
//        simSlotsRequired
//            * (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
//    return slotMillisIncrement;
//  }

  // TODO: JobHistory
  // TODO Change to return a JobHistoryEvent.
  /*
  private static
      TaskAttemptUnsuccessfulCompletionEvent
  createTaskAttemptUnsuccessfulCompletionEvent(TaskAttemptImpl taskAttempt,
      TaskAttemptStateInternal attemptState) {
    TaskAttemptUnsuccessfulCompletionEvent tauce =
    new TaskAttemptUnsuccessfulCompletionEvent(
        TypeConverter.fromYarn(taskAttempt.attemptId),
        TypeConverter.fromYarn(taskAttempt.attemptId.getTaskId()
            .getTaskType()), attemptState.toString(),
        taskAttempt.finishTime,
        taskAttempt.containerNodeId == null ? "UNKNOWN"
            : taskAttempt.containerNodeId.getHost(),
        taskAttempt.containerNodeId == null ? -1
            : taskAttempt.containerNodeId.getPort(),
        taskAttempt.nodeRackName == null ? "UNKNOWN"
            : taskAttempt.nodeRackName,
        StringUtils.join(
            taskAttempt.getDiagnostics(), LINE_SEPARATOR), taskAttempt
            .getProgressSplitBlock().burst());
    return tauce;
  }

  // TODO Incorporate MAPREDUCE-4838
  private JobHistoryEvent createTaskAttemptStartedEvent() {
    TaskAttemptStartedEvent tase = new TaskAttemptStartedEvent(
        TypeConverter.fromYarn(attemptId), TypeConverter.fromYarn(taskId
            .getTaskType()), launchTime, trackerName, httpPort, shufflePort,
        containerId, "", "");
    return new JobHistoryEvent(jobId, tase);

  }
  */

//  private WrappedProgressSplitsBlock getProgressSplitBlock() {
//    return null;
//    // TODO
//    /*
//    readLock.lock();
//    try {
//      if (progressSplitBlock == null) {
//        progressSplitBlock = new WrappedProgressSplitsBlock(conf.getInt(
//            MRJobConfig.MR_AM_NUM_PROGRESS_SPLITS,
//            MRJobConfig.DEFAULT_MR_AM_NUM_PROGRESS_SPLITS));
//      }
//      return progressSplitBlock;
//    } finally {
//      readLock.unlock();
//    }
//    */
//  }

  private void updateProgressSplits() {
//    double newProgress = reportedStatus.progress;
//    newProgress = Math.max(Math.min(newProgress, 1.0D), 0.0D);
//    TezCounters counters = reportedStatus.counters;
//    if (counters == null)
//      return;
//
//    WrappedProgressSplitsBlock splitsBlock = getProgressSplitBlock();
//    if (splitsBlock != null) {
//      long now = clock.getTime();
//      long start = getLaunchTime();
//
//      if (start == 0)
//        return;
//
//      if (start != 0 && now - start <= Integer.MAX_VALUE) {
//        splitsBlock.getProgressWallclockTime().extend(newProgress,
//            (int) (now - start));
//      }
//
//      TezCounter cpuCounter = counters.findCounter(TaskCounter.CPU_MILLISECONDS);
//      if (cpuCounter != null && cpuCounter.getValue() <= Integer.MAX_VALUE) {
//        splitsBlock.getProgressCPUTime().extend(newProgress,
//            (int) cpuCounter.getValue()); // long to int? TODO: FIX. Same below
//      }
//
//      TezCounter virtualBytes = counters
//        .findCounter(TaskCounter.VIRTUAL_MEMORY_BYTES);
//      if (virtualBytes != null) {
//        splitsBlock.getProgressVirtualMemoryKbytes().extend(newProgress,
//            (int) (virtualBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
//      }
//
//      TezCounter physicalBytes = counters
//        .findCounter(TaskCounter.PHYSICAL_MEMORY_BYTES);
//      if (physicalBytes != null) {
//        splitsBlock.getProgressPhysicalMemoryKbytes().extend(newProgress,
//            (int) (physicalBytes.getValue() / (MEMORY_SPLITS_RESOLUTION)));
//      }
//    }
  }

  private void sendTaskAttemptCleanupEvent() {
//    TaskAttemptContext taContext =
//        new TaskAttemptContextImpl(this.conf,
//            TezMRTypeConverter.fromTez(this.attemptId));
//    sendEvent(new TaskCleanupEvent(this.attemptId, this.committer, taContext));
  }
  
  private TaskLocationHint getTaskLocationHint() {
    return task.getTaskLocationHint();
  }

  protected String[] resolveHosts(String[] src) {
    return TaskAttemptImplHelpers.resolveHosts(src);
  }

  protected void logJobHistoryAttemptStarted() {
    final String containerIdStr = containerId.toString();
    String inProgressLogsUrl = nodeHttpAddress
       + "/" + "node/containerlogs"
       + "/" + containerIdStr
       + "/" + this.appContext.getUser();
    String completedLogsUrl = "";
    if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)
        && conf.get(YarnConfiguration.YARN_LOG_SERVER_URL) != null) {
      String contextStr = "v_" + getVertex().getName()
          + "_" + this.attemptId.toString();
      completedLogsUrl = conf.get(YarnConfiguration.YARN_LOG_SERVER_URL)
          + "/" + containerNodeId.toString()
          + "/" + containerIdStr
          + "/" + contextStr
          + "/" + this.appContext.getUser();
    }
    TaskAttemptStartedEvent startEvt = new TaskAttemptStartedEvent(
        attemptId, getVertex().getName(),
        launchTime, containerId, containerNodeId,
        inProgressLogsUrl, completedLogsUrl, nodeHttpAddress);
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), startEvt));
  }

  protected void logJobHistoryAttemptFinishedEvent(TaskAttemptStateInternal state) {
    //Log finished events only if an attempt started.
    if (getLaunchTime() == 0) return;

    TaskAttemptFinishedEvent finishEvt = new TaskAttemptFinishedEvent(
        attemptId, getVertex().getName(), getLaunchTime(),
        getFinishTime(), TaskAttemptState.SUCCEEDED, null,
        "", getCounters(), lastDataEvents, creationTime, creationCausalTA, allocationTime);
    // FIXME how do we store information regd completion events
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), finishEvt));
  }

  protected void logJobHistoryAttemptUnsuccesfulCompletion(
      TaskAttemptState state) {
    long finishTime = getFinishTime();
    if (finishTime <= 0) {
      finishTime = clock.getTime(); // comes here in case it was terminated before launch
    }
    TaskAttemptFinishedEvent finishEvt = new TaskAttemptFinishedEvent(
        attemptId, getVertex().getName(), getLaunchTime(),
        finishTime, state,
        terminationCause,
        StringUtils.join(
            getDiagnostics(), LINE_SEPARATOR), getCounters(), lastDataEvents, 
        creationTime, creationCausalTA, allocationTime);
    // FIXME how do we store information regd completion events
    this.appContext.getHistoryHandler().handle(
        new DAGHistoryEvent(getDAGID(), finishEvt));
  }

  //////////////////////////////////////////////////////////////////////////////
  //                   Start of Transition Classes                            //
  //////////////////////////////////////////////////////////////////////////////

  protected static class ScheduleTaskattemptTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventSchedule scheduleEvent = (TaskAttemptEventSchedule) event;

      // TODO Creating the remote task here may not be required in case of
      // recovery.

      // Create the remote task.
      TaskSpec remoteTaskSpec;
      try {
        remoteTaskSpec = ta.createRemoteTaskSpec();
        if (LOG.isDebugEnabled()) {
          LOG.debug("remoteTaskSpec:" + remoteTaskSpec);
        }
      } catch (AMUserCodeException e) {
        String msg = "Exception in " + e.getSource() + ", taskAttempt=" + ta;
        LOG.error(msg, e);
        String diag = msg + ", " + e.getMessage() + ", " + ExceptionUtils.getStackTrace(e.getCause());
        new TerminateTransition(FAILED_HELPER).transition(ta,
            new TaskAttemptEventAttemptFailed(ta.getID(), TaskAttemptEventType.TA_FAILED, diag,
                TaskAttemptTerminationCause.APPLICATION_ERROR));
        return TaskAttemptStateInternal.FAILED;
      }
      // Create startTaskRequest

      String[] requestHosts = new String[0];

      // Compute node/rack location request even if re-scheduled.
      Set<String> racks = new HashSet<String>();
      TaskLocationHint locationHint = ta.getTaskLocationHint();
      if (locationHint != null) {
        if (locationHint.getRacks() != null) {
          racks.addAll(locationHint.getRacks());
        }
        if (locationHint.getHosts() != null) {
          for (String host : locationHint.getHosts()) {
            racks.add(RackResolver.resolve(host).getNetworkLocation());
          }
          requestHosts = ta.resolveHosts(locationHint.getHosts()
              .toArray(new String[locationHint.getHosts().size()]));
        }
      }

      ta.taskHosts.addAll(Arrays.asList(requestHosts));
      ta.taskRacks = racks;

      // Ask for hosts / racks only if not a re-scheduled task.
      if (ta.isRescheduled) {
        locationHint = null;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Asking for container launch with taskAttemptContext: "
            + remoteTaskSpec);
      }
      
      // Send out a launch request to the scheduler.
      int priority;
      if (ta.isRescheduled) {
        // higher priority for rescheduled attempts
        priority = scheduleEvent.getPriorityHighLimit();
      } else {
        priority = (scheduleEvent.getPriorityHighLimit() + scheduleEvent.getPriorityLowLimit()) / 2;
      }

      AMSchedulerEventTALaunchRequest launchRequestEvent = new AMSchedulerEventTALaunchRequest(
          ta.attemptId, ta.taskResource, remoteTaskSpec, ta, locationHint,
          priority, ta.containerContext);
      ta.sendEvent(launchRequestEvent);
      return TaskAttemptStateInternal.START_WAIT;
    }
  }

  protected static class DiagnosticInformationUpdater implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventDiagnosticsUpdate diagEvent = (TaskAttemptEventDiagnosticsUpdate) event;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Diagnostics update for " + ta.attemptId + ": "
            + diagEvent.getDiagnosticInfo());
      }
      ta.addDiagnosticInfo(diagEvent.getDiagnosticInfo());
    }
  }

  protected static class TerminateTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    TerminatedTransitionHelper helper;

    public TerminateTransition(TerminatedTransitionHelper helper) {
      this.helper = helper;
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.setFinishTime();

      if (event instanceof DiagnosableEvent) {
        ta.addDiagnosticInfo(((DiagnosableEvent) event).getDiagnosticInfo());
      }
      
      if (event instanceof TaskAttemptEventTerminationCauseEvent) {
        ta.trySetTerminationCause(((TaskAttemptEventTerminationCauseEvent) event).getTerminationCause());
      } else {
        throw new TezUncheckedException("Invalid event received in TerminateTransition"
            + ", requiredClass=TaskAttemptEventTerminationCauseEvent"
            + ", eventClass=" + event.getClass().getName());
      }

      ta.sendEvent(createDAGCounterUpdateEventTAFinished(ta,
          helper.getTaskAttemptState()));
      ta.logJobHistoryAttemptUnsuccesfulCompletion(helper
          .getTaskAttemptState());
      // Send out events to the Task - indicating TaskAttemptTermination(F/K)
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId, helper
          .getTaskEventType(), event));
    }
  }

  protected static class StartedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent origEvent) {
      TaskAttemptEventStartedRemotely event = (TaskAttemptEventStartedRemotely) origEvent;

      AMContainer amContainer = ta.appContext.getAllContainers().get(event.getContainerId()); 
      Container container = amContainer.getContainer();

      ta.allocationTime = amContainer.getCurrentTaskAttemptAllocationTime();
      ta.container = container;
      ta.containerId = event.getContainerId();
      ta.containerNodeId = container.getNodeId();
      ta.nodeHttpAddress = StringInterner.weakIntern(container.getNodeHttpAddress());
      ta.nodeRackName = StringInterner.weakIntern(RackResolver.resolve(ta.containerNodeId.getHost())
          .getNetworkLocation());

      ta.launchTime = ta.clock.getTime();

      // TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr = NetUtils
          .createSocketAddr(ta.nodeHttpAddress); // TODO: Costly?
      ta.trackerName = StringInterner.weakIntern(nodeHttpInetAddr.getHostName());
      ta.httpPort = nodeHttpInetAddr.getPort();
      ta.sendEvent(createDAGCounterUpdateEventTALaunched(ta));

      LOG.info("TaskAttempt: [" + ta.attemptId + "] started."
          + " Is using containerId: [" + ta.containerId + "]" + " on NM: ["
          + ta.containerNodeId + "]");

      // JobHistoryEvent
      ta.logJobHistoryAttemptStarted();

      // TODO Remove after HDFS-5098
      // Compute LOCALITY counter for this task.
      if (ta.taskHosts.contains(ta.containerNodeId.getHost())) {
        ta.localityCounter = DAGCounter.DATA_LOCAL_TASKS;
      } else if (ta.taskRacks.contains(ta.nodeRackName)) {
        ta.localityCounter = DAGCounter.RACK_LOCAL_TASKS;
      } else {
        // Not computing this if the task does not have locality information.
        if (ta.getTaskLocationHint() != null) {
          ta.localityCounter = DAGCounter.OTHER_LOCAL_TASKS;
        }
      }

      // Inform the Task
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_LAUNCHED));
      
      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, TaskAttemptState.RUNNING,
            ta.launchTime, true));
      }

      ta.taskHeartbeatHandler.register(ta.attemptId);
    }
  }
  
  private boolean isSpeculationEnabled() {
    return conf.getBoolean(TezConfiguration.TEZ_AM_SPECULATION_ENABLED,
        TezConfiguration.TEZ_AM_SPECULATION_ENABLED_DEFAULT);
  }

  protected static class TerminatedBeforeRunningTransition extends
      TerminateTransition {

    public TerminatedBeforeRunningTransition(
        TerminatedTransitionHelper helper) {
      super(helper);
    }
    
    protected boolean sendSchedulerEvent() {
      return true;
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      // Inform the scheduler
      if (sendSchedulerEvent()) {
        ta.sendEvent(new AMSchedulerEventTAEnded(ta, ta.containerId, helper
            .getTaskAttemptState()));
      }
    }
  }

  protected static class NodeFailedBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {

    public NodeFailedBeforeRunningTransition() {
      super(KILLED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
    }
  }

  protected static class ContainerTerminatingBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {

    public ContainerTerminatingBeforeRunningTransition() {
      super(FAILED_HELPER);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
    }
  }

  protected static class ContainerCompletedBeforeRunningTransition extends
      TerminatedBeforeRunningTransition {
    public ContainerCompletedBeforeRunningTransition() {
      super(FAILED_HELPER);
    }
    
    public ContainerCompletedBeforeRunningTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }

  }

  protected static class StatusUpdaterTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      TaskAttemptEventStatusUpdate sEvent = (TaskAttemptEventStatusUpdate) event; 
      TaskStatusUpdateEvent statusEvent = sEvent.getStatusEvent();
      ta.reportedStatus.state = ta.getState();
      ta.reportedStatus.progress = statusEvent.getProgress();
      ta.reportedStatus.counters = statusEvent.getCounters();
      ta.statistics = statusEvent.getStatistics();
      if (sEvent.getReadErrorReported()) {
        // if there is a read error then track the next last data event
        ta.appendNextDataEvent = true;
      }

      ta.updateProgressSplits();

      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, ta.getState(),
            ta.clock.getTime()));
      }
    }
  }

  protected static class SucceededTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {

      ta.setFinishTime();
      // Send out history event.
      ta.logJobHistoryAttemptFinishedEvent(TaskAttemptStateInternal.SUCCEEDED);
      ta.sendEvent(createDAGCounterUpdateEventTAFinished(ta,
          TaskAttemptState.SUCCEEDED));

      // Inform the Scheduler.
      ta.sendEvent(new AMSchedulerEventTAEnded(ta, ta.containerId,
          TaskAttemptState.SUCCEEDED));

      // Inform the task.
      ta.sendEvent(new TaskEventTAUpdate(ta.attemptId,
          TaskEventType.T_ATTEMPT_SUCCEEDED));

      // Unregister from the TaskHeartbeatHandler.
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      
      ta.reportedStatus.state = TaskAttemptState.SUCCEEDED;
      ta.reportedStatus.progress = 1.0f;
      
      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, TaskAttemptState.SUCCEEDED,
            ta.clock.getTime()));
      }

      // TODO maybe. For reuse ... Stacking pulls for a reduce task, even if the
      // TA finishes independently. // Will likely be the Job's responsibility.

    }
  }

  protected static class TerminatedWhileRunningTransition extends
      TerminatedBeforeRunningTransition {

    public TerminatedWhileRunningTransition(
        TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.taskHeartbeatHandler.unregister(ta.attemptId);
      ta.reportedStatus.state = helper.getTaskAttemptState(); // FAILED or KILLED
      if (ta.isSpeculationEnabled()) {
        ta.sendEvent(new SpeculatorEventTaskAttemptStatusUpdate(ta.attemptId, helper.getTaskAttemptState(),
            ta.clock.getTime()));
      }
    }
  }

  protected static class ContainerCompletedWhileRunningTransition extends
      TerminatedWhileRunningTransition {
    public ContainerCompletedWhileRunningTransition() {
      super(FAILED_HELPER);
    }
    
    public ContainerCompletedWhileRunningTransition(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }
  }

  protected static class ContainerCompletedWhileTerminating implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      ta.sendTaskAttemptCleanupEvent();
      TaskAttemptEventContainerTerminated tEvent = (TaskAttemptEventContainerTerminated) event;
      ta.addDiagnosticInfo(tEvent.getDiagnosticInfo());
    }

  }

  protected static class TerminatedAfterSuccessHelper extends
      TerminatedBeforeRunningTransition {

    @Override
    protected boolean sendSchedulerEvent() {
      // since the success transition would have sent the event
      // there is no need to send it again
      return false;
    }
    
    public TerminatedAfterSuccessHelper(TerminatedTransitionHelper helper) {
      super(helper);
    }

    @Override
    public void transition(TaskAttemptImpl ta, TaskAttemptEvent event) {
      super.transition(ta, event);
      ta.sendTaskAttemptCleanupEvent();
    }

  }

  protected static class RecoverTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl taskAttempt, TaskAttemptEvent taskAttemptEvent) {
      TaskAttemptStateInternal endState = TaskAttemptStateInternal.FAILED;
      switch(taskAttempt.recoveredState) {
        case NEW:
        case RUNNING:
          // FIXME once running containers can be recovered, this
          // should be handled differently
          // TODO abort taskattempt
          taskAttempt.sendEvent(new TaskEventTAUpdate(taskAttempt.attemptId,
              TaskEventType.T_ATTEMPT_KILLED));
          taskAttempt.sendEvent(createDAGCounterUpdateEventTAFinished(taskAttempt,
              getExternalState(TaskAttemptStateInternal.KILLED)));
          taskAttempt.logJobHistoryAttemptUnsuccesfulCompletion(TaskAttemptState.KILLED);
          endState = TaskAttemptStateInternal.KILLED;
          break;
        case SUCCEEDED:
          // Do not inform Task as it already knows about completed attempts
          endState = TaskAttemptStateInternal.SUCCEEDED;
          break;
        case FAILED:
          // Do not inform Task as it already knows about completed attempts
          endState = TaskAttemptStateInternal.FAILED;
          break;
        case KILLED:
          // Do not inform Task as it already knows about completed attempts
          endState = TaskAttemptStateInternal.KILLED;
          break;
        default:
          throw new RuntimeException("Failed to recover from non-handled state"
              + ", taskAttemptId=" + taskAttempt.getID()
              + ", state=" + taskAttempt.recoveredState);
      }

      return endState;
    }

  }

  protected static class TerminatedAfterSuccessTransition implements
      MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {
    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl attempt, TaskAttemptEvent event) {
      if (attempt.leafVertex) {
        return TaskAttemptStateInternal.SUCCEEDED;
      }
      // TODO - TEZ-834. This assumes that the outputs were on that node
      attempt.sendInputFailedToConsumers();
      TaskAttemptImpl.TERMINATED_AFTER_SUCCESS_HELPER.transition(attempt, event);
      return TaskAttemptStateInternal.KILLED;
    }
  }
  
  protected static class OutputReportedFailedTransition implements
  MultipleArcTransition<TaskAttemptImpl, TaskAttemptEvent, TaskAttemptStateInternal> {

    @Override
    public TaskAttemptStateInternal transition(TaskAttemptImpl attempt,
        TaskAttemptEvent event) {
      TaskAttemptEventOutputFailed outputFailedEvent = 
          (TaskAttemptEventOutputFailed) event;
      TezEvent tezEvent = outputFailedEvent.getInputFailedEvent();
      TezTaskAttemptID failedDestTaId = tezEvent.getSourceInfo().getTaskAttemptID();
      InputReadErrorEvent readErrorEvent = (InputReadErrorEvent)tezEvent.getEvent();
      int failedInputIndexOnDestTa = readErrorEvent.getIndex();
      if (readErrorEvent.getVersion() != attempt.getID().getId()) {
        throw new TezUncheckedException(attempt.getID()
            + " incorrectly blamed for read error from " + failedDestTaId
            + " at inputIndex " + failedInputIndexOnDestTa + " version"
            + readErrorEvent.getVersion());
      }
      LOG.info(attempt.getID()
            + " blamed for read error from " + failedDestTaId
            + " at inputIndex " + failedInputIndexOnDestTa);
      long time = attempt.clock.getTime();
      Long firstErrReportTime = attempt.uniquefailedOutputReports.get(failedDestTaId);
      if (firstErrReportTime == null) {
        attempt.uniquefailedOutputReports.put(failedDestTaId, time);
        firstErrReportTime = time;
      }
      
      int readErrorTimespanSec = (int)((time - firstErrReportTime)/1000);
      boolean crossTimeDeadline = readErrorTimespanSec >=
      MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC ? true : false;

      float failureFraction = ((float) attempt.uniquefailedOutputReports.size())
          / outputFailedEvent.getConsumerTaskNumber();

      boolean withinFailureFractionLimits =
          (failureFraction <= MAX_ALLOWED_OUTPUT_FAILURES_FRACTION);
      boolean withinOutputFailureLimits =
          (attempt.uniquefailedOutputReports.size() < MAX_ALLOWED_OUTPUT_FAILURES);

      // If needed we can launch a background task without failing this task
      // to generate a copy of the output just in case.
      // If needed we can consider only running consumer tasks
      if (!crossTimeDeadline && withinFailureFractionLimits && withinOutputFailureLimits) {
        return attempt.getInternalState();
      }
      String message = attempt.getID() + " being failed for too many output errors. "
          + "failureFraction=" + failureFraction
          + ", MAX_ALLOWED_OUTPUT_FAILURES_FRACTION=" + MAX_ALLOWED_OUTPUT_FAILURES_FRACTION
          + ", uniquefailedOutputReports=" + attempt.uniquefailedOutputReports.size()
          + ", MAX_ALLOWED_OUTPUT_FAILURES=" + MAX_ALLOWED_OUTPUT_FAILURES
          + ", MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC=" + MAX_ALLOWED_TIME_FOR_TASK_READ_ERROR_SEC
          + ", readErrorTimespan=" + readErrorTimespanSec;
      LOG.info(message);
      attempt.addDiagnosticInfo(message);
      // send input failed event
      attempt.sendInputFailedToConsumers();
      // Not checking for leafVertex since a READ_ERROR should only be reported for intermediate tasks.
      if (attempt.getInternalState() == TaskAttemptStateInternal.SUCCEEDED) {
        (new TerminatedAfterSuccessHelper(FAILED_HELPER)).transition(
            attempt, event);
        return TaskAttemptStateInternal.FAILED;
      } else {
        (new TerminatedWhileRunningTransition(FAILED_HELPER)).transition(
            attempt, event);
        return TaskAttemptStateInternal.FAIL_IN_PROGRESS;
      }
      // TODO at some point. Nodes may be interested in FetchFailure info.
      // Can be used to blacklist nodes.
    }
  }
  
  @VisibleForTesting
  protected void sendInputFailedToConsumers() {
    Vertex vertex = getVertex();
    Map<Vertex, Edge> edges = vertex.getOutputVertices();
    if (edges != null && !edges.isEmpty()) {
      List<TezEvent> tezIfEvents = Lists.newArrayListWithCapacity(edges.size());
      for (Vertex edgeVertex : edges.keySet()) {
        tezIfEvents.add(new TezEvent(new InputFailedEvent(), 
            new EventMetaData(EventProducerConsumerType.SYSTEM, 
                vertex.getName(), 
                edgeVertex.getName(), 
                getID()), appContext.getClock().getTime()));
      }
      sendEvent(new VertexEventRouteEvent(vertex.getVertexId(), tezIfEvents));
    }
  }
  
  private void trySetTerminationCause(TaskAttemptTerminationCause err) {
    // keep only the first error cause
    if (terminationCause == TaskAttemptTerminationCause.UNKNOWN_ERROR) {
      terminationCause = err;
    }
  }

  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    // result.phase = Phase.STARTING;
    //result.stateString = "NEW";
    result.state = TaskAttemptState.NEW;
    //TezCounters counters = EMPTY_COUNTERS;
    //result.counters = counters;
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  protected interface TerminatedTransitionHelper {

    public TaskAttemptStateInternal getTaskAttemptStateInternal();

    public TaskAttemptState getTaskAttemptState();

    public TaskEventType getTaskEventType();
  }

  protected static class FailedTransitionHelper implements
      TerminatedTransitionHelper {
    public TaskAttemptStateInternal getTaskAttemptStateInternal() {
      return TaskAttemptStateInternal.FAILED;
    }

    @Override
    public TaskAttemptState getTaskAttemptState() {
      return TaskAttemptState.FAILED;
    }

    @Override
    public TaskEventType getTaskEventType() {
      return TaskEventType.T_ATTEMPT_FAILED;
    }
  }

  protected static class KilledTransitionHelper implements
      TerminatedTransitionHelper {

    @Override
    public TaskAttemptStateInternal getTaskAttemptStateInternal() {
      return TaskAttemptStateInternal.KILLED;
    }

    @Override
    public TaskAttemptState getTaskAttemptState() {
      return TaskAttemptState.KILLED;
    }

    @Override
    public TaskEventType getTaskEventType() {
      return TaskEventType.T_ATTEMPT_KILLED;
    }
  }

  @Override
  public String toString() {
    return getID().toString();
  }


  @Override
  public void setLastEventSent(TezEvent lastEventSent) {
    writeLock.lock();
    try {
      // TEZ-3066 ideally Heartbeat just happens in FAIL_IN_PROGRESS & KILL_IN_PROGRESS,
      // add other states here just in case. create TEZ-3068 for a more elegant solution.
      if (!EnumSet.of(TaskAttemptStateInternal.FAIL_IN_PROGRESS,
        TaskAttemptStateInternal.KILL_IN_PROGRESS,
        TaskAttemptStateInternal.FAILED,
        TaskAttemptStateInternal.KILLED,
        TaskAttemptStateInternal.SUCCEEDED).contains(getInternalState())) {
        DataEventDependencyInfo info = new DataEventDependencyInfo(
          lastEventSent.getEventReceivedTime(), lastEventSent.getSourceInfo().getTaskAttemptID());
        // task attempt id may be null for input data information events
        if (appendNextDataEvent) {
          appendNextDataEvent = false;
          lastDataEvents.add(info);
        } else {
          // over-write last event - array list makes it quick
          lastDataEvents.set(lastDataEvents.size() - 1, info);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
}
