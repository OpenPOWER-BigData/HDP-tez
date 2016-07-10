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

package org.apache.tez.dag.app.dag.event;

import org.apache.tez.dag.records.TaskAttemptTerminationCause;
import org.apache.tez.dag.records.TezTaskAttemptID;

public class TaskAttemptEventAttemptFailed extends TaskAttemptEvent 
  implements DiagnosableEvent, TaskAttemptEventTerminationCauseEvent {

  private final String diagnostics;
  private final TaskAttemptTerminationCause errorCause;
  public TaskAttemptEventAttemptFailed(TezTaskAttemptID id,
      TaskAttemptEventType type, String diagnostics, TaskAttemptTerminationCause errorCause) {
    super(id, type);
    this.diagnostics = diagnostics;
    this.errorCause = errorCause;
  }

  @Override
  public String getDiagnosticInfo() {
    return diagnostics;
  }
  
  @Override
  public TaskAttemptTerminationCause getTerminationCause() {
    return errorCause;
  }

}
