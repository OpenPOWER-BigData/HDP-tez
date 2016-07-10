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

import java.util.List;

import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TezEvent;

public class VertexEventRouteEvent extends VertexEvent {
  
  final List<TezEvent> events;

  final boolean recovered;

  public VertexEventRouteEvent(TezVertexID vertexId, List<TezEvent> events) {
    this(vertexId, events, false);
  }

  public VertexEventRouteEvent(TezVertexID vertexId, List<TezEvent> events,
      boolean recovered) {
    super(vertexId, VertexEventType.V_ROUTE_EVENT);
    this.events = events;
    this.recovered = recovered;
  }

  public List<TezEvent> getEvents() {
    return events;
  }

  public boolean isRecovered() {
    return recovered;
  }

}
