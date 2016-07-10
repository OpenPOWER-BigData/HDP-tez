/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.TimelineRESTAdapter = DS.RESTAdapter.extend({
  ajax: function(url, method, hash) {
    hash = hash || {}; // hash may be undefined
    hash.crossDomain = true;
    hash.xhrFields = {withCredentials: true};
    hash.targetServer = "Timeline Server";
    return this._super(url, method, hash);
  },
	namespace: App.Configs.restNamespace.timeline,
	pathForType: App.Helpers.misc.timelinePathForType
});

App.TimelineSerializer = DS.RESTSerializer.extend({
	extractSingle: function(store, primaryType, rawPayload, recordId) {
		// rest serializer expects singular form of model as the root key.
		var payload = {};
		payload[primaryType.typeKey] = rawPayload;
		return this._super(store, primaryType, payload, recordId);
	},

	extractArray: function(store, primaryType, rawPayload) {
		// restserializer expects a plural of the model but TimelineServer returns
		// it in entities.
		var payload = {};
		payload[primaryType.typeKey.pluralize()] = rawPayload.entities;
		return this._super(store, primaryType, payload);
	},

  // normalizes countergroups returns counterGroups and counters.
  normalizeCounterGroupsHelper: function(parentType, parentID, entity) {
    // create empty countergroups if not there - to make code below easier.
    entity.otherinfo.counters = entity.otherinfo.counters || {}
    entity.otherinfo.counters.counterGroups = entity.otherinfo.counters.counterGroups || [];

    var counterGroups = [];
    var counters = [];

    var counterGroupsIDs = entity.otherinfo.counters.counterGroups.map(function(counterGroup) {
      var cg = {
        id: parentID + '/' + counterGroup.counterGroupName,
        name: counterGroup.counterGroupName,
        displayName: counterGroup.counterGroupDisplayName,
        parentID: { // polymorphic requires type and id.
          type: parentType,
          id: parentID
        }
      };
      cg.counters = counterGroup.counters.map(function(counter){
        var c = {
          id: cg.id + '/' + counter.counterName,
          name: counter.counterName,
          displayName: counter.counterName,
          value: counter.counterValue,
          parentID: cg.id
        };
        counters.push(c);
        return c.id;
      });
      counterGroups.push(cg);
      return cg.id;
    });

    return {
      counterGroups: counterGroups,
      counters: counters,
      counterGroupsIDs: counterGroupsIDs
    }
  }
});


var timelineJsonToDagMap = {
  id: 'entity',
  submittedTime: 'starttime',
  startTime: 'otherinfo.startTime',
  endTime: 'otherinfo.endTime',
  name: 'primaryfilters.dagName.0',
  user: 'primaryfilters.user.0',
  status: 'otherinfo.status',
  callerId: 'primaryfilters.callerId.0',

  progress: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 1 : null;
    }
  },

  containerLogs: {
    custom: function(source) {

      var containerLogs = [];
      var otherinfo = Em.get(source, 'otherinfo');
      for (var key in otherinfo) {
        if (key.indexOf('inProgressLogsURL_') === 0) {
          var logs = Em.get(source, 'otherinfo.' + key);
          if (logs.indexOf('http') !== 0) {
            logs = 'http://' + logs;
          }
          var attemptid = key.substring(18);
          containerLogs.push({id : attemptid, containerLog: logs});
        }
      }
      return containerLogs;
    }
  },
  hasFailedTaskAttempts: {
    custom: function(source) {
      // if no other info is available we say no failed tasks attempts.
      // since otherinfo is populated only at the end.
      var numFailedAttempts = Em.get(source, 'otherinfo.numFailedTaskAttempts');
      return !!numFailedAttempts && numFailedAttempts > 0;
    }
  },
  numFailedTasks: 'otherinfo.numFailedTasks',
  diagnostics: 'otherinfo.diagnostics',

  counterGroups: 'otherinfo.counters.counterGroups',

  planName: 'otherinfo.dagPlan.dagName',
  planVersion: 'otherinfo.dagPlan.version',
  amWebServiceVersion: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.amWebServiceVersion') || '1';
    }
  },
  appContextInfo: {
    custom: function (source) {
      var appType = undefined,
          info = undefined;
      var dagInfoStr = Em.get(source, 'otherinfo.dagPlan.dagInfo');
      if (!!dagInfoStr) {
        try {
          var dagInfo = $.parseJSON(dagInfoStr);
          appType = dagInfo['context'];
          info = dagInfo['description'];
        } catch (e) {
          info = dagInfoStr;
        }
      }

      return {
        appType: appType,
        info: info
      };
    }
  },
  vertices: 'otherinfo.dagPlan.vertices',
  edges: 'otherinfo.dagPlan.edges',
  vertexGroups: 'otherinfo.dagPlan.vertexGroups',

  vertexIdToNameMap: {
    custom: function(source) {
      var nameToIdMap = Em.get(source, 'otherinfo.vertexNameIdMapping') || {};
      var idToNameMap = {};
      $.each(nameToIdMap, function(vertexName, vertexId) {
        idToNameMap[vertexId] = vertexName;
      });
      return idToNameMap;
    }
  },
};

App.DagSerializer = App.TimelineSerializer.extend({
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToDagMap);
  },
});

var timelineJsonToTaskAttemptMap = {
  id: 'entity',
  startTime: 'otherinfo.startTime',
  endTime: 'otherinfo.endTime',
  status: 'otherinfo.status',
  diagnostics: 'otherinfo.diagnostics',
  counterGroups: 'otherinfo.counters.counterGroups',

  progress: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 1 : null;
    }
  },

  inProgressLog: 'otherinfo.inProgressLogsURL',
  completedLog: 'otherinfo.completedLogsURL',

  taskID: 'primaryfilters.TEZ_TASK_ID.0',
  vertexID: 'primaryfilters.TEZ_VERTEX_ID.0',
  dagID: 'primaryfilters.TEZ_DAG_ID.0',
  containerId: 'otherinfo.containerId',
  nodeId: 'otherinfo.nodeId',
  diagnostics: 'otherinfo.diagnostics'
};

App.DagTaskAttemptSerializer =
App.VertexTaskAttemptSerializer =
App.TaskTaskAttemptSerializer =
App.TaskAttemptSerializer = App.TimelineSerializer.extend({
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToTaskAttemptMap);
  },
});

var timelineJsonToTaskMap = {
  id: 'entity',
  dagID: 'primaryfilters.TEZ_DAG_ID.0',
  startTime: 'otherinfo.startTime',
  vertexID: 'primaryfilters.TEZ_VERTEX_ID.0',
  endTime: 'otherinfo.endTime',
  status: 'otherinfo.status',
  progress: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 1 : null;
    }
  },
  numFailedTaskAttempts: 'otherinfo.numFailedTaskAttempts',
  diagnostics: 'otherinfo.diagnostics',
  counterGroups: 'otherinfo.counters.counterGroups',
  successfulAttemptId: 'otherinfo.successfulAttemptId',
  attempts: 'relatedentities.TEZ_TASK_ATTEMPT_ID',
  numAttempts: 'relatedentities.TEZ_TASK_ATTEMPT_ID.length'
};

App.DagTaskSerializer =
App.VertexTaskSerializer =
App.TaskSerializer = App.TimelineSerializer.extend({
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToTaskMap);
  },
});

var timelineJsonToVertexMap = {
  id: 'entity',
  name: 'otherinfo.vertexName',
  dagID: 'primaryfilters.TEZ_DAG_ID.0',
  processorClassName: 'processorClassName',
  counterGroups: 'otherinfo.counters.counterGroups',
  inputs: 'inputs',
  outputs: 'outputs',

  startTime: 'otherinfo.startTime',
  endTime: 'otherinfo.endTime',

  progress: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 1 : null;
    }
  },
  runningTasks: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 0 : null;
    }
  },
  pendingTasks: {
    custom: function(source) {
      return Em.get(source, 'otherinfo.status') == 'SUCCEEDED' ? 0 : null;
    }
  },

  status: 'otherinfo.status',
  hasFailedTaskAttempts: {
    custom: function(source) {
      // if no other info is available we say no failed tasks attempts.
      // since otherinfo is populated only at the end.
      var numFailedAttempts = Em.get(source, 'otherinfo.numFailedTaskAttempts');
      return !!numFailedAttempts && numFailedAttempts > 0;
    }
  },
  diagnostics: 'otherinfo.diagnostics',

  failedTaskAttempts: 'otherinfo.numFailedTaskAttempts',
  killedTaskAttempts: 'otherinfo.numKilledTaskAttempts',

  failedTasks: 'otherinfo.numFailedTasks',
  sucessfulTasks: 'otherinfo.numSucceededTasks',
  numTasks: 'otherinfo.numTasks',
  killedTasks: 'otherinfo.numKilledTasks',

  firstTaskStartTime: 'otherinfo.stats.firstTaskStartTime',
  lastTaskFinishTime:  'otherinfo.stats.lastTaskFinishTime',

  firstTasksToStart:  'otherinfo.stats.firstTasksToStart',
  lastTasksToFinish:  'otherinfo.stats.lastTasksToFinish',

  minTaskDuration:  'otherinfo.stats.minTaskDuration',
  maxTaskDuration:  'otherinfo.stats.maxTaskDuration',
  avgTaskDuration:  'otherinfo.stats.avgTaskDuration',

  shortestDurationTasks:  'otherinfo.stats.shortestDurationTasks',
  longestDurationTasks:  'otherinfo.stats.longestDurationTasks'
};

App.VertexSerializer = App.TimelineSerializer.extend({
  _normalizeSingleVertexPayload: function(vertex) {
    processorClassName = Ember.get(vertex, 'otherinfo.processorClassName') || "",
    inputs = [],
    inputIds = [],
    outputs = [],
    outputIds = [];

    vertex.processorClassName = processorClassName.substr(processorClassName.lastIndexOf('.') + 1);

    if(vertex.inputs) {
      vertex.inputs.forEach(function (input, index) {
        input.entity = vertex.entity + '-input' + index;
        inputIds.push(input.entity);
        inputs.push(input);
      });
      vertex.inputs = inputIds;
    }

    if(vertex.outputs) {
      vertex.outputs.forEach(function (output, index) {
        output.entity = vertex.entity + '-output' + index;
        outputIds.push(output.entity);
        outputs.push(output);
      });
      vertex.outputs = outputIds;
    }

    return {
      vertex: vertex,
      inputs: inputs,
      outputs: outputs
    };
  },

  normalizePayload: function(rawPayload, property) {
    var pluralizedPoperty,
        normalizedPayload,
        n;

    property = property || 'vertex',
    pluralizedPoperty = property.pluralize();;
    if (!!rawPayload[pluralizedPoperty]) {
      normalizedPayload = {
        inputs: [],
        outputs: [],
      };
      normalizedPayload[pluralizedPoperty] = [];

      rawPayload[pluralizedPoperty].forEach(function(vertex){
        n = this._normalizeSingleVertexPayload(vertex);
        normalizedPayload[pluralizedPoperty].push(n.vertex);
        [].push.apply(normalizedPayload.inputs, n.inputs);
        [].push.apply(normalizedPayload.outputs, n.outputs);
      }, this);

      // delete so that we dont hang on to the json data.
      delete rawPayload[pluralizedPoperty];

      return normalizedPayload;
    } else {
      n = this._normalizeSingleVertexPayload(rawPayload[property]);
      normalizedPayload = {
        inputs : n.inputs,
        outputs: n.outputs
      };
      normalizedPayload[property] = n.vertex;

      return normalizedPayload;
    }
  },

  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToVertexMap);
  },
});

App.DagVertexSerializer = App.VertexSerializer.extend({
  normalizePayload: function (rawPayload) {
    return this._super(rawPayload, 'dagVertex');
  }
});

App.InputSerializer = App.TimelineSerializer.extend({
  _map: {
    id: 'entity',
    inputName: 'name',
    inputClass: 'class',
    inputInitializer: 'initializer',
    configs: 'configs'
  },
  _normalizeData: function(data) {
    var userPayload = JSON.parse(data.userPayloadAsText || null),
        store = this.get('store'),
        configs,
        configKey,
        configIndex = 0,
        id;

    data.configs = [];

    if(userPayload) {
      configs = userPayload.config || userPayload.dist;
      for(configKey in configs) {
        id = data.entity + configIndex++;
        data.configs.push(id);
        store.push('KVDatum', {
          id: id,
          key: configKey,
          value: configs[configKey]
        });
      }
    }

    return data;
  },
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(this._normalizeData(hash), this._map);
  }
});

App.OutputSerializer = App.TimelineSerializer.extend({
  _map: {
    id: 'entity',
    outputName: 'name',
    outputClass: 'class',
    configs: 'configs'
  },
  _normalizeData: function(data) {
    var userPayload = JSON.parse(data.userPayloadAsText || null),
        store = this.get('store'),
        configs,
        configKey,
        configIndex = 0,
        id;

    data.configs = [];

    if(userPayload) {
      configs = userPayload.config || userPayload.dist;
      for(configKey in configs) {
        id = data.entity + configIndex++;
        data.configs.push(id);
        store.push('KVDatum', {
          id: id,
          key: configKey,
          value: configs[configKey]
        });
      }
    }

    return data;
  },
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(this._normalizeData(hash), this._map);
  }
});

var timelineJsonToAppDetailMap = {
  id: 'appId',
  attemptId: {
    custom: function(source) {
      // while an attempt is in progress the attempt id contains a '-'
      return (Em.get(source, 'currentAppAttemptId') || '').replace('-','');
    }
  },

  name: 'name',
  queue: 'queue',
  user: 'user',
  type: 'type',

  startedTime: 'startedTime',
  elapsedTime: 'elapsedTime',
  finishedTime: 'finishedTime',
  submittedTime: 'submittedTime',

  status: 'appState',

  finalStatus: 'finalAppStatus',
  diagnostics: 'otherinfo.diagnostics',
};

App.AppDetailSerializer = App.TimelineSerializer.extend({
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToAppDetailMap);
  },
});

var timelineJsonToTezAppMap = {
  id: 'entity',

  appId: 'appId',

  entityType: 'entitytype',

  startedTime: 'startedTime',
  domain: 'domain',

  user: 'primaryfilters.user.0',

  dags: 'relatedentities.TEZ_DAG_ID',
  configs: 'configs',

  tezBuildTime: 'otherinfo.tezVersion.buildTime',
  tezRevision: 'otherinfo.tezVersion.revision',
  tezVersion: 'otherinfo.tezVersion.version'
};

App.TezAppSerializer = App.TimelineSerializer.extend({
  _normalizeSinglePayload: function(rawPayload){
    var configs = rawPayload.otherinfo.config,
    appId = rawPayload.entity.substr(4),
    kVData = [],
    id;

    rawPayload.appId = appId;
    rawPayload.configs = [];

    for(var key in configs) {
      id = appId + key;
      rawPayload.configs.push(id);
      kVData.push({
        id: id,
        key: key,
        value: configs[key]
      });
    }

    return {
      tezApp: rawPayload,
      kVData: kVData
    };
  },
  normalizePayload: function(rawPayload) {
    if (!!rawPayload.tezApps) {
      var normalizedPayload = {
        tezApps: [],
        kVData: []
      },
      push = Array.prototype.push;
      rawPayload.tezApps.forEach(function(app){
        var n = this._normalizeSinglePayload(app);
        normalizedPayload.tezApps.push(n.tezApp);
        push.apply(normalizedPayload.kVData,n.kVData);
      });
      return normalizedPayload;
    }
    else {
      return this._normalizeSinglePayload(rawPayload.tezApp)
    }
  },
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToTezAppMap);
  },
});

var timelineJsonToHiveQueryMap = {
  id: 'entity',
  query: 'otherinfo.QUERY'
};

App.HiveQuerySerializer = App.TimelineSerializer.extend({
  _normalizeSingleDagPayload: function(hiveQuery) {
    return {
      hiveQuery: hiveQuery
    }
  },

  normalizePayload: function(rawPayload){
    // we handled only single hive
    return this._normalizeSingleDagPayload(rawPayload.hiveQuery);
  },

  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, timelineJsonToHiveQueryMap);
  }
});

App.VertexProgressSerializer = App.DagProgressSerializer = DS.RESTSerializer.extend({});

// v2 version of am web services
App.DagInfoSerializer = DS.RESTSerializer.extend({
  normalizePayload: function(rawPayload) {
    return {
      dagInfo : [rawPayload.dag]
    }
  }
});

App.VertexInfoSerializer = DS.RESTSerializer.extend({
  map: {
    id: 'id',
    progress: 'progress',
    status: 'status',
    numTasks: 'totalTasks',
    runningTasks: 'runningTasks',
    sucessfulTasks: 'succeededTasks',
    failedTaskAttempts: 'failedTaskAttempts',
    killedTaskAttempts: 'killedTaskAttempts',
    counters: 'counters'
  },
  normalizePayload: function(rawPayload) {
    return {
      vertexInfo : rawPayload.vertices
    }
  },
  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, this.get('map'));
  }
});

App.TaskInfoSerializer = DS.RESTSerializer.extend({
  normalizePayload: function(rawPayload) {
    return {
      taskInfo : rawPayload.tasks
    }
  }
});

App.AttemptInfoSerializer = DS.RESTSerializer.extend({
  normalizePayload: function(rawPayload) {
    return {
      attemptInfo : rawPayload.attempts
    }
  }
});

App.ClusterAppSerializer = App.TimelineSerializer.extend({
  map: {
    id: 'id',
    status: 'state',
    finalStatus: 'finalStatus',

    name: 'name',
    queue: 'queue',
    user: 'user',
    type: 'type',

    startedTime: 'startedTime',
    elapsedTime: 'elapsedTime',
    finishedTime: 'finishedTime',

    progress: 'progress'
  },

  _normalizeSingleDagPayload: function(rawPayload) {
    return {
      clusterApp: rawPayload.clusterApp.app
    }
  },

  normalizePayload: function(rawPayload){
    // we handled only single clusterApp
    return this._normalizeSingleDagPayload(rawPayload);
  },

  normalize: function(type, hash, prop) {
    return Em.JsonMapper.map(hash, this.get('map'));
  }
});
