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

App.Helpers.EntityArrayPollster = App.Helpers.Pollster.extend({
  entityType: null, // Entity type to be polled
  store: null,
  mergeProperties: [],
  options: null,

  isRunning: false,
  isWaiting: false,

  polledRecords: null,
  targetRecords: [],

  _ready: function () {
    return this.get('entityType') &&
        this.get('store') &&
        this.get('options') &&
        this.get('targetRecords.length');
  }.property('entityType', 'store', 'options', 'targetRecords.length'),

  start: function(runImmediate, interval) {
    if(!this.get('isRunning')) {
      this.set('isRunning', true);
      this._super(runImmediate == undefined ? true : runImmediate, interval);
    }
  },

  stop: function() {
    if(this.get('isRunning')) {
      this._super();
      this.set('isRunning', false);
    }
  },

  onPoll: function(){
    if(!this.get('isWaiting') && this.get('_ready')) {
      this.set('isWaiting', true);

      return this.store.findQuery(this.get('entityType'), {
        metadata: this.get('options')
      }).then(this._callIfRunning(this, 'onResponse')).
      catch(this._callIfRunning(this, 'onFailure')).
      finally(this._final.bind(this));
    }
  },

  _preRequisitesObserver: function () {
    if(this.get('isRunning')) {
      this.onPoll();
    }
  }.observes('options', 'targetRecords'),

  _callIfRunning: function (that, funName) {
    return function (data) {
      var fun = that.get(funName);
      if(fun && that.get('isRunning')) {
        fun.call(that, data);
      }
    };
  },

  onResponse: function (data) {
    this.set('polledRecords', data);
    this.mergeToTarget();
  },

  onFailure: function (err) {
    // Implement based on requirement
  },

  _final: function () {
    this.set('isWaiting', false);
  },

  mergeToTarget: function () {
    App.Helpers.emData.mergeRecords(
      this.get('targetRecords'),
      this.get('polledRecords'),
      this.get('mergeProperties') || []
    );
  }.observes('targetRecords').on('init')
});
