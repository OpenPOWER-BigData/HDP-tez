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

App.DagCountersController = App.PollingController.extend(App.Helpers.DisplayHelper, App.ModelRefreshMixin, {
  controllerName: 'DagCountersController',

  pollingType: 'dagInfo',

  pollsterControl: function () {
    if(this.get('status') == 'RUNNING' &&
        this.get('amWebServiceVersion') != '1' &&
        this.get('pollingEnabled') &&
        this.get('isActive')) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('status', 'amWebServiceVersion', 'isActive', 'pollingEnabled'),

  pollsterOptionsObserver: function () {
    var model = this.get('model');

    this.get('pollster').setProperties( (model && model.get('status') != 'SUCCEEDED') ? {
      targetRecords: [model],
      options: {
        appID: this.get('applicationId'),
        dagID: App.Helpers.misc.getIndexFromId(this.get('id')),
        counters: '*'
      }
    } : {
      targetRecords: [],
      options: null
    });
  }.observes('applicationId', 'model', 'model.status', 'id'),

  applicationComplete: function () {
    this.get('pollster').stop();
    this.set('pollster.polledRecords', null);
  }
});
