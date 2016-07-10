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

App.TezAppIndexController = App.PollingController.extend(App.ModelRefreshMixin, {

  needs: "tezApp",
  controllerName: 'TezAppIndexController',

  rmTrackingURL: function() {
    return "%@/%@/app/%@".fmt(App.env.RMWebUrl, App.Configs.otherNamespace.cluster, this.get('appId'));
  }.property('appId'),

  load: function () {
    var tezApp = this.get('model'),
      store  = this.get('store');

      tezApp.reload().then(function (tezApp) {
        var appId = tezApp.get('appId');
        if(!appId) return tezApp;
        return App.Helpers.misc.loadApp(store, appId).then(function (appDetails){
          tezApp.set('appDetail', appDetails);
          return tezApp;
        });
      }).catch(function (error) {
        Em.Logger.error(error);
        var err = App.Helpers.misc.formatError(error);
        var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
        App.Helpers.ErrorBar.getInstance().show(msg, err.details);
      });
  },

  appUser: function() {
    return this.get('appDetail.user') || this.get('user');
  }.property('appDetail.user', 'user'),

  iconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('model.appDetail.finalStatus'));
  }.property('id', 'appDetail.finalStatus'),
});
