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

App.Router.map(function() {
  this.resource('dags', { path: '/' });
  this.resource('dag', { path: '/dag/:dag_id'}, function() {
    this.route('vertices');
    this.route('view');
    this.route('tasks');
    this.route('taskAttempts');
    this.route('counters');
    this.route('swimlane');
  });

  this.resource('tez-app', {path: '/tez-app/:app_id'}, function(){
    this.route('dags');
    this.route('configs');
  });

  this.resource('vertex', {path: '/vertex/:vertex_id'}, function(){
    this.route('tasks');
    this.route('additionals');
    this.resource('input', {path: '/input/:input_id'}, function(){
      this.route('configs');
    });
    this.resource('output', {path: '/output/:input_id'}, function(){
      this.route('configs');
    });
    this.route('taskAttempts');
    this.route('counters');
    this.route('details');
    this.route('swimlane');
  });

  this.resource('tasks', {path: '/tasks'});
  this.resource('task', {path: '/task/:task_id'}, function(){
    this.route('attempts');
    this.route('counters');
  });

  this.resource('taskAttempt', {path: '/task_attempt/:task_attempt_id'}, function() {
    this.route('counters');
  });

  this.resource('error', {path: '/error'});
});

/* --- Router helper functions --- */

function renderSwimlanes () {
  this.render('common/swimlanes');
}

function renderConfigs() {
  this.render('common/configs');
}

function renderTable() {
  this.render('common/table');
}

/*
 * Creates a setupController function
 * @param format Unformatted title string.
 * @param Optional, arguments as string can be tailed after format to specify the property path.
 *        i.e. 'Dag - %@ (%@)', 'name', 'id' would give 'Dag - dag_name (dag_id)'
 * @return setupController function
 */
function setupControllerFactory(format) {
  var fmtArgs = Array.prototype.slice.call(arguments, 1);

  return function (controller, model) {
    var fmtValues, title;

    if(format) {
      if(model && fmtArgs.length) {
        fmtValues = fmtArgs.map(function (key) {
          return model.get(key);
        }),
        title = format.fmt.apply(format, fmtValues);
      }
      else {
        title = format;
      }

      $(document).attr('title', title);
    }

    this._super(controller, model);
    if(controller.setup) {
      controller.setup();
    }

    if(controller.loadData) {
      controller.loadData();
    }
  };
}

/* --- Base route class --- */
App.BaseRoute = Em.Route.extend({
  setupController: setupControllerFactory(),
  resetController: function() {
    if(this.controller.reset) {
      this.controller.reset();
    }
  },
  actions: {
    pollingEnabledChanged: function (enabled) {
      if(this.get('controller.pollster')) {
        this.set('controller.pollingEnabled', enabled);
      }
      return true;
    }
  }
});

App.ApplicationRoute = Em.Route.extend({
  actions: {
    willTransition: function(transition) {
      App.Helpers.ErrorBar.getInstance().hide();
      $(document).tooltip("close");
    },
    error: function(error, transition, originRoute) {
      this.replaceWith('error');
      Em.Logger.error(error);
      var defaultError = 'Error while loading %@.'.fmt(transition.targetName);
      var err = App.Helpers.misc.formatError(error, defaultError);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, error.details);
    },
  }
});
/* --- Dag related routes --- */

App.DagsRoute = App.BaseRoute.extend({
  queryParams:  {
    count: App.Helpers.misc.defaultQueryParamsConfig,
    fromID: App.Helpers.misc.defaultQueryParamsConfig,
    user: App.Helpers.misc.defaultQueryParamsConfig,
    status: App.Helpers.misc.defaultQueryParamsConfig,
    appid: App.Helpers.misc.defaultQueryParamsConfig,
    dag_name: App.Helpers.misc.defaultQueryParamsConfig
  },
  setupController: setupControllerFactory('All Dags'),
});

App.DagRoute = App.BaseRoute.extend({
  model: function(params) {
    return this.store.find('dag', params.dag_id);
  },
  afterModel: function(model) {
    return this.controllerFor('dag').loadAdditional(model);
  },
  setupController: setupControllerFactory('Dag: %@ (%@)', 'name', 'id'),
});

App.DagViewRoute = App.BaseRoute.extend({
  setupController: setupControllerFactory()
});

App.DagSwimlaneRoute = App.BaseRoute.extend({
  renderTemplate: renderSwimlanes,
  model: function(params) {
    var model = this.modelFor('dag'),
        queryParams = {'primaryFilter': 'TEZ_DAG_ID:' + model.id};
    this.store.unloadAll('task_attempt');
    return this.store.findQuery('task_attempt', queryParams);
  },
  setupController: setupControllerFactory()
});

/* --- Task related routes --- */

App.TaskRoute = App.BaseRoute.extend({
  model: function(params) {
    return this.store.find('task', params.task_id);
  },
  afterModel: function(model) {
    return this.controllerFor('task').loadAdditional(model);
  },
  setupController: setupControllerFactory('Task: %@', 'id')
});

App.TasksRoute = App.BaseRoute.extend({
  setupController: setupControllerFactory()
});

/* --- Vertex related routes --- */

App.VertexRoute = App.BaseRoute.extend({
  model: function(params) {
    return this.store.find('vertex', params.vertex_id);
  },
  afterModel: function(model) {
    return this.controllerFor('vertex').loadAdditional(model);
  },
  setupController: setupControllerFactory('Vertex: %@ (%@)', 'name', 'id')
});

App.VertexAdditionalsRoute = App.BaseRoute.extend({
  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadEntities();
  }
});

App.InputRoute = App.BaseRoute.extend({
  model: function (params) {
    var model = this.modelFor('vertex');
    return model.get('inputs').findBy('id', params.input_id);
  },
  setupController: setupControllerFactory()
});

App.OutputRoute = App.BaseRoute.extend({
  model: function (params) {
    var model = this.modelFor('vertex');
    return model.get('outputs').findBy('id', params.input_id);
  },
  setupController: setupControllerFactory()
});

App.VertexSwimlaneRoute = App.BaseRoute.extend({
  renderTemplate: renderSwimlanes,
  model: function(params) {
    var model = this.modelFor('vertex'),
        queryParams = {'primaryFilter': 'TEZ_VERTEX_ID:' + model.id };
    this.store.unloadAll('task_attempt');
    return this.store.find('task_attempt', queryParams);
  },
  setupController: setupControllerFactory()
});

/* --- Task Attempt related routes--- */

App.TaskAttemptRoute = App.BaseRoute.extend({
  model: function(params) {
    return this.store.find('task_attempt', params.task_attempt_id);
  },
  afterModel: function(model) {
    return this.controllerFor('task_attempt').loadAdditional(model);
  },
  setupController: setupControllerFactory('Task Attempt: %@', 'id')
});

App.TaskAttemptsRoute = App.BaseRoute.extend({
  renderTemplate: renderTable,
  setupController: setupControllerFactory('Task Attempt: %@', 'id')
});

/* --- Tez-app related routes --- */

App.TezAppRoute = App.BaseRoute.extend({
  model: function(params) {
    var store = this.store;
    return store.find('tezApp', 'tez_' + params.app_id).then(function (tezApp){
      if(!tezApp.get('appId')) return tezApp;
      return App.Helpers.misc.loadApp(store, tezApp.get('appId')).then(function (appDetails){
        tezApp.set('appDetail', appDetails);
        return tezApp;
      }).catch(function() {
        return tezApp;
      });
    });
  },
  setupController: setupControllerFactory('Application: %@', 'id')
});

App.TezAppIndexRoute = App.BaseRoute.extend({
  setupController: setupControllerFactory()
});

App.TezAppDagsRoute = App.BaseRoute.extend({
  renderTemplate: renderTable,
  setupController: setupControllerFactory()
});

App.TezAppConfigsRoute = App.BaseRoute.extend({
  renderTemplate: renderConfigs
});

/* --- Shared routes --- */
App.DagIndexRoute = App.BaseRoute.extend({
  setupController: setupControllerFactory()
});

App.DagTasksRoute =
    App.DagVerticesRoute =
    App.DagTaskAttemptsRoute =
    App.VertexTasksRoute =
    App.VertexTaskAttemptsRoute =
    App.BaseRoute.extend({
      renderTemplate: renderTable,
      setupController: setupControllerFactory()
    });

App.DagCountersRoute =
    App.VertexCountersRoute =
    App.TaskCountersRoute =
    App.TaskAttemptCountersRoute =
    App.BaseRoute.extend({
      renderTemplate: function() {
        this.render('common/counters');
      }
    });
