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

App.TablePageController = App.PollingController.extend(
    App.DataArrayLoaderMixin,
    App.ColumnSelectorMixin, {
      queryParams: ['pageNum', 'rowCount', 'searchText', 'sortColumnId', 'sortOrder'],

      sortColumnId: '',
      sortOrder: '',

      pageNum: 1,
      rowCount: 25,

      searchText: '',
      rowsDisplayed: [],

      isRefreshable: true,

      parentStatus: null,

      rowsDisplayedObserver: function () {
        this.set('pollster.targetRecords', this.get('rowsDisplayed'));
      }.observes('rowsDisplayed', 'pollster'),

      parentStatusObserver: function () {
        var parentStatus = this.get('status'),
            previousStatus = this.get('parentStatus');

        if(parentStatus != previousStatus && previousStatus == 'RUNNING' && this.get('pollingEnabled')) {
          this.get('pollster').stop();
          this.loadData(true);
        }
        this.set('parentStatus', parentStatus);
      }.observes('status'),

      applicationComplete: function () {
        this.set('pollster.polledRecords', null);
        this.loadData(true);
      },

      statusMessage: function () {
        return this.get('loading') ? "Loading all records..." : null;
      }.property('loading'),

      onInProgressColumnSort: function (columnDef) {
        var inProgress = this.get('pollster.isRunning');
        if(inProgress) {
          App.Helpers.Dialogs.alert(
            'Cannot sort',
            'Sorting on %@ is disabled for running DAGs!'.fmt(columnDef.get('headerCellName')),
            this
          );
        }
        return !inProgress;
      },

      actions: {
        refresh: function () {
          this.loadData(true);
        },
        tableRowsChanged: function (rows) {
          this.set('rowsDisplayed', rows);
        }
      }
    }
);
