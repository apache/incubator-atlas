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

define(['require',
    'backbone',
    'hbs!tmpl/business_catalog/MoveTermLayoutView_tmpl',
    'utils/Utils'
], function(require, Backbone, MoveTermLayoutViewTmpl, Utils) {
    'use strict';

    var MoveTermLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends MoveTermLayoutView */
        {
            _viewName: 'MoveTermLayoutView',

            template: MoveTermLayoutViewTmpl,
            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                moveTree: '[data-id="moveTree"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                return events;
            },
            /**
             * intialize a new MoveTermLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'taxanomyCollection'));
            },
            bindEvents: function() {},
            onRender: function() {}
        });
    return MoveTermLayoutView;
});
