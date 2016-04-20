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
    'hbs!tmpl/tag/addTagAttributeItemView_tmpl',

], function(require, Backbone, addTagAttributeItemViewTmpl) {
    'use strict';

    return Backbone.Marionette.ItemView.extend(
        /** @lends GlobalExclusionListView */
        {

            template: addTagAttributeItemViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: {
                'click #close': 'onCloseButton',
                'change input': function(e) {
                    this.saveBtn.removeAttr("disabled");
                    this.model.set({ name: e.currentTarget.value });
                },
                'keypress #attributeId': function(e) {
                    if (this.$(e.currentTarget).val() == "") {
                        this.saveBtn.removeAttr("disabled");
                    }
                },
                'keyup #attributeId': function(e) {
                    if (e.keyCode == 8 && this.$(e.currentTarget).val() == "") {
                        this.saveBtn.attr("disabled", "true");
                    }
                },
            },
            /**
             * intialize a new GlobalExclusionComponentView Layout
             * @constructs
             */
            initialize: function(options) {
                this.saveBtn = options.saveBtn;
            },
            onRender: function() {},
            bindEvents: function() {},
            onCloseButton: function() {
                this.model.destroy();
                this.saveBtn.removeAttr("disabled");
            }
        });
});
