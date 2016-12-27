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
    'hbs!tmpl/business_catalog/SideNavLayoutView_tmpl',
    'utils/Utils',
    'utils/Globals',
], function(require, tmpl, Utils, Globals) {
    'use strict';

    var SideNavLayoutView = Marionette.LayoutView.extend({
        template: tmpl,

        regions: {
            RBusinessCatalogLayoutView: "#r_businessCatalogLayoutView",
            RTagLayoutView: "#r_tagLayoutView",
            RSearchLayoutView: "#r_searchLayoutView",
        },
        ui: {
            tabs: '.tabs li a',
        },
        templateHelpers: function() {
            return {
                taxonomy: Globals.taxonomy,
                tabClass: this.tabClass
            };
        },
        events: function() {
            var events = {},
                that = this;
            events["click " + this.ui.tabs] = function(e) {
                var urlString = "",
                    elementName = $(e.currentTarget).data();
                var tabStateUrls = Globals.saveApplicationState.tabState;
                if (elementName.name == "tab-tag") {
                    urlString = tabStateUrls.tagUrl; //'#!/tag';
                } else if (elementName.name == "tab-taxonomy") {
                    urlString = tabStateUrls.taxonomyUrl; // '#!/taxonomy';
                } else if (elementName.name == "tab-search") {
                    urlString = tabStateUrls.searchUrl; // '#!/search';
                }
                Utils.setUrl({
                    url: urlString,
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: function() {
                        return { stateChanged: true };
                    },
                });
            };
            return events;
        },
        initialize: function(options) {
            _.extend(this, _.pick(options, 'globalVent', 'url', 'value', 'tag', 'selectFirst','collection'));
            if (Globals.taxonomy) {
                this.tabClass = "tab col-sm-4";
            } else {
                this.tabClass = "tab col-sm-6";
            }
        },
        onRender: function() {
            this.bindEvent();
            this.renderTagLayoutView();
            this.renderSearchLayoutView();
            if (Globals.taxonomy) {
                this.rendeBusinessCatalogLayoutView();
            }
            this.selectTab();

        },
        bindEvent: function() {},
        rendeBusinessCatalogLayoutView: function() {
            var that = this;
            require(['views/business_catalog/BusinessCatalogLayoutView'], function(BusinessCatalogLayoutView) {
                that.RBusinessCatalogLayoutView.show(new BusinessCatalogLayoutView({
                    globalVent: that.globalVent,
                    url: that.url
                }));
            });
        },
        renderTagLayoutView: function() {
            var that = this;
            require(['views/tag/TagLayoutView'], function(TagLayoutView) {
                that.RTagLayoutView.show(new TagLayoutView({
                    globalVent: that.globalVent,
                    collection: that.collection,
                    tag: that.tag
                }));
            });
        },
        renderSearchLayoutView: function() {
            var that = this;
            require(['views/search/SearchLayoutView'], function(SearchLayoutView) {
                that.RSearchLayoutView.show(new SearchLayoutView({
                    globalVent: that.globalVent,
                    vent: that.vent,
                    value: that.value
                }));
            });
        },
        selectTab: function() {
            if (Utils.getUrlState.isTagTab() || (Utils.getUrlState.isInitial() && !Globals.taxonomy)) {
                this.$('.tabs').find('li a[aria-controls="tab-tag"]').parents('li').addClass('active').siblings().removeClass('active');
                this.$('.tab-content').find('div#tab-tag').addClass('active').siblings().removeClass('active');
            } else if (Utils.getUrlState.isTaxonomyTab() || (Utils.getUrlState.isInitial() && Globals.taxonomy)) {
                this.$('.tabs').find('li a[aria-controls="tab-taxonomy"]').parents('li').addClass('active').siblings().removeClass('active');
                this.$('.tab-content').find('div#tab-taxonomy').addClass('active').siblings().removeClass('active');
            } else if (Utils.getUrlState.isSearchTab() || (Utils.getUrlState.isDetailPage())) {
                this.$('.tabs').find('li a[aria-controls="tab-search"]').parents('li').addClass('active').siblings().removeClass('active');
                this.$('.tab-content').find('div#tab-search').addClass('active').siblings().removeClass('active');
            }
        },
    });
    return SideNavLayoutView;
});
