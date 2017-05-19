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
    'hbs!tmpl/tag/TagLayoutView_tmpl',
    'utils/Utils',
    'utils/Messages',
    'utils/Globals',
    'utils/UrlLinks',
    'models/VTag'
], function(require, Backbone, TagLayoutViewTmpl, Utils, Messages, Globals, UrlLinks, VTag) {
    'use strict';

    var TagLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagLayoutView */
        {
            _viewName: 'TagLayoutView',

            template: TagLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},

            /** ui selector cache */
            ui: {
                tagsParent: "[data-id='tagsParent']",
                createTag: "[data-id='createTag']",
                tags: "[data-id='tags']",
                offLineSearchTag: "[data-id='offlineSearchTag']",
                refreshTag: '[data-id="refreshTag"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.createTag] = 'onClickCreateTag';
                events["click " + this.ui.tags] = 'onTagList';
                events["keyup " + this.ui.offLineSearchTag] = 'offlineSearchTag';
                events['click ' + this.ui.refreshTag] = 'fetchCollections';
                return events;
            },
            /**
             * intialize a new TagLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'tag', 'collection', 'typeHeaders'));
            },
            bindEvents: function() {
                var that = this;
                this.listenTo(this.collection, "reset add remove", function() {
                    this.tagsAndTypeGenerator('collection');
                }, this);
                this.ui.tagsParent.on('click', 'li.parent-node a', function() {
                    that.setUrl(this.getAttribute("href"));
                });
            },
            onRender: function() {
                var that = this;
                this.bindEvents();
                this.fetchCollections();
                $('body').on("click", '.tagPopoverList li', function(e) {
                    that[$(this).find("a").data('fn')](e);
                });
                $('body').click(function(e) {
                    if ($('.tagPopoverList').length) {
                        if ($(e.target).hasClass('tagPopover')) {
                            return;
                        }
                        that.$('.tagPopover').popover('hide');
                    }
                });
            },
            fetchCollections: function() {
                this.collection.fetch({ reset: true });
                this.ui.offLineSearchTag.val("");
            },
            manualRender: function(tagName) {
                this.tag = tagName;
                if (!this.createTag) {
                    this.setValues(true);
                }
            },
            setValues: function(manual) {
                if (Utils.getUrlState.isTagTab()) {
                    if (!this.tag) {
                        this.selectFirst = false;
                        this.ui.tagsParent.find('li').first().addClass('active');
                        if (this.ui.tagsParent.find('li a').first().length) {
                            Utils.setUrl({
                                url: this.ui.tagsParent.find('li a').first().attr("href"),
                                mergeBrowserUrl: false,
                                trigger: true,
                                updateTabState: function() {
                                    return { tagUrl: this.url, stateChanged: true };
                                }
                            });
                        }
                    } else {
                        Utils.setUrl({
                            url: Utils.getUrlState.getQueryUrl().hash,
                            updateTabState: function() {
                                return { tagUrl: this.url, stateChanged: true };
                            }
                        });
                        var tag = Utils.getUrlState.getLastValue();
                        if (this.tag) {
                            tag = this.tag;
                        }
                        this.ui.tagsParent.find('li').removeClass('active');
                        var target = this.ui.tagsParent.find('li').filter(function() {
                            return $(this).text() === tag;
                        }).addClass('active');
                        if (this.createTag || !manual) {
                            if (target && target.offset()) {
                                $('#sidebar-wrapper').animate({
                                    scrollTop: target.offset().top - 100
                                }, 500);
                            }
                        }

                    }
                }
            },
            tagsAndTypeGenerator: function(collection, searchString) {
                var that = this,
                    str = '';
                that.collection.fullCollection.comparator = function(model) {
                    return Utils.getName(model.toJSON(), 'name').toLowerCase();
                };
                that.collection.fullCollection.sort().each(function(model) {
                    var name = Utils.getName(model.toJSON(), 'name');
                    var checkTagOrTerm = Utils.checkTagOrTerm(name);
                    if (checkTagOrTerm.tag) {
                        if (searchString) {
                            if (name.search(new RegExp(searchString, "i")) != -1) {
                                str += '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-ellipsis-h tagPopover"></i></div><a href="#!/tag/tagAttribute/' + name + '"  data-name="' + name + '" >' + name + '</a></li>';
                            } else {
                                return;
                            }
                        } else {
                            str += '<li class="parent-node" data-id="tags"><div class="tools"><i class="fa fa-ellipsis-h tagPopover"></i></div><a href="#!/tag/tagAttribute/' + name + '"  data-name="' + name + '">' + name + '</a></li>';
                        }
                    }
                });
                this.ui.tagsParent.empty().html(str);
                this.setValues();
                this.createTagAction();
                if (this.createTag) {
                    this.createTag = false;
                }
            },
            onClickCreateTag: function(e) {
                var that = this;
                $(e.currentTarget).attr("disabled", "true");
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'tagCollection': that.collection });
                    var modal = new Modal({
                        title: 'Create a new tag',
                        content: view,
                        cancelText: "Cancel",
                        okCloses: false,
                        okText: 'Create',
                        allowCancel: true,
                    }).open();
                    modal.$el.find('button.ok').attr("disabled", "true");
                    view.ui.tagName.on('keyup', function(e) {
                        modal.$el.find('button.ok').removeAttr("disabled");
                    });
                    view.ui.tagName.on('keyup', function(e) {
                        if ((e.keyCode == 8 || e.keyCode == 32 || e.keyCode == 46) && e.currentTarget.value.trim() == "") {
                            modal.$el.find('button.ok').attr("disabled", "true");
                        }
                    });
                    modal.on('shownModal', function() {
                        view.ui.parentTag.select2({
                            multiple: true,
                            placeholder: "Search Tags",
                            allowClear: true
                        });
                    });
                    modal.on('ok', function() {
                        modal.$el.find('button.ok').attr("disabled", "true");
                        that.onCreateButton(view, modal);
                    });
                    modal.on('closeModal', function() {
                        modal.trigger('cancel');
                        that.ui.createTag.removeAttr("disabled");
                    });
                });
            },
            onCreateButton: function(ref, modal) {
                var that = this;
                var validate = true;
                if (modal.$el.find(".attributeInput").length > 0) {
                    modal.$el.find(".attributeInput").each(function() {
                        if ($(this).val() === "") {
                            $(this).css('borderColor', "red")
                            validate = false;
                        }
                    });
                }
                modal.$el.find(".attributeInput").keyup(function() {
                    $(this).css('borderColor', "#e8e9ee");
                    modal.$el.find('button.ok').removeAttr("disabled");
                });
                if (!validate) {
                    Utils.notifyInfo({
                        content: "Please fill the attributes or delete the input box"
                    });
                    return;
                }

                this.name = ref.ui.tagName.val();
                this.description = ref.ui.description.val();
                var superTypes = [];
                if (ref.ui.parentTag.val() && ref.ui.parentTag.val()) {
                    superTypes = ref.ui.parentTag.val();
                }
                var attributeObj = ref.collection.toJSON();
                if (ref.collection.length === 1 && ref.collection.first().get("name") === "") {
                    attributeObj = [];
                }

                if (attributeObj.length) {
                    var superTypesAttributes = [];
                    _.each(superTypes, function(name) {
                        var parentTags = that.collection.fullCollection.findWhere({ name: name });
                        superTypesAttributes = superTypesAttributes.concat(parentTags.get('attributeDefs'));
                    });


                    var duplicateAttributeList = [];
                    _.each(attributeObj, function(obj) {
                        var duplicateCheck = _.find(superTypesAttributes, function(activeTagObj) {
                            return activeTagObj.name.toLowerCase() === obj.name.toLowerCase();
                        });
                        if (duplicateCheck) {
                            duplicateAttributeList.push(obj.name);
                        }
                    });
                    var notifyObj = {
                        modal: true,
                        confirm: {
                            confirm: true,
                            buttons: [{
                                    text: 'Ok',
                                    addClass: 'btn-primary',
                                    click: function(notice) {
                                        notice.remove();
                                    }
                                },
                                null
                            ]
                        }
                    }

                    if (duplicateAttributeList.length) {
                        if (duplicateAttributeList.length < 2) {
                            var text = "Attribute <b>" + duplicateAttributeList.join(",") + "</b> is duplicate !"
                        } else {
                            if (attributeObj.length > duplicateAttributeList.length) {
                                var text = "Attributes: <b>" + duplicateAttributeList.join(",") + "</b> are duplicate !"
                            } else {
                                var text = "All attributes are duplicate !"
                            }
                        }
                        notifyObj['text'] = text;
                        Utils.notifyConfirm(notifyObj);
                        return false;
                    }
                }
                this.json = {
                    classificationDefs: [{
                        'name': this.name.trim(),
                        'description': this.description.trim(),
                        'superTypes': superTypes.length ? superTypes : [],
                        "attributeDefs": attributeObj
                    }],
                    entityDefs: [],
                    enumDefs: [],
                    structDefs: []

                };
                new this.collection.model().set(this.json).save(null, {
                    success: function(model, response) {
                        that.ui.createTag.removeAttr("disabled");
                        that.createTag = true;
                        that.collection.add(model.get('classificationDefs'));
                        that.setUrl('#!/tag/tagAttribute/' + ref.ui.tagName.val(), true);
                        Utils.notifySuccess({
                            content: "Tag " + that.name + Messages.addSuccessMessage
                        });
                        modal.trigger('cancel');
                        that.typeHeaders.fetch({ reset: true });
                    }
                });
            },
            setUrl: function(url, create) {
                Utils.setUrl({
                    url: url,
                    mergeBrowserUrl: false,
                    trigger: true,
                    updateTabState: function() {
                        return { tagUrl: this.url, stateChanged: true };
                    }
                });
            },
            onTagList: function(e, toggle) {
                this.ui.tagsParent.find('li').removeClass("active");
                $(e.currentTarget).addClass("active");
            },
            offlineSearchTag: function(e) {
                var type = $(e.currentTarget).data('type');
                this.tagsAndTypeGenerator('collection', $(e.currentTarget).val());
            },
            createTagAction: function() {
                var that = this;
                this.$('.tagPopover').popover({
                    placement: 'bottom',
                    html: true,
                    trigger: 'manual',
                    container: 'body',
                    content: function() {
                        return "<ul class='tagPopoverList'>" +
                            "<li class='listTerm' ><i class='fa fa-search'></i> <a href='javascript:void(0)' data-fn='onSearchTag'>Search Tag</a></li>" +
                            "<li class='listTerm' ><i class='fa fa-trash-o'></i> <a href='javascript:void(0)' data-fn='onDeleteTag'>Delete Tag</a></li>" +
                            "</ul>";
                    }
                });
                this.$('.tagPopover').off('click').on('click', function(e) {
                    // if any other popovers are visible, hide them
                    e.preventDefault();
                    that.$('.tagPopover').not(this).popover('hide');
                    $(this).popover('toggle');
                });
            },
            onSearchTag: function() {
                Utils.setUrl({
                    url: '#!/search/searchResult',
                    urlParams: {
                        tag: this.ui.tagsParent.find('li.active').find("a").data('name'),
                        searchType: "basic",
                        dslChecked: false
                    },
                    updateTabState: function() {
                        return { searchUrl: this.url, stateChanged: true };
                    },
                    mergeBrowserUrl: false,
                    trigger: true
                });
            },
            onDeleteTag: function() {
                var that = this;
                this.tagName = this.ui.tagsParent.find('li.active').find("a").data('name');
                this.tagDeleteData = this.ui.tagsParent.find('li.active');
                var notifyObj = {
                    modal: true,
                    ok: function(argument) {
                        that.onNotifyOk();
                    },
                    cancel: function(argument) {}
                }
                var text = "Are you sure you want to delete the tag"
                notifyObj['text'] = text;
                Utils.notifyConfirm(notifyObj);
            },
            onNotifyOk: function(data) {
                var that = this,
                    deleteTagData = this.collection.fullCollection.findWhere({ name: this.tagName }),
                    classificationData = deleteTagData.toJSON(),
                    deleteJson = {
                        classificationDefs: [classificationData],
                        entityDefs: [],
                        enumDefs: [],
                        structDefs: []
                    };
                deleteTagData.deleteTag({
                    data: JSON.stringify(deleteJson),
                    success: function() {
                        Utils.notifySuccess({
                            content: "Tag " + that.tagName + Messages.deleteSuccessMessage
                        });
                        var searchUrl = Globals.saveApplicationState.tabState.searchUrl;
                        var urlObj = Utils.getUrlState.getQueryParams(searchUrl);
                        if (urlObj && urlObj.tag && urlObj.tag === that.tagName) {
                            Globals.saveApplicationState.tabState.searchUrl = "#!/search";
                        }
                        that.ui.tagsParent.find('li.active').removeClass('active');
                        if (that.tagDeleteData.prev().length === 0) {
                            that.tagDeleteData.next().addClass('active');
                        } else {
                            that.tagDeleteData.prev().addClass('active');
                        }
                        Utils.setUrl({
                            url: that.ui.tagsParent.find('li.active').find("a").attr("href"),
                            mergeBrowserUrl: false,
                            trigger: true,
                            updateTabState: function() {
                                return { tagUrl: that.url, stateChanged: true };
                            }
                        });
                        that.collection.remove(deleteTagData);
                        that.typeHeaders.fetch({ reset: true });
                    }
                });
            }
        });
    return TagLayoutView;
});
