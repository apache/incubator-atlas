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
    'hbs!tmpl/tag/TagAttributeDetailLayoutView_tmpl',
    'utils/Utils',
    'views/tag/AddTagAttributeView',
    'collection/VTagList',
    'models/VTag',
    'utils/Messages',
    'utils/UrlLinks'
], function(require, Backbone, TagAttributeDetailLayoutViewTmpl, Utils, AddTagAttributeView, VTagList, VTag, Messages, UrlLinks) {
    'use strict';

    var TagAttributeDetailLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends TagAttributeDetailLayoutView */
        {
            template: TagAttributeDetailLayoutViewTmpl,
            /** Layout sub regions */
            regions: {},
            /** ui selector cache */
            ui: {
                title: '[data-id="title"]',
                editButton: '[data-id="editButton"]',
                editBox: '[data-id="editBox"]',
                saveButton: "[data-id='saveButton']",
                showAttribute: "[data-id='showAttribute']",
                addTagListBtn: '[data-id="addTagListBtn"]',
                addTagPlus: '[data-id="addTagPlus"]',
                addTagBtn: '[data-id="addTagBtn"]',
                description: '[data-id="description"]',
                publishButton: '[data-id="publishButton"]',
                showSuperType: "[data-id='showSuperType']"
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTagListBtn] = 'onClickAddTagAttributeBtn';
                events["click " + this.ui.editButton] = 'onEditButton';
                return events;
            },
            /**
             * intialize a new TagAttributeDetailLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'tag', 'collection'));
            },
            bindEvents: function() {
                this.listenTo(this.collection, 'reset', function() {
                    if (!this.model) {
                        this.model = this.collection.fullCollection.findWhere({ name: this.tag });
                        if (this.model) {
                            this.renderTagDetail();
                        } else {
                            this.$('.fontLoader').hide();
                            Utils.notifyError({
                                content: 'Tag Not Found'
                            });
                        }
                    }
                }, this);
                this.listenTo(this.tagCollection, 'error', function(error, response) {
                    if (response.responseJSON && response.responseJSON.error) {
                        Utils.notifyError({
                            content: response.responseJSON.error
                        });
                    } else {
                        Utils.notifyError({
                            content: 'Something went wrong'
                        });
                    }
                    this.$('.fontLoader').hide();
                }, this);
            },
            onRender: function() {
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                if (this.collection.models.length && !this.model) {
                    this.model = this.collection.fullCollection.findWhere({ name: this.tag });
                    this.renderTagDetail();
                }
                this.bindEvents();
                this.ui.saveButton.attr("disabled", "true");
                this.ui.publishButton.prop('disabled', true);
            },
            renderTagDetail: function() {
                var attributeData = "",
                    supertypeData = "",
                    attributeDefs = this.model.get("attributeDefs"),
                    superTypeArr = this.model.get('superTypes');
                this.ui.title.html('<span>' + (Utils.getName(this.model.toJSON())) + '</span>');
                if (this.model.get("description")) {
                    this.ui.description.text(this.model.get("description"));
                }
                if (attributeDefs) {
                    if (!_.isArray(attributeDefs)) {
                        attributeDefs = [attributeDefs];
                    }
                    _.each(attributeDefs, function(value, key) {
                        attributeData += '<span class="inputAttribute">' + (Utils.getName(value)) + '</span>';
                    });
                    this.ui.showAttribute.html(attributeData);
                }
                if (superTypeArr.length > 0) {
                    this.$(".superType").show();
                    _.each(superTypeArr, function(value, key) {
                        supertypeData += ' <a class="inputAttribute" href="#!/tag/tagAttribute/' + value + '">' + value + '</a>';
                    });
                    this.ui.showSuperType.html(supertypeData);
                }
                Utils.hideTitleLoader(this.$('.fontLoader'), this.$('.tagDetail'));
            },
            onSaveButton: function(saveObject, message) {
                var that = this;
                var validate = true;

                this.modal.$el.find(".attributeInput").each(function() {
                    if ($(this).val() === "") {
                        $(this).css('borderColor', "red")
                        validate = false;
                    }
                });

                this.modal.$el.find(".attributeInput").keyup(function() {
                    $(this).css('borderColor', "#e8e9ee");
                });
                if (!validate) {
                    Utils.notifyInfo({
                        content: "Please fill the attributes or delete the input box"
                    });
                    return;
                }
                Utils.showTitleLoader(this.$('.page-title .fontLoader'), this.$('.tagDetail'));
                this.model.saveTagAttribute({
                    data: JSON.stringify({
                        classificationDefs: [saveObject],
                        entityDefs: [],
                        enumDefs: [],
                        structDefs: []

                    }),
                    success: function(model, response) {
                        if (model.classificationDefs) {
                            that.model.set(model.classificationDefs[0]);
                        }
                        that.renderTagDetail();
                        Utils.notifySuccess({
                            content: message
                        });
                    },
                    cust_error: function() {
                        Utils.hideTitleLoader(that.$('.fontLoader'), that.$('.tagDetail'));
                    }
                });
                that.modal.close();
            },
            onClickAddTagAttributeBtn: function(e) {
                var that = this;
                require(['views/tag/AddTagAttributeView',
                        'modules/Modal'
                    ],
                    function(AddTagAttributeView, Modal) {
                        var view = new AddTagAttributeView();
                        that.modal = new Modal({
                            title: 'Add Attribute',
                            content: view,
                            cancelText: "Cancel",
                            okText: 'Add',
                            allowCancel: true,
                            okCloses: false
                        }).open();
                        that.modal.$el.find('button.ok').attr("disabled", "true");
                        view.ui.addAttributeDiv.on('keyup', '.attributeInput', function(e) {
                            if (e.target.value.trim() == "") {
                                that.modal.$el.find('button.ok').attr("disabled", "disabled");
                            } else {
                                that.modal.$el.find('button.ok').removeAttr("disabled");
                            }
                        });
                        that.modal.on('ok', function() {
                            var newAttributeList = view.collection.toJSON(),
                                activeTagAttribute = _.extend([], that.model.get('attributeDefs')),
                                superTypes = that.model.get('superTypes');

                            _.each(superTypes, function(name) {
                                var parentTags = that.collection.fullCollection.findWhere({ name: name });
                                activeTagAttribute = activeTagAttribute.concat(parentTags.get('attributeDefs'));
                            });

                            var duplicateAttributeList = [],
                                saveObj = $.extend(true, {}, that.model.toJSON());
                            _.each(newAttributeList, function(obj) {
                                var duplicateCheck = _.find(activeTagAttribute, function(activeTagObj) {
                                    return activeTagObj.name.toLowerCase() === obj.name.toLowerCase();
                                });
                                if (duplicateCheck) {
                                    duplicateAttributeList.push(obj.name);
                                } else {
                                    saveObj.attributeDefs.push(obj);
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
                            if (saveObj && !duplicateAttributeList.length) {
                                that.onSaveButton(saveObj, Messages.addAttributeSuccessMessage);
                            } else {
                                if (duplicateAttributeList.length < 2) {
                                    var text = "Attribute <b>" + duplicateAttributeList.join(",") + "</b> is duplicate !"
                                } else {
                                    if (newAttributeList.length > duplicateAttributeList.length) {
                                        var text = "Attributes: <b>" + duplicateAttributeList.join(",") + "</b> are duplicate ! Do you want to continue with other attributes ?"
                                        notifyObj = {
                                            ok: function(argument) {
                                                that.onSaveButton(saveObj, Messages.addAttributeSuccessMessage);
                                            },
                                            cancel: function(argument) {}
                                        }
                                    } else {
                                        var text = "All attributes are duplicate !"
                                    }
                                }
                                notifyObj['text'] = text;
                                Utils.notifyConfirm(notifyObj);
                            }
                        });
                        that.modal.on('closeModal', function() {
                            that.modal.trigger('cancel');
                        });
                    });
            },
            textAreaChangeEvent: function(view) {
                if (this.model.get('description') === view.ui.description.val()) {
                    this.modal.$el.find('button.ok').prop('disabled', true);
                } else {
                    this.modal.$el.find('button.ok').prop('disabled', false);
                }
            },
            onPublishClick: function(view) {
                var saveObj = _.extend(this.model.toJSON(), { 'description': view.ui.description.val() });
                this.onSaveButton(saveObj, Messages.updateTagDescriptionMessage);
                this.ui.description.show();
            },
            onEditButton: function(e) {
                var that = this;
                $(e.currentTarget).blur();
                require([
                    'views/tag/CreateTagLayoutView',
                    'modules/Modal'
                ], function(CreateTagLayoutView, Modal) {
                    var view = new CreateTagLayoutView({ 'tagCollection': that.collection, 'model': that.model, 'tag': that.tag });
                    that.modal = new Modal({
                        title: 'Edit Tag',
                        content: view,
                        cancelText: "Cancel",
                        okText: 'Save',
                        allowCancel: true,
                    }).open();
                    view.ui.description.on('keyup', function(e) {
                        that.textAreaChangeEvent(view);
                    });
                    that.modal.$el.find('button.ok').prop('disabled', true);
                    that.modal.on('ok', function() {
                        that.onPublishClick(view);
                    });
                    that.modal.on('closeModal', function() {
                        that.modal.trigger('cancel');
                    });
                });
            }
        });
    return TagAttributeDetailLayoutView;
});
