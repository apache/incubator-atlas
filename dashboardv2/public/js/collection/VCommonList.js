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
    'utils/Globals',
    'collection/BaseCollection',
    'models/VCommon',
    'utils/UrlLinks'
], function(require, Globals, BaseCollection, VCommon, UrlLinks) {
    'use strict';
    var VCommonList = BaseCollection.extend(
        //Prototypal attributes
        {
            url: UrlLinks.baseURL + '',

            model: VCommon,

            initialize: function() {
                this.modelName = 'VCommon';
                this.modelAttrName = '';
                this.bindErrorEvents();
            },
        },
        //Static Class Members
        {
            /**
             * Table Cols to be passed to Backgrid
             * UI has to use this as base and extend this.
             *
             */
            tableCols: {}
        }
    );
    return VCommonList;
});
