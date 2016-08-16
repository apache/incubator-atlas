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

define(['require'], function(require) {
    'use strict';

    var Globals = {};

    Globals.baseURL = '';
    Globals.settings = {};
    Globals.settings.PAGE_SIZE = 25;
    Globals.saveApplicationState = {
        mainPageState: {},
        tabState: {
            stateChanged: false,
            tagUrl: "#!/tag",
            taxonomyUrl: "#!/taxonomy",
            searchUrl: "#!/search"
        },
        detailPageState: {}
    };
    Globals.auditAction = {
        ENTITY_CREATE: "Entity Created",
        ENTITY_UPDATE: "Entity Updated",
        ENTITY_DELETE: "Entity Deleted",
        TAG_ADD: "Tag Added",
        TAG_DELETE: "Tag Deleted"
    }
    Globals.entityStateReadOnly = {
        ACTIVE: false,
        DELETED: true
    }
    Globals.userLogedIn = {
        status: false,
        response: {}
    }

    return Globals;
});
