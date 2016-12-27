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

    var Messages = {
        addSuccessMessage: " has been created successfully",
        addErrorMessage: " could not be Created",
        addTermToEntitySuccessMessage: " has been added to entity",
        deleteTerm: "Delete Term",
        removeTag: "Remove Tag Assignment",
        removeTerm: "Remove Term Assignment",
        deleteSuccessMessage: " has been deleted successfully",
        deleteErrorMessage: " could not be deleted",
        removeSuccessMessage: " has been removed successfully",
        removeErrorMessage: " could not be removed",
        addAttributeSuccessMessage: "Tag attribute is added successfully",
        updateTagDescriptionMessage: "Tag description is updated successfully",
        updateTermDescriptionMessage: "Term description is updated successfully",
        editSuccessMessage: " has been updated successfully"
    };
    return Messages;
});
