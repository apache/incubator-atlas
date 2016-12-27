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

package org.apache.atlas.authorize;


public interface AtlasAuthorizer {


    /**
     * This method will load the policy file and would initialize the required data-structures.
     */
    void init();

    /**
     * This method is responsible to perform the actual authorization for every REST API call. It will check if
     * user can perform action on resource.
     */
    boolean isAccessAllowed(AtlasAccessRequest request) throws AtlasAuthorizationException;

    /**
     * This method is responsible to perform the cleanup and release activities. It must be called when you are done
     * with the Authorization activity and once it's called a restart would be required. Try to invoke this while
     * destroying the context.
     */
    void cleanUp();
}
