/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;

/**
 * Abstract notification interface implementation.
 */
public abstract class AbstractNotification implements NotificationInterface {

    private static final String PROPERTY_EMBEDDED = PROPERTY_PREFIX + ".embedded";
    private final boolean embedded;


    // ----- Constructors ------------------------------------------------------

    public AbstractNotification(Configuration applicationProperties) throws AtlasException {
        this.embedded = applicationProperties.getBoolean(PROPERTY_EMBEDDED, false);
    }


    // ----- AbstractNotificationInterface -------------------------------------

    /**
     * Determine whether or not the notification service embedded in Atlas server.
     *
     * @return true if the the notification service embedded in Atlas server.
     */
    protected final boolean isEmbedded() {
        return embedded;
    }
}
