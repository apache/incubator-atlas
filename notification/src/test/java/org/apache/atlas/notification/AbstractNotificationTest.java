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

package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.commons.configuration.Configuration;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * AbstractNotification tests.
 */
public class AbstractNotificationTest {

    @Test
    public void testSend() throws Exception {
        Configuration configuration = mock(Configuration.class);

        TestNotification notification = new TestNotification(configuration);

        TestMessage message1 = new TestMessage(HookNotification.HookNotificationType.ENTITY_CREATE, "user1");
        TestMessage message2 = new TestMessage(HookNotification.HookNotificationType.TYPE_CREATE, "user1");
        TestMessage message3 = new TestMessage(HookNotification.HookNotificationType.ENTITY_FULL_UPDATE, "user1");

        String messageJson1 = AbstractNotification.getMessageJson(message1);
        String messageJson2 = AbstractNotification.getMessageJson(message2);
        String messageJson3 = AbstractNotification.getMessageJson(message3);

        notification.send(NotificationInterface.NotificationType.HOOK, message1, message2, message3);

        assertEquals(NotificationInterface.NotificationType.HOOK, notification.type);
        assertEquals(3, notification.messages.length);
        assertEquals(messageJson1, notification.messages[0]);
        assertEquals(messageJson2, notification.messages[1]);
        assertEquals(messageJson3, notification.messages[2]);
    }

    @Test
    public void testSend2() throws Exception {
        Configuration configuration = mock(Configuration.class);

        TestNotification notification = new TestNotification(configuration);

        TestMessage message1 = new TestMessage(HookNotification.HookNotificationType.ENTITY_CREATE, "user1");
        TestMessage message2 = new TestMessage(HookNotification.HookNotificationType.TYPE_CREATE, "user1");
        TestMessage message3 = new TestMessage(HookNotification.HookNotificationType.ENTITY_FULL_UPDATE, "user1");

        List<TestMessage> messages = new LinkedList<>();
        messages.add(message1);
        messages.add(message2);
        messages.add(message3);

        String messageJson1 = AbstractNotification.getMessageJson(message1);
        String messageJson2 = AbstractNotification.getMessageJson(message2);
        String messageJson3 = AbstractNotification.getMessageJson(message3);

        notification.send(NotificationInterface.NotificationType.HOOK, messages);

        assertEquals(NotificationInterface.NotificationType.HOOK, notification.type);
        assertEquals(3, notification.messages.length);
        assertEquals(messageJson1, notification.messages[0]);
        assertEquals(messageJson2, notification.messages[1]);
        assertEquals(messageJson3, notification.messages[2]);
    }

    public static class TestMessage extends HookNotification.HookNotificationMessage {

        public TestMessage(HookNotification.HookNotificationType type, String user) {
            super(type, user);
        }
    }

    public static class TestNotification extends AbstractNotification {
        private NotificationType type;
        private String[] messages;

        public TestNotification(Configuration applicationProperties) throws AtlasException {
            super(applicationProperties);
        }

        @Override
        protected void sendInternal(NotificationType notificationType, String[] notificationMessages)
            throws NotificationException {

            type = notificationType;
            messages = notificationMessages;
        }

        @Override
        public <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType, int numConsumers) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
