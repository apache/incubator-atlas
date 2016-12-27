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

import com.google.inject.Inject;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.web.resources.BaseResourceIT;
import org.codehaus.jettison.json.JSONArray;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

@Guice(modules = NotificationModule.class)
public class NotificationHookConsumerIT extends BaseResourceIT {

    private static final String TEST_USER = "testuser";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String CLUSTER_NAME = "clusterName";

    @Inject
    private NotificationInterface kafka;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        createTypeDefinitionsV1();
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.close();
    }

    private void sendHookMessage(HookNotification.HookNotificationMessage message) throws NotificationException {
        kafka.send(NotificationInterface.NotificationType.HOOK, message);
    }

    @Test
    public void testMessageHandleFailureConsumerContinues() throws Exception {
        //send invalid message - update with invalid type
        sendHookMessage(new HookNotification.EntityPartialUpdateRequest(TEST_USER, randomString(), null, null,
                new Referenceable(randomString())));

        //send valid message
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());
        sendHookMessage(new HookNotification.EntityCreateRequest(TEST_USER, entity));

        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = searchByDSL(String.format("%s where name='%s'", DATABASE_TYPE, entity.get(NAME)));
                return results.length() == 1;
            }
        });
    }

    @Test
    public void testCreateEntity() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());

        sendHookMessage(new HookNotification.EntityCreateRequest(TEST_USER, entity));

        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE, entity.get(QUALIFIED_NAME)));
                return results.length() == 1;
            }
        });

        //Assert that user passed in hook message is used in audit
        Referenceable instance = atlasClientV1.getEntity(DATABASE_TYPE, QUALIFIED_NAME, (String) entity.get(QUALIFIED_NAME));
        List<EntityAuditEvent> events =
                atlasClientV1.getEntityAuditEvents(instance.getId()._getId(), (short) 1);
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).getUser(), TEST_USER);
    }

    @Test
    public void testUpdateEntityPartial() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());

        atlasClientV1.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        newEntity.set("owner", randomString());
        sendHookMessage(
                new HookNotification.EntityPartialUpdateRequest(TEST_USER, DATABASE_TYPE, QUALIFIED_NAME, dbName, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                Referenceable localEntity = atlasClientV1.getEntity(DATABASE_TYPE, QUALIFIED_NAME, dbName);
                return (localEntity.get("owner") != null && localEntity.get("owner").equals(newEntity.get("owner")));
            }
        });

        //Its partial update and un-set fields are not updated
        Referenceable actualEntity = atlasClientV1.getEntity(DATABASE_TYPE, QUALIFIED_NAME, dbName);
        assertEquals(actualEntity.get(DESCRIPTION), entity.get(DESCRIPTION));
    }

    @Test
    public void testUpdatePartialUpdatingQualifiedName() throws Exception {
        final Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());

        atlasClientV1.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        final String newName = "db" + randomString();
        newEntity.set(QUALIFIED_NAME, newName);

        sendHookMessage(
                new HookNotification.EntityPartialUpdateRequest(TEST_USER, DATABASE_TYPE, QUALIFIED_NAME, dbName, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE, newName));
                return results.length() == 1;
            }
        });

        //no entity with the old qualified name
        JSONArray results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE, dbName));
        assertEquals(results.length(), 0);

    }

    @Test
    public void testDeleteByQualifiedName() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());

        final String dbId = atlasClientV1.createEntity(entity).get(0);

        sendHookMessage(
            new HookNotification.EntityDeleteRequest(TEST_USER, DATABASE_TYPE, QUALIFIED_NAME, dbName));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                Referenceable getEntity = atlasClientV1.getEntity(dbId);
                return getEntity.getId().getState() == Id.EntityState.DELETED;
            }
        });
    }

    @Test
    public void testUpdateEntityFullUpdate() throws Exception {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        final String dbName = "db" + randomString();
        entity.set(NAME, dbName);
        entity.set(DESCRIPTION, randomString());
        entity.set(QUALIFIED_NAME, dbName);
        entity.set(CLUSTER_NAME, randomString());

        atlasClientV1.createEntity(entity);

        final Referenceable newEntity = new Referenceable(DATABASE_TYPE);
        newEntity.set(NAME, randomString());
        newEntity.set(DESCRIPTION, randomString());
        newEntity.set("owner", randomString());
        newEntity.set(QUALIFIED_NAME, dbName);
        newEntity.set(CLUSTER_NAME, randomString());

        //updating unique attribute
        sendHookMessage(new HookNotification.EntityUpdateRequest(TEST_USER, newEntity));
        waitFor(MAX_WAIT_TIME, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                JSONArray results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE, newEntity.get(QUALIFIED_NAME)));
                return results.length() == 1;
            }
        });

        Referenceable actualEntity = atlasClientV1.getEntity(DATABASE_TYPE, QUALIFIED_NAME, dbName);
        assertEquals(actualEntity.get(DESCRIPTION), newEntity.get(DESCRIPTION));
        assertEquals(actualEntity.get("owner"), newEntity.get("owner"));
    }


}
