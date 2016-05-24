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

package org.apache.atlas.falcon.hook;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.atlas.falcon.bridge.FalconBridge;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.falcon.atlas.event.FalconEvent;
import org.apache.falcon.atlas.publisher.FalconEventPublisher;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Falcon hook sends lineage information to the Atlas Service.
 */
public class FalconHook extends AtlasHook implements FalconEventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(FalconHook.class);

    public static final String CONF_PREFIX = "atlas.hook.falcon.";
    private static final String MIN_THREADS = CONF_PREFIX + "minThreads";
    private static final String MAX_THREADS = CONF_PREFIX + "maxThreads";
    private static final String KEEP_ALIVE_TIME = CONF_PREFIX + "keepAliveTime";
    public static final String QUEUE_SIZE = CONF_PREFIX + "queueSize";
    public static final String CONF_SYNC = CONF_PREFIX + "synchronous";

    public static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    // wait time determines how long we wait before we exit the jvm on
    // shutdown. Pending requests after that will not be sent.
    private static final int WAIT_TIME = 3;
    private static ExecutorService executor;

    private static final int minThreadsDefault = 5;
    private static final int maxThreadsDefault = 5;
    private static final long keepAliveTimeDefault = 10;
    private static final int queueSizeDefault = 10000;

    private static boolean sync;

    private static ConfigurationStore STORE;

    private static enum Operation {
        ADD,
        UPDATE,
    }

    private List<HookNotification.HookNotificationMessage> messages = new ArrayList<>();

    static {
        try {
            // initialize the async facility to process hook calls. We don't
            // want to do this inline since it adds plenty of overhead for the query.
            int minThreads = atlasProperties.getInt(MIN_THREADS, minThreadsDefault);
            int maxThreads = atlasProperties.getInt(MAX_THREADS, maxThreadsDefault);
            long keepAliveTime = atlasProperties.getLong(KEEP_ALIVE_TIME, keepAliveTimeDefault);
            int queueSize = atlasProperties.getInt(QUEUE_SIZE, queueSizeDefault);
            sync = atlasProperties.getBoolean(CONF_SYNC, false);

            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTime, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(queueSize),
                    new ThreadFactoryBuilder().setNameFormat("Atlas Logger %d").build());

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        executor.shutdown();
                        executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
                        executor = null;
                    } catch (InterruptedException ie) {
                        LOG.info("Interrupt received in shutdown.");
                    }
                    // shutdown client
                }
            });

            STORE = ConfigurationStore.get();
        } catch (Exception e) {
            LOG.info("Caught exception initializing the falcon hook.", e);
        }

        Injector injector = Guice.createInjector(new NotificationModule());
        notifInterface = injector.getInstance(NotificationInterface.class);

        LOG.info("Created Atlas Hook for Falcon");
    }

    @Override
    public void publish(final Data data) throws Exception {
        final FalconEvent event = data.getEvent();
        if (sync) {
            fireAndForget(event);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        fireAndForget(event);
                    } catch (Throwable e) {
                        LOG.info("Atlas hook failed", e);
                    }
                }
            });
        }
    }

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    private void fireAndForget(FalconEvent event) throws Exception {
        LOG.info("Entered Atlas hook for Falcon hook operation {}", event.getOperation());

        Operation op = getOperation(event.getOperation());

        switch (op) {
            case ADD:
                LOG.info("fireAndForget user:{}, ugi: {}", event.getUser(), event.getUgi());
                messages.add(new HookNotification.EntityCreateRequest(getAuthenticatedUser(event.getUser()), createEntities(event)));
                break;

            case UPDATE:
                LOG.info("fireAndForget user:{}, ugi: {}", event.getUser(), event.getUgi());
                messages.add(new HookNotification.EntityUpdateRequest(getAuthenticatedUser(event.getUser()), createEntities(event)));
                break;
        }
        notifyEntities(messages);
    }

    private String getAuthenticatedUser(final String user) {
        return getUser(user, null);
    }

    private List<Referenceable> createEntities(FalconEvent event) throws Exception {
        List<Referenceable> entities = new ArrayList<>();

        switch (event.getOperation()) {
            case ADD_CLUSTER:
            case UPDATE_CLUSTER:
                entities.addAll(FalconBridge.createClusterEntity((org.apache.falcon.entity.v0.cluster.Cluster) event.getEntity(), event.getUser(),
                        event.getTimestamp()));
                break;

            case ADD_PROCESS:
            case UPDATE_PROCESS:
                entities.addAll(FalconBridge.createProcessEntity((Process) event.getEntity(), STORE,
                        event.getUser(), event.getTimestamp()));
                break;

            case ADD_FEED:
            case UPDATE_FEED:
                entities.addAll(FalconBridge.createFeedEntity((Feed) event.getEntity(), STORE,
                        event.getUser(), event.getTimestamp()));
                break;

            case ADD_REPLICATION_FEED:
            case UPDATE_REPLICATION_FEED:
                entities.addAll(FalconBridge.createReplicationFeedEntity((Feed) event.getEntity(), STORE,
                        event.getUser(), event.getTimestamp()));
                break;

            default:
                throw new Exception("Falcon operation " + event.getOperation() + " is not valid or supported");
        }

        return entities;
    }

    private static Operation getOperation(final FalconEvent.OPERATION op) throws Exception {
        switch (op) {
            case ADD_CLUSTER:
            case ADD_FEED:
            case ADD_PROCESS:
            case ADD_REPLICATION_FEED:
                return Operation.ADD;

            case UPDATE_CLUSTER:
            case UPDATE_FEED:
            case UPDATE_PROCESS:
            case UPDATE_REPLICATION_FEED:
                return Operation.UPDATE;

            default:
                throw new Exception("Falcon operation " + op + " is not valid or supported");
        }
    }
}
