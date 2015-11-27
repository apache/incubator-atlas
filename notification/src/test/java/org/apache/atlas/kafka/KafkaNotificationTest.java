/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.kafka;

import com.google.inject.Inject;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.codehaus.jettison.json.JSONArray;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = NotificationModule.class)
public class KafkaNotificationTest {

    @Inject
    private KafkaNotification kafka;

    @BeforeClass
    public void setUp() throws Exception {
        kafka.start();
    }

    @Test
    public void testSendReceiveMessage() throws Exception {
        String msg1 = "[{\"message\": " + 123 + "}]";
        String msg2 = "[{\"message\": " + 456 + "}]";
        kafka.send(NotificationInterface.NotificationType.HOOK, msg1, msg2);
        List<NotificationConsumer<JSONArray>> consumers =
                kafka.createConsumers(NotificationInterface.NotificationType.HOOK, 1);
        NotificationConsumer<JSONArray> consumer = consumers.get(0);
        assertTrue(consumer.hasNext());
        assertEquals(new JSONArray(msg1), consumer.next());
        assertTrue(consumer.hasNext());
        assertEquals(new JSONArray(msg2), consumer.next());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateConsumers() throws Exception {
        Configuration configuration = mock(Configuration.class);
        Iterator iterator = mock(Iterator.class);
        ConsumerConnector consumerConnector = mock(ConsumerConnector.class);
        KafkaStream kafkaStream1 = mock(KafkaStream.class);
        KafkaStream kafkaStream2 = mock(KafkaStream.class);
        String groupId = "groupId9999";

        when(configuration.subset(KafkaNotification.PROPERTY_PREFIX)).thenReturn(configuration);
        when(configuration.getKeys()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn("entities." + KafkaNotification.CONSUMER_GROUP_ID_PROPERTY);
        when(configuration.getList("entities." + KafkaNotification.CONSUMER_GROUP_ID_PROPERTY))
                .thenReturn(Collections.<Object>singletonList(groupId));

        Map<String, List<KafkaStream<String, String>>> streamsMap = new HashMap<>();
        List<KafkaStream<String, String>> kafkaStreamList = new LinkedList<>();
        kafkaStreamList.add(kafkaStream1);
        kafkaStreamList.add(kafkaStream2);
        streamsMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, kafkaStreamList);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(KafkaNotification.ATLAS_ENTITIES_TOPIC, 2);

        when(consumerConnector.createMessageStreams(
            eq(topicCountMap), any(StringDecoder.class), any(StringDecoder.class))).thenReturn(streamsMap);

        TestKafkaNotification kafkaNotification = new TestKafkaNotification(configuration, consumerConnector);

        List<NotificationConsumer<String>> consumers =
            kafkaNotification.createConsumers(NotificationInterface.NotificationType.ENTITIES, 2);

        assertEquals(2, consumers.size());

        // assert that all of the given kafka streams were used to create kafka consumers
        List<KafkaStream> streams = kafkaNotification.kafkaStreams;
        assertTrue(streams.contains(kafkaStream1));
        assertTrue(streams.contains(kafkaStream2));

        // assert that the given consumer group id was added to the properties used to create the consumer connector
        Properties properties = kafkaNotification.consumerProperties;
        assertEquals(groupId, properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(5);
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.stop();
    }

    // Extended kafka notification class for testing.
    private static class TestKafkaNotification extends KafkaNotification {

        private final ConsumerConnector consumerConnector;

        private Properties consumerProperties;
        private List<KafkaStream> kafkaStreams = new LinkedList<>();

        public TestKafkaNotification(Configuration applicationProperties,
                                     ConsumerConnector consumerConnector) throws AtlasException {
            super(applicationProperties);
            this.consumerConnector = consumerConnector;
        }

        @Override
        protected ConsumerConnector createConsumerConnector(Properties properties) {
            this.consumerProperties = properties;
            kafkaStreams.clear();
            return consumerConnector;
        }

        @Override
        protected <T> org.apache.atlas.kafka.KafkaConsumer<T> createKafkaConsumer(Class<T> type, KafkaStream stream,
                                                                                  int consumerId) {
            kafkaStreams.add(stream);
            return super.createKafkaConsumer(type, stream, consumerId);
        }
    }
}
