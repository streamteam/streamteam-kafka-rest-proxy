/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.consumer;

import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.DataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.KafkaRestProxy;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.properties.PropertyReadHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;

/**
 * Consumes the streams from Kafka.
 */
public class StreamConsumer implements Closeable, Runnable {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(StreamConsumer.class);

    /**
     * KafkaRestProxy
     */
    private final KafkaRestProxy kafkaRestProxy;

    /**
     * KafkaConsumer
     */
    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    /**
     * Poll timeout
     */
    private final long pollTimeout;

    /**
     * Interval in which the SubscriptionUpdater updates the subscriptions
     */
    private final long subscriptionInterval;

    /**
     * Flag that indicates if the StreamConsumer should continue polling new data stream elements or not
     */
    private boolean runFlag;

    /**
     * SubscriptionUpdater
     */
    private SubscriptionUpdater subscriptionUpdater;

    /**
     * StreamConsumer constructor.
     *
     * @param properties     Properties
     * @param kafkaRestProxy KafkaRestProxy
     */
    public StreamConsumer(Properties properties, KafkaRestProxy kafkaRestProxy) {
        logger.info("Initializing StreamConsumer");

        this.kafkaRestProxy = kafkaRestProxy;

        this.pollTimeout = PropertyReadHelper.readLongOrDie(properties, "kafka.pollTimeout");
        this.subscriptionInterval = PropertyReadHelper.readLongOrDie(properties, "kafka.subscriptionInterval");
        String brokerList = PropertyReadHelper.readStringOrDie(properties, "kafka.brokerList");
        String groupIdPrefix = PropertyReadHelper.readStringOrDie(properties, "kafka.groupIdPrefix");

        // https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupIdPrefix + "_" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.kafkaConsumer = new KafkaConsumer<>(props);

        this.subscriptionUpdater = new SubscriptionUpdater();
        Thread subscriptionUpdaterThread = new Thread(this.subscriptionUpdater);
        subscriptionUpdaterThread.start();

        this.runFlag = true;
    }

    /**
     * Continuously polls new data stream elements from the subscribed topics and adds them to the buffer.
     */
    public void run() {
        while (this.runFlag) {
            try {
                ConsumerRecords<String, byte[]> records;
                synchronized (this.kafkaConsumer) { // required for SubscriptionUpdater
                    records = this.kafkaConsumer.poll(this.pollTimeout);
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    DataStreamElement dataStreamElement = new DataStreamElement(record);
                    logger.debug("Consumed: {}", dataStreamElement);
                    this.kafkaRestProxy.addToBuffer(dataStreamElement);
                }
            } catch (WakeupException e) {
                logger.info("Poll interrupted with wakeup call.");
            } catch (IllegalStateException e) {
                // No topic is subscribed yet
                try {
                    Thread.sleep(this.pollTimeout); // Otherwise 100% CPU usage until the first topic is subscribed
                } catch (InterruptedException e2) {
                    logger.trace("InterruptedException in main loop.", e2);
                }
            }
        }
        this.kafkaConsumer.close();
        logger.info("Closed StreamConsumer");
    }

    /**
     * Stops the StreamConsumer.
     */
    public void close() {
        this.runFlag = false;
        this.kafkaConsumer.wakeup();
        this.subscriptionUpdater.subscriptionUpdaterRunFlag = false;
    }

    /**
     * Updater for the subscriptions of the Kafka consumer.
     */
    private class SubscriptionUpdater implements Runnable {

        /**
         * Flag that indicates if the SubscriptionUpdater should update the subscriptions or not
         */
        private boolean subscriptionUpdaterRunFlag;

        /**
         * List of currently subscribed topics
         */
        private List<String> currentlySubscribeTopics;

        /**
         * SubscriptionUpdater constructor.
         */
        private SubscriptionUpdater() {
            this.subscriptionUpdaterRunFlag = true;
            this.currentlySubscribeTopics = new LinkedList<>();
        }

        /**
         * Continuously updates the subscriptions.
         */
        @Override
        public void run() {
            while (this.subscriptionUpdaterRunFlag) {
                synchronized (StreamConsumer.this.kafkaConsumer) { // required for SubscriptionUpdater
                    Set<String> topicSet = StreamConsumer.this.kafkaConsumer.listTopics().keySet();

                    List<String> topicsToSubscribe = new LinkedList<>();
                    for (String topic : topicSet) {
                        if (!topic.startsWith("__") && !topic.contains("changelog") && !topic.contains("metrics")) {
                            topicsToSubscribe.add(topic);
                        }
                    }

                    if (haveTopicsChanged(topicsToSubscribe)) {
                        StreamConsumer.this.kafkaConsumer.subscribe(topicsToSubscribe);
                        this.currentlySubscribeTopics = topicsToSubscribe;
                        StringBuilder sb = new StringBuilder("New subscription list: ");
                        for (String topic : topicsToSubscribe) {
                            sb.append(topic).append(" ");
                        }
                        logger.info(sb.toString());
                    }

                }

                try {
                    Thread.sleep(StreamConsumer.this.subscriptionInterval);
                } catch (InterruptedException e) {
                    logger.trace("InterruptedException in SubscriptionUpdater", e);
                }
            }
        }

        /**
         * Check if the topics have changed.
         *
         * @param newTopicsToSubscribe New list of topics
         * @return True if the topics have changed
         */
        private boolean haveTopicsChanged(List<String> newTopicsToSubscribe) {
            if (this.currentlySubscribeTopics.size() != newTopicsToSubscribe.size()) {
                return true;
            } else {
                for (String topic : this.currentlySubscribeTopics) {
                    if (!newTopicsToSubscribe.contains(topic)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
