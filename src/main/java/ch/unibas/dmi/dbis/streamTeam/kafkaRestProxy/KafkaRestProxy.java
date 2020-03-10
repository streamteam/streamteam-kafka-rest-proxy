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

package ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy;

import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.consumer.StreamConsumer;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.ErrorCode;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.ShutdownHelper;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.properties.PropertyReadHelper;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.server.RequestHandler;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.server.RestResult;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main simulation class which reads data stream elements from the sensor data file and generates the sensor data stream w.r.t. the current match time.
 */
public class KafkaRestProxy {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(KafkaRestProxy.class);

    /**
     * Properties
     */
    private final Properties properties;

    /**
     * Buffer
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<String, LinkedList<DataStreamElement>>> buffer;

    /**
     * The number of data stream elements that are buffered for each topic-key-combination (includig the dedicated all-key)
     */
    private final int bufferSize;

    /**
     * Dedicated all-key which enables additionally storing the latest data stream elements of a topic for all keys
     */
    private final String dedicatedAllKey;

    /**
     * Creates and starts the KafkaRestProxy.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        ShutdownHelper.initialize();

        String propertiesFilePath = "/kafkaRestProxy.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = KafkaRestProxy.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}", propertiesFilePath, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
        }

        KafkaRestProxy sensorSimulator = new KafkaRestProxy(properties);
    }

    /**
     * KafkaRestProxy constructor.
     *
     * @param properties Properties
     */
    public KafkaRestProxy(Properties properties) {
        this.properties = properties;

        this.buffer = new ConcurrentHashMap<>();
        this.bufferSize = PropertyReadHelper.readIntOrDie(properties, "bufferSize");
        this.dedicatedAllKey = PropertyReadHelper.readStringOrDie(properties, "dedicatedAllKey");
        int port = PropertyReadHelper.readIntOrDie(properties, "jetty.port");

        // Start StreamConsumer
        StreamConsumer streamConsumer = new StreamConsumer(properties, this);
        ShutdownHelper.addCloseable(streamConsumer);
        Thread streamConsumerThead = new Thread(streamConsumer);
        streamConsumerThead.start();

        // Start Jetty server
        Server server = new Server(port);
        RequestHandler requestHandler = new RequestHandler(this);
        server.setHandler(requestHandler);
        try {
            server.start();
            JettyCloseHelper jettyCloseHelper = new JettyCloseHelper(server);
            ShutdownHelper.addCloseable(jettyCloseHelper);
            server.join();
        } catch (Exception e) {
            logger.error("Caught exception during Jetty server start or join.", e);
            ShutdownHelper.shutdown(ErrorCode.ServerException);
        }
    }

    /**
     * Helper class for enabling stopping Jetty with the ShutdownHelper that expects Closables.
     */
    private static class JettyCloseHelper implements Closeable {

        /**
         * Slf4j logger
         */
        private static final Logger jettyCloseHelperLogger = LoggerFactory.getLogger(JettyCloseHelper.class);

        /**
         * Jetty server
         */
        private final Server jettyCloseHelperServerReference;

        /**
         * JettyCloseHelper constructor
         *
         * @param server Jetty server
         */
        private JettyCloseHelper(Server server) {
            this.jettyCloseHelperServerReference = server;
        }

        /**
         * Stops the Jetty server.
         *
         * @throws IOException Never thrown since all exceptions are caught (but Closable expects close() to throw IOExctions)
         */
        @Override
        public void close() throws IOException {
            jettyCloseHelperLogger.info("Call server.stop().");
            try {
                this.jettyCloseHelperServerReference.stop();
            } catch (Exception e) {
                jettyCloseHelperLogger.error("Caught exception during Jetty server stopping.", e);
            }
        }
    }

    /**
     * Adds a new data stream element to the buffer (thread-safe).
     *
     * @param dataStreamElement
     */
    public void addToBuffer(DataStreamElement dataStreamElement) {
        // Get or create map for the topic of the dataStreamElement
        ConcurrentHashMap<String, LinkedList<DataStreamElement>> mapForTopic;
        synchronized (this.buffer) { // to ensure that the mapForTopic is not replaced with an empty map after it has been filled with a listForKey and a data stream element (can happen if there is a context switch between creating an empty map and adding it to the map)
            if (this.buffer.containsKey(dataStreamElement.topic)) {
                mapForTopic = this.buffer.get(dataStreamElement.topic);
            } else {
                mapForTopic = new ConcurrentHashMap<>();
                this.buffer.put(dataStreamElement.topic, mapForTopic);
            }
        }

        // Get or create queue for the topic and the key of the dataStreamElement
        LinkedList<DataStreamElement> listForKey;
        LinkedList<DataStreamElement> listForAll;
        synchronized (mapForTopic) { // to ensure that the listForKey is not replaced with an empty list after it has been filled with a data stream element (can happen if there is a context switch between creating an empty list and adding it to the map)
            if (mapForTopic.containsKey(dataStreamElement.key)) {
                listForKey = mapForTopic.get(dataStreamElement.key);
            } else {
                listForKey = new LinkedList();
                mapForTopic.put(dataStreamElement.key, listForKey);
            }

            if (mapForTopic.containsKey(this.dedicatedAllKey)) {
                listForAll = mapForTopic.get(this.dedicatedAllKey);
            } else {
                listForAll = new LinkedList();
                mapForTopic.put(this.dedicatedAllKey, listForAll);
            }
        }

        synchronized (listForKey) { // to ensure that listForKey is only modified or read by one thread at a time
            if (listForKey.size() == this.bufferSize) {
                listForKey.removeLast();
            }
            listForKey.addFirst(dataStreamElement);
        }

        synchronized (listForAll) { // to ensure that listForAll is only modified or read by one thread at a time
            if (listForAll.size() == this.bufferSize) {
                listForAll.removeLast();
            }
            listForAll.addFirst(dataStreamElement);
        }
    }


    /**
     * Generates the RestResult for a /consume REST API call with a limit parameter.
     *
     * @param topic Topic (?t=...) of the /consume REST API call
     * @param key   Key (?k=...) of the /consume REST API call, or null if /comsume had no key
     * @param limit Limit (?l=...) of the /consume REST API call
     * @return RestResult
     */
    public RestResult getDataStreamElementsWithLimit(String topic, String key, int limit) {
        if (this.buffer.containsKey(topic)) {
            ConcurrentHashMap<String, LinkedList<DataStreamElement>> mapForTopic = this.buffer.get(topic);
            if (key == null) {
                key = this.dedicatedAllKey;
            }
            if (mapForTopic.containsKey(key)) {
                LinkedList<DataStreamElement> listForKey = mapForTopic.get(key);

                StringBuffer dataJsonArray = new StringBuffer("[");
                int i = 0;

                synchronized (listForKey) { // to ensure that a list is only modified or read by one thread at a time
                    Iterator<DataStreamElement> iterator = listForKey.iterator();
                    while (iterator.hasNext() && i < limit) {
                        if (i > 0) {
                            dataJsonArray.append(",");
                        }
                        dataJsonArray.append(iterator.next().getConsumeResultJson());
                        i++;
                    }
                }

                dataJsonArray.append("]");
                return RestResult.generateDataResult(topic, key, dataJsonArray.toString());
            } else {
                return RestResult.generateNoDataResult(topic, key);
            }
        } else {
            return RestResult.generateNoDataResult(topic, key);
        }
    }

    /**
     * Generates the RestResult for a /listsTopics REST API call.
     *
     * @return RestResult
     */
    public RestResult getTopicList() {
        synchronized (this.buffer) { // to ensure that no new topics are added to the buffer while iterating
            if (this.buffer.isEmpty()) {
                return RestResult.generateNoTopicsResult();
            } else {
                StringBuffer topicsJsonArray = new StringBuffer("[");
                boolean isFirst = true;
                Iterator<String> iterator = this.buffer.keySet().iterator();
                while (iterator.hasNext()) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        topicsJsonArray.append(",");
                    }
                    topicsJsonArray.append("\"");
                    topicsJsonArray.append(iterator.next());
                    topicsJsonArray.append("\"");
                }
                topicsJsonArray.append("]");
                return RestResult.generateListTopicsResult(topicsJsonArray.toString());
            }
        }
    }

    /**
     * Generates the RestResult for a /listKeys REST API call.
     *
     * @param topic Topic (?t=...) of the /listKeys REST API call
     * @return RestResult
     */
    public RestResult getKeyList(String topic) {
        if (this.buffer.containsKey(topic)) {
            ConcurrentHashMap<String, LinkedList<DataStreamElement>> mapForTopic = this.buffer.get(topic);
            synchronized (mapForTopic) { // to ensure that no new keys are added to mapForTopic while iterating
                if (mapForTopic.isEmpty()) {
                    return RestResult.generateNoKeysResult(topic);
                } else {
                    StringBuffer keysJsonArray = new StringBuffer("[");
                    boolean isFirst = true;
                    Iterator<String> iterator = mapForTopic.keySet().iterator();
                    while (iterator.hasNext()) {
                        String key = iterator.next();
                        if (key != this.dedicatedAllKey) { // Do not list the dedicated all-key as it is not an actual key
                            if (isFirst) {
                                isFirst = false;
                            } else {
                                keysJsonArray.append(",");
                            }
                            keysJsonArray.append("\"");
                            keysJsonArray.append(key);
                            keysJsonArray.append("\"");
                        }
                    }
                    keysJsonArray.append("]");
                    return RestResult.generateListKeysResult(topic, keysJsonArray.toString());
                }
            }
        } else {
            return RestResult.generateNoKeysResult(topic);
        }
    }
}
