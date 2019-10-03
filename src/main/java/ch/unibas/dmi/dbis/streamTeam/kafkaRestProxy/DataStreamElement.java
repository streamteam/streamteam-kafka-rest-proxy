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

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Represents a single data stream element.
 */
public class DataStreamElement {

    /**
     * Content of the data stream element
     */
    public final byte[] content;

    /**
     * Key of the data stream element
     */
    public final String key;

    /**
     * Topic from which the data stream element was consumed
     */
    public final String topic;

    /**
     * Partition from which the data stream element was consumed
     */
    public final int partition;

    /**
     * Offset of the data stream element in the partition
     */
    public final long offset;

    /**
     * DataStreamElement constructor.
     *
     * @param content   Content of the data stream element
     * @param key       Key of the data stream element
     * @param topic     Topic from which the data stream element was consumed
     * @param partition Partition from which the data stream element was consumed
     * @param offset    Offset of the data stream element in the partition
     */
    public DataStreamElement(byte[] content, String key, String topic, int partition, long offset) {
        this.content = content;
        this.key = key;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * DataStreamElement constructor.
     *
     * @param record Kafka ConsumerRecord
     */
    public DataStreamElement(ConsumerRecord<String, byte[]> record) {
        this(record.value(), record.key(), record.topic(), record.partition(), record.offset());
    }

    /**
     * Returns a string representation of the data stream element.
     *
     * @return String representation of the data stream element
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer("[value=<");

        // https://stackoverflow.com/questions/20706783/put-byte-array-to-json-and-vice-versa
        String base64String = Base64.encodeBase64String(this.content);
        buffer.append(base64String);

        buffer.append(">,key=");
        buffer.append(this.key);
        buffer.append(",topic=");
        buffer.append(this.topic);
        buffer.append(",partition=");
        buffer.append(this.partition);
        buffer.append(",offset=");
        buffer.append(this.offset);
        buffer.append("]");
        return buffer.toString();
    }

    /**
     * Returns a JSON representation fo the data stream element.
     *
     * @return JSON representation of the data stream element
     */
    public String getConsumeResultJson() {
        StringBuffer buffer = new StringBuffer("{\"v\":\"");

        // https://stackoverflow.com/questions/20706783/put-byte-array-to-json-and-vice-versa
        String base64String = Base64.encodeBase64String(this.content);
        buffer.append(base64String);

        buffer.append("\",\"k\":\"");
        buffer.append(this.key);
        buffer.append("\",\"t\":\"");
        buffer.append(this.topic);
        buffer.append("\",\"p\":");
        buffer.append(this.partition);
        buffer.append(",\"o\":");
        buffer.append(this.offset);
        buffer.append("}");
        return buffer.toString();
    }
}
