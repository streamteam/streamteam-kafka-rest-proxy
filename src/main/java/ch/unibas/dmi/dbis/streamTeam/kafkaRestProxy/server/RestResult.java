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

package ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.server;

/**
 * Represents the result of a REST API call.
 */
public class RestResult {

    /**
     * HTTP status code
     */
    public final int httpStatusCode;

    /**
     * Content
     */
    public final String content;

    /**
     * RestResult constructor.
     *
     * @param httpStatusCode HTTP status code
     * @param content        Content
     */
    private RestResult(int httpStatusCode, String content) {
        this.httpStatusCode = httpStatusCode;
        this.content = content;
    }

    /**
     * Generates a RestResult that indicates that there are no data stream elements.
     *
     * @param topic Topic of the data stream elements
     * @param key   Key of the data stream elements
     * @return RestResult
     */
    public static RestResult generateNoDataResult(String topic, String key) {
        int httpStatusCode = 204; // NO CONTENT
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":\"");
        jsonBuffer.append(topic);
        jsonBuffer.append("\",\"k\":\"");
        jsonBuffer.append(key);
        jsonBuffer.append("\",\"d\":null}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that contains the data stream elements.
     *
     * @param topic         Topic of the data stream elements
     * @param key           Key of the data stream elements
     * @param dataJsonArray JSON representation of the data stream elements
     * @return RestResult
     */
    public static RestResult generateDataResult(String topic, String key, String dataJsonArray) {
        int httpStatusCode = 200; // OK
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":\"");
        jsonBuffer.append(topic);
        jsonBuffer.append("\",\"k\":\"");
        jsonBuffer.append(key);
        jsonBuffer.append("\",\"d\":");
        jsonBuffer.append(dataJsonArray);
        jsonBuffer.append("}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that indicates that there are no topics.
     *
     * @return RestResult
     */
    public static RestResult generateNoTopicsResult() {
        int httpStatusCode = 204; // NO CONTENT
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":null}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that contains the topics.
     *
     * @param topicsJsonArray JSON representation of the topics
     * @return RestResult
     */
    public static RestResult generateListTopicsResult(String topicsJsonArray) {
        int httpStatusCode = 200; // OK
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":");
        jsonBuffer.append(topicsJsonArray);
        jsonBuffer.append("}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that indicates that there are no keys.
     *
     * @param topic Topic of the data stream elements
     * @return RestResult
     */
    public static RestResult generateNoKeysResult(String topic) {
        int httpStatusCode = 204; // NO CONTENT
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":\"");
        jsonBuffer.append(topic);
        jsonBuffer.append("\",\"k\":null}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that contains the keys.
     *
     * @param topic         Topic of the data stream elements
     * @param keysJsonArray JSON representation of the keys
     * @return RestResult
     */
    public static RestResult generateListKeysResult(String topic, String keysJsonArray) {
        int httpStatusCode = 200; // OK
        StringBuffer jsonBuffer = new StringBuffer("{\"t\":\"");
        jsonBuffer.append(topic);
        jsonBuffer.append("\",\"k\":");
        jsonBuffer.append(keysJsonArray);
        jsonBuffer.append("}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that indicates that there are missing parameters for the REST API call.
     *
     * @param target    Target of the REST API call
     * @param parameter Missing parameter
     * @return RestResult
     */
    public static RestResult generateMissingParameterResult(String target, String parameter) {
        // http://stackoverflow.com/questions/3050518/what-http-status-response-code-should-i-use-if-the-request-is-missing-a-required
        int httpStatusCode = 422; // UNPROCESSSABLE ENTITY
        StringBuffer jsonBuffer = new StringBuffer("{\"e\":\"Missing parameter for target ");
        jsonBuffer.append(target);
        jsonBuffer.append(". Requires: ");
        jsonBuffer.append(parameter);
        jsonBuffer.append("\"}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that indicates that the value for a parameter of the REST API call is wrong.
     *
     * @param target Target of the REST API call
     * @param info   Information why the value is wrong
     * @return RestResult
     */
    public static RestResult generateWrongParameterValueResult(String target, String info) {
        int httpStatusCode = 422; // UNPROCESSSABLE ENTITY
        StringBuffer jsonBuffer = new StringBuffer("{\"e\":\"Wrong parameter value for target ");
        jsonBuffer.append(target);
        jsonBuffer.append(": ");
        jsonBuffer.append(info);
        jsonBuffer.append("\"}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }

    /**
     * Generates a RestResult that indicates that the REST API call is not implemented.
     *
     * @param target Target of the REST API call
     * @return RestResult
     */
    public static RestResult generateNotImplementedResult(String target) {
        int httpStatusCode = 501; // NOT IMPLEMENTED
        StringBuffer jsonBuffer = new StringBuffer("{\"e\":\"");
        jsonBuffer.append(target);
        jsonBuffer.append(" is not implemented.\"}");
        return new RestResult(httpStatusCode, jsonBuffer.toString());
    }
}
