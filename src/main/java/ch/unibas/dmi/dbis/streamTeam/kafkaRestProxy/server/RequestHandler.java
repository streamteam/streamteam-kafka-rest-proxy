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

import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.KafkaRestProxy;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Handler for the REST API calls.
 */
public class RequestHandler extends AbstractHandler {

    /**
     * KafkaRestProxy
     */
    private KafkaRestProxy kafkaRestProxy;

    /**
     * RequestHandler constructor
     *
     * @param kafkaRestProxy KafkaRestProxy
     */
    public RequestHandler(KafkaRestProxy kafkaRestProxy) {
        this.kafkaRestProxy = kafkaRestProxy;
    }

    /**
     * Distributes incoming REST API calls.
     *
     * @param target              The target of the REST API call
     * @param request             Request
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws IOException      Thrown if unable to handle the REST API call
     * @throws ServletException Thrown if unable to handle the REST API call
     */
    @Override
    public void handle(String target, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        switch (target) {
            case "/consume":
                handleConsume(request, httpServletRequest, httpServletResponse);
                break;
            case "/listTopics":
                handleListTopics(request, httpServletRequest, httpServletResponse);
                break;
            case "/listKeys":
                handleListKeys(request, httpServletRequest, httpServletResponse);
                break;
            default:
                handleDefault(target, request, httpServletRequest, httpServletResponse);
                break;
        }
        request.setHandled(true);
    }

    /**
     * Handles a /consume call.
     *
     * @param request             Request
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws IOException Thrown if unable to handle the REST API call
     */
    private void handleConsume(Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.addHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setContentType("text/plain;charset=utf-8");
        PrintWriter writer = httpServletResponse.getWriter();
        RestResult restResult;

        if (request.getParameter("t") == null) {
            restResult = RestResult.generateMissingParameterResult("consume", "topic (t=...)");
        } else {
            if (request.getParameter("l") != null) {
                String topic = request.getParameter("t"); // ?t=...
                String key = request.getParameter("k"); // ?k=... (can be null)
                try {
                    int limit = Integer.parseInt(request.getParameter("l")); // ?l=....
                    if (limit < 1) {
                        restResult = RestResult.generateWrongParameterValueResult("consume", "limit (l=...) has to be greater than 0.");
                    } else {
                        restResult = this.kafkaRestProxy.getDataStreamElementsWithLimit(topic, key, limit);
                    }
                } catch (NumberFormatException e) {
                    restResult = RestResult.generateWrongParameterValueResult("consume", "limit (l=...) has to be a number (integer).");
                }
            } else if (request.getParameter("o") != null) {
                String topic = request.getParameter("t"); // ?t=...
                String key = request.getParameter("k"); // ?k=... (can be null)
                try {
                    long offset = Long.parseLong(request.getParameter("o")); // ?o=...
                    restResult = this.kafkaRestProxy.getDataStreamElementsWithOffset(topic, key, offset);
                } catch (NumberFormatException e) {
                    restResult = RestResult.generateWrongParameterValueResult("consume", "offset (o=...) has to be a number (long).");
                }
            } else {
                restResult = RestResult.generateMissingParameterResult("consume", "limit (l=...) or offset (o=...)");
            }
        }

        httpServletResponse.setStatus(restResult.httpStatusCode);
        writer.println(restResult.content);
    }

    /**
     * Handles a /listKeys call.
     *
     * @param request             Request
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws IOException Thrown if unable to handle the REST API call
     */
    private void handleListKeys(Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.addHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setContentType("text/plain;charset=utf-8");
        PrintWriter writer = httpServletResponse.getWriter();
        RestResult restResult;

        if (request.getParameter("t") == null) {
            restResult = RestResult.generateMissingParameterResult("listKeys", "topic (t=...)");
        } else {
            String topic = request.getParameter("t"); // ?t=...
            restResult = this.kafkaRestProxy.getKeyList(topic);
        }

        httpServletResponse.setStatus(restResult.httpStatusCode);
        writer.println(restResult.content);
    }

    /**
     * Handles a /listTopics call.
     *
     * @param request             Request
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws IOException Thrown if unable to handle the REST API call
     */
    private void handleListTopics(Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.addHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setContentType("text/plain;charset=utf-8");
        PrintWriter writer = httpServletResponse.getWriter();
        RestResult restResult = this.kafkaRestProxy.getTopicList();
        httpServletResponse.setStatus(restResult.httpStatusCode);
        writer.println(restResult.content);
    }

    /**
     * Handles all remaining REST API calls.
     *
     * @param target              The target of the REST API call
     * @param request             Request
     * @param httpServletRequest  HttpServletRequest
     * @param httpServletResponse HttpServletResponse
     * @throws IOException Thrown if unable to handle the REST API call
     */
    private void handleDefault(String target, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        httpServletResponse.addHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setContentType("text/plain;charset=utf-8");
        PrintWriter writer = httpServletResponse.getWriter();
        RestResult restResult = RestResult.generateNotImplementedResult(target);
        httpServletResponse.setStatus(restResult.httpStatusCode);
        writer.println(restResult.content);
    }
}
