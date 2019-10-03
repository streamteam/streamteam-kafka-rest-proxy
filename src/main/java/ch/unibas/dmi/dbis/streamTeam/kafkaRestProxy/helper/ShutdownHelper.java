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

package ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Stack;

/**
 * Helper class for properly shutting down the program.
 */
public class ShutdownHelper {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ShutdownHelper.class);

    /**
     * Stack storing the Closeables that have to be closed in case of a shutdown
     */
    private static Stack<Closeable> closeOnDieStack;

    /**
     * Initializes the Shutdown helper. This method should be called at the very beginning of the main method of a program.
     */
    public static void initialize() {
        closeOnDieStack = new Stack<>();

        // Called on System.exit(), crlt-c and kill (with SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Shutdown Hook called.");
                closeEverythingOnStack();
                logger.info("Shutdown Hook finished properly.");
                try {
                    Thread.sleep(50); // time for the logger to flush
                } catch (InterruptedException e) {
                    logger.error("Caught exception.", e);
                }
            }
        });
    }

    /**
     * Shuts the program down.
     *
     * @param errorCode Error code
     */
    public static void shutdown(ErrorCode errorCode) {
        logger.error("Shutdown with error code {} ({})", errorCode.getCode(), errorCode.name());
        System.exit(errorCode.getCode());
    }

    /**
     * Closes all Closeables in the correct order.
     */
    private static void closeEverythingOnStack() {
        while (!closeOnDieStack.empty()) {
            Closeable topMostCloseable = closeOnDieStack.pop();
            logger.info("Close {} of type {}", topMostCloseable, topMostCloseable.getClass().getName());
            try {
                topMostCloseable.close();
            } catch (IOException e) {
                logger.error("Caught Exception.", e);
            }
        }
    }

    /**
     * Adds a Closeable to the stack.
     *
     * @param closeable Closeable object
     */
    public static void addCloseable(Closeable closeable) {
        closeOnDieStack.push(closeable);
    }

    /**
     * Removes a Closeable from the stack if it is the topmost element.
     *
     * @param closeable Closeable object
     */
    public static void removeClosable(Closeable closeable) {
        Closeable topMostCloseable = closeOnDieStack.pop();
        if (closeable != topMostCloseable) {
            logger.error("Cannot remove {} of type {} from closeOnDieStack since it is not the topmost element.", topMostCloseable, topMostCloseable.getClass().getName());
            ShutdownHelper.addCloseable(topMostCloseable);
        }
    }

}
