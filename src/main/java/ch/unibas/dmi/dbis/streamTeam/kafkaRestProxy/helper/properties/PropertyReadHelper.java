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

package ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.properties;

import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.ErrorCode;
import ch.unibas.dmi.dbis.streamTeam.kafkaRestProxy.helper.ShutdownHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Helper class for reading typed values from a properties object.
 */
public class PropertyReadHelper {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(PropertyReadHelper.class);

    /**
     * Reads a boolean value from the properties object.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Boolean value
     * @throws PropertyHasWrongFormatException Thrown if the value has the wrong format.
     * @throws PropertyDoesNotExistException   Thrown if the key does not exist.
     */
    public static boolean readBoolean(Properties properties, String key) throws PropertyHasWrongFormatException, PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            value = value.trim();
            if (value.toLowerCase().equals("true")) {
                return true;
            } else if (value.toLowerCase().equals("false")) {
                return false;
            } else {
                throw new PropertyHasWrongFormatException(key, "boolean", value);
            }
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads a boolean value from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Boolean value
     */
    public static boolean readBooleanOrDie(Properties properties, String key) {
        try {
            return readBoolean(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read boolean for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return false; // never reached
        }
    }

    /**
     * Reads an integer value from the properties object.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Integer value
     * @throws PropertyHasWrongFormatException Thrown if the value has the wrong format.
     * @throws PropertyDoesNotExistException   Thrown if the key does not exist.
     */
    public static int readInt(Properties properties, String key) throws PropertyHasWrongFormatException, PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            value = value.trim();
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new PropertyHasWrongFormatException(key, "integer", value);
            }
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads an integer value from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Integer value
     */
    public static int readIntOrDie(Properties properties, String key) {
        try {
            return readInt(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read int for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return 0; // never reached
        }
    }

    /**
     * Reads a long value from the properties object.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Long value
     * @throws PropertyHasWrongFormatException Thrown if the value has the wrong format.
     * @throws PropertyDoesNotExistException   Thrown if the key does not exist.
     */
    public static long readLong(Properties properties, String key) throws PropertyHasWrongFormatException, PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            value = value.trim();
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new PropertyHasWrongFormatException(key, "long", value);
            }
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads a long value from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Long value
     */
    public static long readLongOrDie(Properties properties, String key) {
        try {
            return readLong(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read long for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return 0; // never reached
        }
    }

    /**
     * Reads a double value from the properties object.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Double value
     * @throws PropertyHasWrongFormatException Thrown if the value has the wrong format.
     * @throws PropertyDoesNotExistException   Thrown if the key does not exist.
     */
    public static double readDouble(Properties properties, String key) throws PropertyHasWrongFormatException, PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            value = value.trim();
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new PropertyHasWrongFormatException(key, "double", value);
            }
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads a double value from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return Double value
     */
    public static double readDoubleOrDie(Properties properties, String key) {
        try {
            return readDouble(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read double for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return 0; // never reached
        }
    }

    /**
     * Reads a string value from the properties object.
     *
     * @param properties Properties object
     * @param key        Key
     * @return String value
     * @throws PropertyDoesNotExistException Thrown if the key does not exist.
     */
    public static String readString(Properties properties, String key) throws PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads a string value from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return String value
     */
    public static String readStringOrDie(Properties properties, String key) {
        try {
            return readString(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read string for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return null; // never reached
        }
    }

    /**
     * Reads a comma-separated list of strings from the properties object
     *
     * @param properties Properties object
     * @param key        Key
     * @return List of strings
     * @throws PropertyDoesNotExistException Thrown if the key does not exist.
     */
    public static List<String> readListOfStrings(Properties properties, String key) throws PropertyDoesNotExistException {
        if (properties.containsKey(key)) {
            String listAsString = properties.getProperty(key);
            listAsString = listAsString.trim();
            String[] values = listAsString.split(",");

            List<String> result = new ArrayList<>();
            Collections.addAll(result, values);
            return result;
        } else {
            throw new PropertyDoesNotExistException(key);
        }
    }

    /**
     * Reads a comma-separated list of strings from the properties object or dies in case of an exception.
     *
     * @param properties Properties object
     * @param key        Key
     * @return List of strings
     */
    public static List<String> readListOfStringsOrDie(Properties properties, String key) {
        try {
            return readListOfStrings(properties, key);
        } catch (Exception e) {
            logger.error("Unable to read list of strings for key {} from properties", key, e);
            ShutdownHelper.shutdown(ErrorCode.PropertyException);
            return null; // never reached
        }
    }
}