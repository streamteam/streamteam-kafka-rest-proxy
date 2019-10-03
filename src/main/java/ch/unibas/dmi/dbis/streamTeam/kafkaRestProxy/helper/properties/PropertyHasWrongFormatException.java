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

/**
 * Thrown to indicate that a property value has a wrong format.
 */
public class PropertyHasWrongFormatException extends Exception {

    /**
     * PropertyHasWrongFormatException constructor.
     *
     * @param key          Key of the property
     * @param exceptedType Expected type of the property value
     * @param value        Actual value
     */
    public PropertyHasWrongFormatException(String key, String exceptedType, String value) {
        super("Property " + key + " is expected to be of type " + exceptedType + " but has value: " + value);
    }
}
