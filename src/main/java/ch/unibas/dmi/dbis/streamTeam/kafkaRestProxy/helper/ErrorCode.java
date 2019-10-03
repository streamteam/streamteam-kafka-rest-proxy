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

/**
 * Error codes.
 */
public enum ErrorCode {
    /**
     * Indicates that the program has to be shutten down due to wrong parameters.
     */
    WrongParameters(1),
    /**
     * Indicates that the program has to be shutten down due to a PropertyDoesNotExistException or a PropertyHasWrongFormatException.
     */
    PropertyException(2),
    /**
     * Indicates that the program has to be shutten down due to an exception thrown by the server.
     */
    ServerException(3);

    /**
     * Code that is used in the System.exit() call
     */
    private final int code;

    /**
     * Error code constructor
     *
     * @param code Integer value
     */
    ErrorCode(int code) {
        this.code = code;
    }

    /**
     * Returns the integer value of the error code.
     *
     * @return Integer value of the error code
     */
    public int getCode() {
        return this.code;
    }
}
