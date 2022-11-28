/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stargate.sdk.rest.exception;

/**
 * Specialized Error.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class TableNotFoundException extends RuntimeException {
    
    /** Serial. */
    private static final long serialVersionUID = -4491748257797687008L;

    /**
     * Constructor with message.
     *
     * @param colName
     *      column name
     */
    public TableNotFoundException(String colName) {
        super("Cannot find Table " + colName);
    }
    
    /**
     * Constructor with message and parent.
     *
     * @param colName
     *      column name     
     * @param parent
     *      parent exception
     */
    public TableNotFoundException(String colName, Throwable parent) {
        super("Cannot find Table " + colName, parent);
    }

}
