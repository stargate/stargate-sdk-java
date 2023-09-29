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

package io.stargate.sdk.http.domain;

/**
 * Constants in the JSON API.
 */
public enum FilterKeyword {

    /**
     * ALL.
     */
    ALL("$all"),

    /**
     * SIZE.
     */
    SIZE("$size"),

    /**
     * EXISTS.
     */
    EXISTS("$exists"),

    /**
     * SIMILARITY.
     */
    SIMILARITY("$similarity"),

    /**
     * VECTOR.
     */
    VECTOR("$vector"),

    /**
     * VECTORIZE.
     */
    VECTORIZE("$vectorize");

    /**
     * Keyword.
     */
    private String keyword;

    /**
     * Constructor for the enum.
     *
     * @param op
     *      current operator
     */
    private FilterKeyword(String op) {
        this.keyword = op;
    }

    public String getKeyword() {
        return keyword;
    }

}
