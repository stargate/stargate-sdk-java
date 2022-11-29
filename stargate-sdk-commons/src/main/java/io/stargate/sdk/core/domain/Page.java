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

package io.stargate.sdk.core.domain;

import java.util.List;
import java.util.Optional;

/**
 * Hold results for paging
 *
 * @author Cedrick LUNVEN (@clunven)
 *
 * @param <R>
 *      document type
 */
public class Page<R> {
 
    /** size of page asked. */
    private final int pageSize;
    
    /** Of present there is a next page. */
    private final String pageState;
    
    /** list of results matchin the request. */
    private final List< R > results;
    
    /**
     * Default Constructor.
     */
    public Page() {
        this.pageSize  = 0;
        this.pageState = null;
        this.results   = null;
    }
    /**
     * Default constructor.
     * 
     * @param pageSize int
     * @param pageState String
     * @param results List
     */
    public Page(int pageSize, String pageState, List<R> results) {
        this.pageSize  = pageSize;
        this.pageState = pageState;
        this.results   = results;
    }
 
    /**
     * Getter accessor for attribute 'pageSize'.
     *
     * @return
     *       current value of 'pageSize'
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Getter accessor for attribute 'pageState'.
     *
     * @return
     *       current value of 'pageState'
     */
    public Optional<String> getPageState() {
        return Optional.ofNullable(pageState);
    }

    /**
     * Getter accessor for attribute 'results'.
     *
     * @return
     *       current value of 'results'
     */
    public List<R> getResults() {
        return results;
    }

    /**
     * When the result is a singleton.
     *
     * @return
     *      result as a singleton
     */
    public R one() {
        if (getResults() == null || getResults().size() !=1) {
            throw new IllegalStateException("Current page does not contain a single record");
        }
        return getResults().get(0);
    }
}
