/*
 * Copyright (c) 2010, 2020 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly.http.util;

import org.glassfish.grizzly.ThreadCache;

/**
 * {@link DataChunk} implementation, which could be cached in the thread cache.
 * 
 * @author Alexey Stashok
 */
public class CacheableDataChunk extends DataChunk {
    private static final ThreadCache.CachedTypeIndex<CacheableDataChunk> CACHE_IDX = ThreadCache.obtainIndex(CacheableDataChunk.class, 2);

    public static CacheableDataChunk create() {
        final CacheableDataChunk dataChunk = ThreadCache.takeFromCache(CACHE_IDX);
        if (dataChunk != null) {
            return dataChunk;
        }

        return new CacheableDataChunk();
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public void recycle() {
        reset();
        ThreadCache.putToCache(CACHE_IDX, this);
    }
}
