/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.AbstractEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;

/**
 *
 */
public class EvictionPoliciesReproducer extends GridCommonAbstractTest {

    /** Keys count. */
    private static final int KEYS_COUNT = 30;

    /** Eviction factory. */
    private AbstractEvictionPolicyFactory evictionFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, byte[]> ccfg = new CacheConfiguration<Integer, byte[]>()
            .setName(DEFAULT_CACHE_NAME)
            .setEvictionPolicyFactory(evictionFactory.setMaxMemorySize(1024))
            .setOnheapCacheEnabled(true);

        cfg.setCacheConfiguration(ccfg);
        cfg.setIncludeEventTypes(EVT_CACHE_ENTRY_EVICTED);

        return cfg;
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * {@link FifoEvictionPolicyFactory} test.
     */
    @Test
    public void testFifoEvictionPolicy() throws Exception {
        evictionFactory = new FifoEvictionPolicyFactory();
        testEvictionPolicy();
    }

    /**
     * {@link LruEvictionPolicyFactory} test.
     */
    @Test
    public void testLruEvictionPolicy() throws Exception {

        evictionFactory = new LruEvictionPolicyFactory();
        testEvictionPolicy();
    }

    /**
     * {@link SortedEvictionPolicyFactory} test.
     */
    @Test
    public void testSortedEvictionPolicy() throws Exception {

        evictionFactory = new SortedEvictionPolicyFactory();
        testEvictionPolicy();
    }

    /**
     * @throws Exception
     */
    public void testEvictionPolicy() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        // Cache is empty, and all entries won't be evicted.
        addEntries(ignite0, 0);

        stopAllGrids();

        ignite0 = startGrid(0);

        ignite0.cache(DEFAULT_CACHE_NAME).put(777, new byte[1000]);
        ignite0.cache(DEFAULT_CACHE_NAME).remove(777);

        // Cache is empty, but all entries will be evicted.
        addEntries(ignite0, 0);
        //addEntries(ignite0, KEYS_COUNT);
    }

    /**
     * @param ignite Ignite.
     * @param expectedEvictedEntries expected number of entries to be evicted.
     */
    private static void addEntries(Ignite ignite, int expectedEvictedEntries) {
        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        AtomicInteger evictedEntries = new AtomicInteger();

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                evictedEntries.incrementAndGet();

                return true;
            }
        }, EVT_CACHE_ENTRY_EVICTED);

        assertEquals(0, cache.size());

        for (int i = 0; i < KEYS_COUNT; ++i)
            cache.put(i, new byte[29]);

        assertEquals(expectedEvictedEntries, evictedEntries.get());
    }
}
