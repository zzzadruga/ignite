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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.eviction.fifo.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Tests that common cache objects' toString() methods do not lead to stack overflow.
 */
public class GridCacheObjectToStringSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache mode for test. */
    private CacheMode cacheMode;

    /** Cache eviction policy. */
    private CacheEvictionPolicy evictionPlc;

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setEvictionPolicy(evictionPlc);
        cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        evictionPlc = null;
    }

    /** @throws Exception If failed. */
    public void testLocalCacheFifoEvictionPolicy() throws Exception {
        cacheMode = LOCAL;
        evictionPlc = new CacheFifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testLocalCacheLruEvictionPolicy() throws Exception {
        cacheMode = LOCAL;
        evictionPlc = new CacheLruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testReplicatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new CacheFifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testReplicatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new CacheLruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testPartitionedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new CacheFifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testPartitionedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new CacheLruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testColocatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new CacheFifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    public void testColocatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new CacheLruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    private void checkToString() throws Exception {
        Ignite g = startGrid(0);

        try {
            IgniteCache<Object, Object> cache = g.jcache(null);

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            for (int i = 0; i < 10; i++) {
                GridCacheEntryEx entry = ((IgniteKernal)g).context().cache().internalCache().peekEx(i);

                if (entry != null)
                    assertFalse("Entry is locked after implicit transaction commit: " + entry, entry.lockedByAny());
            }

            assertFalse(cache.toString().isEmpty());
            assertFalse(cache.iterator().toString().isEmpty());

            try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, cache.get(1));

                cache.put(2, 22);

                assertFalse(tx.toString().isEmpty());

                assertFalse(cache.toString().isEmpty());

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
