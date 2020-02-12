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

import com.google.common.util.concurrent.AtomicDouble;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.cache.CacheGroupsMetricsRebalanceTest.randomArray;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.extractEntryInfo;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Test for remove operation.
 */
public class CacheDhtLocalPartitionAfterRemoveSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration()
            .setAffinity(new RendezvousAffinityFunction(false, 8))
            .setGroupName(DEFAULT_CACHE_NAME)
            .setName("CACHE1");
        //cfg.setDataStorageConfiguration(new DataStorageConfiguration().setMetricsEnabled(true).setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMetricsEnabled(true).setPersistenceEnabled(true)));
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setNearConfiguration(null);
        ccfg.setEvictionPolicyFactory(new FifoEvictionPolicyFactory<>().setMaxMemorySize(200));
        ccfg.setOnheapCacheEnabled(true);
        ccfg.setCacheMode(REPLICATED);
        cfg.setRebalanceBatchSize(200);
        cfg.setCacheConfiguration(ccfg, new CacheConfiguration(ccfg).setName("CACHE2"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryUsage() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        ignite0.cluster().active(true);
        MetricRegistry mreg0 = ignite0.context().metric()
            .registry(metricName(CACHE_GROUP_METRICS_PREFIX, DEFAULT_CACHE_NAME));
        LongMetric totalAllocatedSize =  mreg0.findMetric("TotalAllocatedSize");
        System.out.println(">>>>>>>> totalAllocatedSize " + totalAllocatedSize.value());

        assertEquals(10_000, GridDhtLocalPartition.MAX_DELETE_QUEUE_SIZE);

        long size = 0;

        for (IgniteCache<Integer, byte[]> cache : Arrays.<IgniteCache<Integer, byte[]>>asList(/*grid(0).cache("CACHE1"),*/ grid(0).cache("CACHE2")))
        for (int i = 1; i < 10; ++i) {
            byte[] bytes = randomArray(256);
            cache.put(i, bytes);
            size += Integer.BYTES + bytes.length;
        }



/*        for (int g = 0; g < GRID_CNT; g++) {
            cache = grid(g).cache(DEFAULT_CACHE_NAME);

            for (GridDhtLocalPartition p : dht(cache).topology().localPartitions()) {
                long size = p.dataStore().fullSize();

                System.out.println("id: " + p.id() + " size: " + size);

                //assertTrue("Unexpected size: " + size, size <= 32);
            }
        }*/

/*        for (int i = 0; i < 20_000; ++i)
            cache.remove(new TestKey(String.valueOf(i)));

        assertEquals(0, cache.size());
        System.out.println("------------------------ after delete");

        for (int g = 0; g < GRID_CNT; g++) {
            cache = grid(g).cache(DEFAULT_CACHE_NAME);

            for (GridDhtLocalPartition p : dht(cache).topology().localPartitions()) {
                long size = p.dataStore().fullSize();

                System.out.println("id: " + p.id() + " size: " + size);

                //assertTrue("Unexpected size: " + size, size <= 32);
            }
        }*/

        CacheGroupContext grpCtx = ignite(0).context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

/*        if (grpCtx == null || !grpCtx.persistenceEnabled())
            return;

        GridKernalContext ctx = ignite(0).context();
        GridCacheSharedContext cctx = ctx.cache().context();

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)cctx.pageStore();

        FilePageStore pageStore = (FilePageStore)pageStoreMgr.getStore(grpCtx.groupId(), 1);

        pageStore.size();*/

        grid(0).cache("CACHE2").put(-1, new byte[512]);

        long startTime = System.currentTimeMillis();

        IgniteDhtDemandedPartitionsMap map = new IgniteDhtDemandedPartitionsMap();

        for (int i = 0; i < grpCtx.affinity().partitions(); i++) {
            map.addFull(i);
        }

        IgniteRebalanceIterator iter = grpCtx.offheap().rebalanceIterator(map, grpCtx.affinity().lastVersion());

        long sizes = 0;
        long count = 0;
        for (CacheDataRow row : iter) {
            GridCacheEntryInfo info = extractEntryInfo(row, grpCtx.mvccEnabled());
            //String cacheName = grpCtx.caches().stream().filter(c -> c.cacheId() == info.cacheId()).findAny().get().name();

            //ignite(0).context().cache().internalCache(cacheName).preloader()
            count++;
            sizes += info.marshalledSize(grpCtx.cacheObjectContext());
        }

        long time = System.currentTimeMillis() - startTime;
        IgniteEx ignite1 = startGrid(1);
        ignite1.cluster().active(true);
        MetricRegistry mreg1 = ignite1.context().metric()
            .registry(metricName(CACHE_GROUP_METRICS_PREFIX, DEFAULT_CACHE_NAME));
        LongMetric receivedBytes =  mreg1.findMetric("RebalancingReceivedBytes");
        LongMetric totalSize1 =  mreg1.findMetric("TotalSize");
        LongMetric totalSize0 =  mreg0.findMetric("TotalSize");
        LongMetric realSize1 =  mreg1.findMetric("RealSize");
        LongMetric realSize0 =  mreg0.findMetric("RealSize");
        LongMetric rebalanceSize1 =  mreg1.findMetric("RebalanceSize");
        LongMetric rebalanceSize0 =  mreg0.findMetric("RebalanceSize");
        LongMetric totalAllocatedSize1 =  mreg1.findMetric("TotalAllocatedSize");

        ignite1.context().cache().internalCache("CACHE2").preloader().rebalanceFuture().get();

        System.out.println(">>>>>>>> totalAllocatedSize " + totalAllocatedSize.value());
        System.out.println(">>>>>>>> totalAllocatedSize1 " + totalAllocatedSize1.value());
        System.out.println(">>>>>>>> rebalanceSize 0 " + rebalanceSize0.value());
        System.out.println(">>>>>>>> rebalanceSize 1 " + rebalanceSize1.value());
        System.out.println(">>>>>>>> totalSize 0 " + totalSize0.value());
        System.out.println(">>>>>>>> totalSize 1 " + totalSize1.value());
        System.out.println(">>>>>>>> realSize 0 " + realSize0.value());
        System.out.println(">>>>>>>> realSize 1 " + realSize1.value());
        System.out.println(">>>>>>>> keyCount  " + count);
        System.out.println(">>>>>>>> sizeOnAdd " + size);
        System.out.println(">>>>>>>> countedSize " + sizes);
        System.out.println(">>>>>>>> receivedSize " + receivedBytes.value());
        System.out.println(">>>>>>>> time " + time);
    }
}
