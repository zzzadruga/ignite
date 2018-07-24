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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.TestDebugLog1;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.testframework.GridTestUtils.mergeExchangeWaitVersion;

/**
 *
 */
public class CacheExchangeMergeTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long WAIT_SECONDS = 15;

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** */
    private boolean testSpi;

    /** */
    private boolean testDelaySpi;

    /** */
    private static String[] cacheNames = {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"};

    /** */
    private boolean cfgCache = true;

    /** */
    private IgniteClosure<String, Boolean> clientC;

    /** */
    private static ExecutorService executor;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        else if (testDelaySpi)
            cfg.setCommunicationSpi(new TestDelayExchangeMessagesSpi());

        Boolean clientMode = client.get();

        if (clientMode == null && clientC != null)
            clientMode = clientC.apply(igniteInstanceName);

        if (clientMode != null) {
            cfg.setClientMode(clientMode);

            client.set(null);
        }

        if (cfgCache) {
            cfg.setCacheConfiguration(
                cacheConfiguration("c1", ATOMIC, PARTITIONED, 0),
                cacheConfiguration("c2", ATOMIC, PARTITIONED, 1),
                cacheConfiguration("c3", ATOMIC, PARTITIONED, 2),
                cacheConfiguration("c4", ATOMIC, PARTITIONED, 10),
                cacheConfiguration("c5", ATOMIC, REPLICATED, 0),
                cacheConfiguration("c6", TRANSACTIONAL, PARTITIONED, 0),
                cacheConfiguration("c7", TRANSACTIONAL, PARTITIONED, 1),
                cacheConfiguration("c8", TRANSACTIONAL, PARTITIONED, 2),
                cacheConfiguration("c9", TRANSACTIONAL, PARTITIONED, 10),
                cacheConfiguration("c10", TRANSACTIONAL, REPLICATED, 0)
            );
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (executor != null)
            executor.shutdown();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name,
        CacheAtomicityMode atomicityMode,
        CacheMode cacheMode,
        int backups)
    {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartServersAndClients() throws Exception {
        concurrentStart(true);
    }

    /**
     * @param withClients If {@code true} also starts client nodes.
     * @throws Exception If failed.
     */
    private void concurrentStart(final boolean withClients) throws Exception {
        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            startGrid(0);

            final AtomicInteger idx = new AtomicInteger(1);

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    if (withClients)
                        client.set(ThreadLocalRandom.current().nextBoolean());

                    int nodeIdx = idx.getAndIncrement();

                    Ignite node = startGrid(nodeIdx);

                    checkNodeCaches(node, nodeIdx * 1000, 1000);

                    return null;
                }
            }, 10, "start-node");

            fut.get();

            checkCaches();

            startGrid(1000);

            checkCaches();

            stopAllGrids();

            TestDebugLog1.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches() throws Exception {
        checkAffinity();

        checkCaches0();

        checkAffinity();

        awaitPartitionMapExchange();

        checkTopologiesConsistency();

        checkCaches0();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches0() throws Exception {
        List<Ignite> nodes = G.allGrids();

        assertTrue(nodes.size() > 0);

        for (Ignite node : nodes)
            checkNodeCaches(node);
    }

    /**
     * Checks that after exchange all nodes have consistent state about partition owners.
     *
     * @throws Exception If failed.
     */
    private void checkTopologiesConsistency() throws Exception {
        List<Ignite> nodes = G.allGrids();

        IgniteEx crdNode = null;

        for (Ignite node : nodes) {
            ClusterNode locNode = node.cluster().localNode();

            if (crdNode == null || locNode.order() < crdNode.localNode().order())
                crdNode = (IgniteEx) node;
        }

        for (Ignite node : nodes) {
            IgniteEx node0 = (IgniteEx) node;

            if (node0.localNode().id().equals(crdNode.localNode().id()))
                continue;

            for (IgniteInternalCache cache : node0.context().cache().caches()) {
                int partitions = cache.context().affinity().partitions();
                for (int p = 0; p < partitions; p++) {
                    List<ClusterNode> crdOwners = crdNode.cachex(cache.name()).cache().context().topology().owners(p);

                    List<ClusterNode> owners = cache.context().topology().owners(p);

                    assertEquals(crdOwners, owners);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinity() throws Exception {
        List<Ignite> nodes = G.allGrids();

        ClusterNode crdNode = null;

        for (Ignite node : nodes) {
            ClusterNode locNode = node.cluster().localNode();

            if (crdNode == null || locNode.order() < crdNode.order())
                crdNode = locNode;
        }

        AffinityTopologyVersion topVer = ((IgniteKernal)grid(crdNode)).
            context().cache().context().exchange().readyAffinityVersion();

        Map<String, List<List<ClusterNode>>> affMap = new HashMap<>();

        for (Ignite node : nodes) {
            IgniteKernal node0 = (IgniteKernal)node;

            for (IgniteInternalCache cache : node0.context().cache().caches()) {
                List<List<ClusterNode>> aff = affMap.get(cache.name());
                List<List<ClusterNode>> aff0 = cache.context().affinity().assignments(topVer);

                if (aff != null)
                    assertEquals(aff, aff0);
                else
                    affMap.put(cache.name(), aff0);
            }
        }
    }

    /**
     * @param node Node.
     * @param startKey Start key.
     * @param keyRange Keys range.
     */
    private void checkNodeCaches(Ignite node, int startKey, int keyRange) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (String cacheName : cacheNames) {
            String err = "Invalid value [node=" + node.name() +
                ", client=" + node.configuration().isClientMode() +
                ", order=" + node.cluster().localNode().order() +
                ", cache=" + cacheName + ']';

            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (int i = 0; i < 10; i++) {
                Integer key = rnd.nextInt(keyRange) + startKey;

                cache.put(key, i);

                Object val = cache.get(key);

                assertEquals(err, i, val);
            }
        }
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void checkNodeCaches(final Ignite node) throws Exception {
        log.info("Check node caches [node=" + node.name() + ']');

        List<Future<?>> futs = new ArrayList<>();

        for (final String cacheName : cacheNames) {
            final IgniteCache<Object, Object> cache = node.cache(cacheName);

            futs.add(executor.submit(new Runnable() {
                @Override public void run() {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    assertNotNull("No cache [node=" + node.name() +
                            ", client=" + node.configuration().isClientMode() +
                            ", order=" + node.cluster().localNode().order() +
                            ", cache=" + cacheName + ']', cache);

                    String err = "Invalid value [node=" + node.name() +
                            ", client=" + node.configuration().isClientMode() +
                            ", order=" + node.cluster().localNode().order() +
                            ", cache=" + cacheName + ']';

                    for (int i = 0; i < 5; i++) {
                        Integer key = rnd.nextInt(20_000);

                        cache.put(key, i);

                        Object val = cache.get(key);

                        assertEquals(err, i, val);
                    }

                    for (int i = 0; i < 5; i++) {
                        Map<Integer, Integer> map = new TreeMap<>();

                        for (int j = 0; j < 10; j++) {
                            Integer key = rnd.nextInt(20_000);

                            map.put(key, i);
                        }

                        cache.putAll(map);

                        Map<Object, Object> res = cache.getAll(map.keySet());

                        for (Map.Entry<Integer, Integer> e : map.entrySet()) {
                            try {
                                assertEquals(err, e.getValue(), res.get(e.getKey()));
                            } catch (AssertionFailedError err0) {
                                err0.printStackTrace(System.out);
                                TestDebugLog1.addMessage("1 " + err0.getMessage());
                                TestDebugLog1.printKeyAndPartMessages("test_debug.txt", e.getKey(), node.affinity(cacheName).partition(e.getKey()), CU.cacheId(cacheName));
                                System.exit(1);
                            }
                        }
                    }

                    if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL) {
                        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                            for (TransactionIsolation isolation : TransactionIsolation.values())
                                checkNodeCaches(err, node, cache, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
                        }
                    }
                }
            }));
        }

        for (Future<?> fut : futs)
            fut.get();
    }

    /**
     * @param err Error message.
     * @param node Node.
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     */
    private void checkNodeCaches(
        String err,
        Ignite node,
        IgniteCache<Object, Object> cache,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Map<Object, Object> map = new HashMap<>();

        try {
            try (Transaction tx = node.transactions().txStart(concurrency, isolation)) {
                for (int i = 0; i < 5; i++) {
                    Integer key = rnd.nextInt(20_000);

                    cache.put(key, i);

                    Object val = cache.get(key);

                    assertEquals(i, val);

                    map.put(key, val);
                }

                tx.commit();
            }
        }
        catch (ClusterTopologyException ignore) {
            // No-op.
            ignore.printStackTrace();

            return;
        }

        for (Map.Entry<Object, Object> e : map.entrySet()) {
            try {
                assertEquals(err + " " + concurrency + " " + isolation, e.getValue(), cache.get(e.getKey()));
            } catch (AssertionFailedError err0) {
                err0.printStackTrace(System.out);
                TestDebugLog1.addMessage("2 " + node.cluster().localNode().id() + " " + err0.getMessage());
                TestDebugLog1.printKeyAndPartMessages("test_debug.txt", e.getKey(), node.affinity(cache.getName()).partition(e.getKey()), CU.cacheId(cache.getName()));
                System.exit(1);
            }
        }
    }

    /**
     * @param node Node.
     * @param vers Expected exchange versions.
     */
    private void checkExchanges(Ignite node, long... vers) {
        IgniteKernal node0 = (IgniteKernal)node;

        List<AffinityTopologyVersion> expVers = new ArrayList<>();

        for (long ver : vers)
            expVers.add(new AffinityTopologyVersion(ver));

        List<AffinityTopologyVersion> doneVers = new ArrayList<>();

        List<GridDhtPartitionsExchangeFuture> futs =
            node0.context().cache().context().exchange().exchangeFutures();

        for (int i = futs.size() - 1; i >= 0; i--) {
            GridDhtPartitionsExchangeFuture fut = futs.get(i);

            if (fut.exchangeDone() && fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT) {
                AffinityTopologyVersion resVer = fut.topologyVersion();

                if (resVer != null)
                    doneVers.add(resVer);
            }
        }

        assertEquals(expVers, doneVers);

        for (CacheGroupContext grpCtx : node0.context().cache().cacheGroups()) {
            for (AffinityTopologyVersion ver : grpCtx.affinity().cachedVersions()) {
                if (ver.minorTopologyVersion() > 0)
                    continue;

                assertTrue("Unexpected version [ver=" + ver + ", exp=" + expVers + ']',
                    expVers.contains(ver));
            }
        }
    }

    /**
     * @param node Node.
     * @param topVer Exchange version.
     * @throws Exception If failed.
     */
    private void waitForExchangeStart(final Ignite node, final long topVer) throws Exception {
        final GridCachePartitionExchangeManager exch = ((IgniteKernal)node).context().cache().context().exchange();

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return exch.lastTopologyFuture().initialVersion().topologyVersion() >= topVer;
            }
        }, 15_000);

        assertTrue(wait);
    }

    /**
     * Sequentially starts nodes so that node name is consistent with node order.
     *
     * @param node Some existing node.
     * @param startIdx Start node index.
     * @param cnt Number of nodes.
     * @return Start future.
     * @throws Exception If failed.
     */
    private IgniteInternalFuture startGridsAsync(Ignite node, int startIdx, int cnt) throws Exception {
        GridCompoundFuture fut = new GridCompoundFuture();

        for (int i = 0; i < cnt; i++) {
            final CountDownLatch latch = new CountDownLatch(1);

            node.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    log.info("Got event: " + ((DiscoveryEvent)evt).eventNode().id());

                    latch.countDown();

                    return false;
                }
            }, EventType.EVT_NODE_JOINED);

            final int nodeIdx = startIdx + i;

            IgniteInternalFuture fut0 = GridTestUtils.runAsync(new Callable() {
                @Override public Object call() throws Exception {
                    log.info("Start new node: " + nodeIdx);

                    startGrid(nodeIdx);

                    return null;
                }
            }, "start-node-" + nodeIdx);

            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS))
                fail();

            fut.add(fut0);
        }

        fut.markInitialized();

        return fut;
    }

    /**
     *
     */
    enum CoordinatorChangeMode {
        /**
         * Coordinator failed, did not send full message.
         */
        NOBODY_RCVD,

        /**
         * Coordinator failed, but new coordinator received full message
         * and finished exchange.
         */
        NEW_CRD_RCDV,

        /**
         * Coordinator failed, but one of servers (not new coordinator) received full message.
         */
        NON_CRD_RCVD
    }

    /**
     *
     */

    static class TestDelayExchangeMessagesSpi extends TestDelayingCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
            if (msg instanceof GridDhtPartitionsAbstractMessage)
                return ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null || (msg instanceof GridDhtPartitionsSingleRequest);

            return false;
        }
    }
}
