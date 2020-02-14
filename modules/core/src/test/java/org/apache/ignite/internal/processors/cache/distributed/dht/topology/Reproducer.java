package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

public class Reproducer extends GridCommonAbstractTest {
    /** */
    private static final String GROUP = "group";

    /** */
    private static final List<String> CACHE_NAMES = Arrays.asList("cache1", "cache2", "cache3");

    /** Node received supply message. */
    private CountDownLatch defaultSupplyMsg = new CountDownLatch(1);

    /** Node received supply message. */
    private CountDownLatch groupSupplyMsg;

    /** Test log. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
                    if (msg instanceof GridIoMessage &&
                        ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                        GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message();

                        if (msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME))
                            defaultSupplyMsg.countDown();

                        if (msg0.groupId() == CU.cacheId(GROUP) && groupSupplyMsg != null)
                            groupSupplyMsg.countDown();
                    }

                    super.notifyListener(sndId, msg, msgC);
                }
            })
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                ));
    }
    /**
     * @throws Exception If failed.
     */
    @Test
    public void partitionsEvictionBeforeRebalanceTest1() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        stopGrid(1);

        loadData(ig0);

        IgniteEx ig1 = startGrid(1);

        U.await(defaultSupplyMsg);

        GridDhtPartitionDemander.RebalanceFuture futBeforeClusterInactive = (GridDhtPartitionDemander.RebalanceFuture)ig1.context()
            .cache().cacheGroup(CU.cacheId(GROUP)).preloader().rebalanceFuture();

        LongMetric startTime = ig1.context().metric().registry(metricName(CACHE_GROUP_METRICS_PREFIX, GROUP))
            .findMetric("RebalancingStartTime");

        assertEquals(U.<Long>field(futBeforeClusterInactive, "startTime").longValue(), startTime.value());

        ig0.cluster().state(ClusterState.INACTIVE);

        doSleep(500);

        ig0.cluster().state(ClusterState.ACTIVE);

        groupSupplyMsg = new CountDownLatch(1);

        U.await(groupSupplyMsg);

        GridDhtPartitionDemander.RebalanceFuture futAfterClasterInactive = (GridDhtPartitionDemander.RebalanceFuture)ig1.context()
            .cache().cacheGroup(CU.cacheId("group")).preloader().rebalanceFuture();

        assertEquals(U.<Long>field(futAfterClasterInactive, "startTime").longValue(), startTime.value());
    }

    /**
     * @param node Node.
     */
    private void loadData(Ignite node) {
        List<CacheConfiguration> configs = CACHE_NAMES.stream()
            .map(name -> new CacheConfiguration<>(name)
                .setGroupName(GROUP)
                .setCacheMode(CacheMode.REPLICATED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setAffinity(new RendezvousAffinityFunction())
            ).collect(Collectors.toList());

        configs.add(configs.get(0).setName(DEFAULT_CACHE_NAME).setGroupName(DEFAULT_CACHE_NAME));

        node.getOrCreateCaches(configs);

        configs.forEach(cfg -> {
            try (IgniteDataStreamer<Object, Object> streamer = node.dataStreamer(cfg.getName())) {
                for (int i = 0; i < 1_000; i++)
                    streamer.addData(i, i);

                streamer.flush();
            }
        });
    }
}
