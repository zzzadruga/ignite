package org.apache.ignite.cache.affinity.rendezvous;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;



/**
 * Test affinity with co-located backup filter.
 */
public class ClusterNodeAttributeColocatedBackupFilterTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Colocation attr. */
    private static final String COLOCATION_ATTR = "attr";

    /** Partition count. */
    private static final int PART_CNT = 128;

    /** Backups count. */
    private static final int BACKUPS = 2;

    /** Number of cells. */
    private static final int CELLS_COUNT = 10;

    /** Number of nodes per cell. */
    private static final int NODES_PER_CELL = 1 + BACKUPS;

    /** Prefix for cell. */
    private static final String PREFIX = "CELL_AFFINITY_";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(null);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setAffinityBackupFilter(
                new ClusterNodeAttributeColocatedBackupFilter(COLOCATION_ATTR)).setPartitions(PART_CNT))
            .setBackups(BACKUPS)
        );

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * Test affinity backup filter.
     */
    @Test
    public void testAffinityBackupFilter() throws Exception {
        for (int i = 0; i < CELLS_COUNT; i++) {
            for (int j = 0; j < NODES_PER_CELL; j++) {
                startGrid(i * NODES_PER_CELL + j, PREFIX + i);
            }
        }

        waitForAffinityVersion(new AffinityTopologyVersion(CELLS_COUNT * NODES_PER_CELL, 1));

        checkPartitions();
    }

    /**
     *
     */
    private void waitForAffinityVersion(AffinityTopologyVersion ver) throws Exception {
        AffinityTopologyVersion maxVer = AffinityTopologyVersion.ZERO;

        for (int i = 0; i < 10; i++) {
            for (Ignite ignite : G.allGrids()) {
                AffinityTopologyVersion curVer = ((IgniteEx)ignite).context().discovery().topologyVersionEx();

                if (curVer.compareTo(maxVer) > 0)
                    maxVer = curVer;
            }

            if (maxVer.compareTo(ver) >= 0)
                break;

            Thread.sleep(200L);
        }

        if (maxVer.compareTo(ver) < 0)
            fail("Failed to wait for version [expVer=" + ver + ", actVer=" + maxVer + ']');

        for (Ignite ignite : G.allGrids()) {
            IgniteInternalFuture<?> exchFut =
                ((IgniteEx)ignite).context().cache().context().exchange().affinityReadyFuture(maxVer);

            if (!exchFut.isDone()) {
                try {
                    exchFut.get(3_000L);
                }
                catch (IgniteCheckedException e) {
                    fail("Failed to wait for exchange [topVer=" + maxVer + ", node=" + ignite.name() + ']');
                }
            }
        }
    }

    /**
     *
     */
    private void checkPartitions() throws Exception {
        IgniteEx client = startClientGrid();

        Map<Object, T2<List<Integer>, Collection<UUID>>> attributes = new TreeMap<>();

        for (int i = 0; i < PART_CNT; i++) {
            Collection<ClusterNode> nodes = client.affinity(CACHE_NAME).mapKeyToPrimaryAndBackups(i);

            Map<Object, Long> stat = nodes.stream().collect(Collectors.groupingBy(
                n -> n.attributes().get(COLOCATION_ATTR), Collectors.counting()));

            assertEquals("Partitions should be located on nodes from only one cell [partition: + " + i +
                ", attributes: " + stat.keySet() + "]", 1, stat.keySet().size());

            assertEquals("Partitions should be located on all nodes of the cell [node per cell: " +
                NODES_PER_CELL + ", nodes: " + nodes.stream().map(ClusterNode::consistentId).collect(Collectors.toSet())
                + "]", NODES_PER_CELL, stat.values().iterator().next().longValue());

            Object attribute = stat.keySet().iterator().next();

            List<UUID> uuids =  nodes.stream().map(ClusterNode::id).collect(Collectors.toList());

            attributes.putIfAbsent(attribute, new T2<>(new ArrayList<>(), uuids));
            attributes.get(attribute).get1().add(i);

            assertTrue(uuids.containsAll(attributes.get(attribute).get2()));
        }

        for (Map.Entry<Object, T2<List<Integer>, Collection<UUID>>> entry : attributes.entrySet()) {
            System.out.println(entry.getKey() + " nodes: " + entry.getValue()
                .get2()
                .stream()
                .map(id -> id.toString().substring(id.toString().length() - 2))
                .collect(Collectors.toCollection(TreeSet::new)) + " parts: " + entry.getValue().get1());
        }
    }

    /**
     * @param idx Index.
     * @param colocationAttrVal Co-located attribute value.
     */
    protected Ignite startGrid(int idx, String colocationAttrVal) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), optimize(getConfiguration(getTestIgniteInstanceName(idx)).setUserAttributes(
            U.map(COLOCATION_ATTR, colocationAttrVal))), null);
    }

}
