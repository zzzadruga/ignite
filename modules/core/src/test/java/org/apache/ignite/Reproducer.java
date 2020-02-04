package org.apache.ignite;

import com.google.common.collect.Maps;
import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Reproducer extends GridCommonAbstractTest implements Serializable {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName("INDIVIDUAL")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            //.setBackups(3)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setMaxConcurrentAsyncOperations(0)
            .setAffinity(new RendezvousAffinityFunction()
                .setPartitions(1024)
                .setAffinityBackupFilter(
                    new Reproducer.ClusterNodeAttributeColocatedBackupFilter("CELL")
                ))
            .setStatisticsEnabled(true)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity()
                    .setKeyType(Long.class.getName())
                    .setFields(Maps.newLinkedHashMap(
                        F.asList(F.t("id", Long.class.getName())).stream()
                            .collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2))
                    ))
                    .setKeyFieldName("id")
                    .setValueType(UcpRecord.class.getName())));

        CacheConfiguration cfg2 = new CacheConfiguration()
            .setName("EXAMPLE")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            //.setBackups(3)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setMaxConcurrentAsyncOperations(0)
            .setAffinity(new RendezvousAffinityFunction()
                .setPartitions(1024)
                .setAffinityBackupFilter(
                    new Reproducer.ClusterNodeAttributeColocatedBackupFilter("CELL")
                ))
            .setStatisticsEnabled(true)
            .setKeyConfiguration(new CacheKeyConfiguration()
                .setTypeName(Reproducer.ExampleKey.class.getName())
                .setAffinityKeyFieldName("collocationid"))
            .setQueryEntities(
                Collections.singletonList(
                    new QueryEntity()
                        .setKeyType(ExampleKey.class.getName())
                        .setValueType(Example.class.getName())
                ));

        cfg.setCacheConfiguration(cfg1, cfg2);

        return cfg;
    }

    class Example implements Serializable {
        String value;

        public Example(String value) {
            this.value = value;
        }
    }

    class ExampleKey implements Serializable {
        long id;
        long collocationid;

        public ExampleKey(int id, long collocationid) {
            this.id = id;
            this.collocationid = collocationid;
        }

        @Override public int hashCode() {
            return 0;
        }
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    @Test
    public void testMethod() throws Exception {
        startGrids(2);
        grid(0).cluster().active(true);

        IgniteCache<Long, UcpRecord> ind = grid(0).getOrCreateCache("INDIVIDUAL");
        IgniteCache<ExampleKey, Example> ex = grid(0).getOrCreateCache("EXAMPLE");

        long keyForNode0 = keyForNode(grid(0).affinity("INDIVIDUAL"), new AtomicInteger(), grid(0).cluster().localNode());
        long keyForNode1 = keyForNode(grid(1).affinity("INDIVIDUAL"), new AtomicInteger(), grid(1).cluster().localNode());

        assertFalse(keyForNode0 == keyForNode1);

        ind.put(keyForNode0, UcpRecord.builder().setId(1).setVersion(1).build());
        ind.put(keyForNode1, UcpRecord.builder().setId(4).setVersion(1).build());

        ex.put(new ExampleKey(1, keyForNode0), new Example("val1"));
        ex.put(new ExampleKey(2, keyForNode1), new Example("val2"));
        ex.put(new ExampleKey(3, keyForNode0), new Example("val3"));
        ex.put(new ExampleKey(4, keyForNode1), new Example("val4"));

        SqlFieldsQuery query = new SqlFieldsQuery("SELECT * FROM INDIVIDUAL");

        TimeUnit.SECONDS.sleep(10000);
    }


    /**
     * This class can be used as a {@link RendezvousAffinityFunction#setAffinityBackupFilter } to create
     * cache templates in Spring that force each partition's primary and backup to be co-located on nodes with the same
     * attribute value.
     * <p>
     * This implementation will discard backups rather than place copies on nodes with different attribute values. This
     * avoids trying to cram more data onto remaining nodes when some have failed.
     * <p>
     * A node attribute to compare is provided on construction.  Note: "All cluster nodes,
     * on startup, automatically register all the environment and system properties as node attributes."
     * <p>
     * This class is constructed with a node attribute name, and a candidate node will be rejected if previously selected
     * nodes for a partition have a different value for attribute on the candidate node.
     * </pre>
     * <h2 class="header">Spring Example</h2>
     * Create a partitioned cache template plate with 1 backup, where the backup will be placed in the same cell
     * as the primary.   Note: This example requires that the environment variable "CELL" be set appropriately on
     * each node via some means external to Ignite.
     * <pre name="code" class="xml">
     * &lt;property name="cacheConfiguration"&gt;
     *     &lt;list&gt;
     *         &lt;bean id="cache-template-bean" abstract="true" class="org.apache.ignite.configuration.CacheConfiguration"&gt;
     *             &lt;property name="name" value="JobcaseDefaultCacheConfig*"/&gt;
     *             &lt;property name="cacheMode" value="PARTITIONED" /&gt;
     *             &lt;property name="backups" value="1" /&gt;
     *             &lt;property name="affinity"&gt;
     *                 &lt;bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction"&gt;
     *                     &lt;property name="affinityBackupFilter"&gt;
     *                         &lt;bean class="com.sbt.sbergrid.extras.ClusterNodeAttributeColocatedBackupFilter"&gt;
     *                             &lt;!-- Backups must go to the same CELL as primary --&gt;
     *                             &lt;constructor-arg value="CELL" /&gt;
     *                         &lt;/bean&gt;
     *                     &lt;/property&gt;
     *                 &lt;/bean&gt;
     *             &lt;/property&gt;
     *         &lt;/bean&gt;
     *     &lt;/list&gt;
     * &lt;/property&gt;
     * </pre>
     * <p>
     */
    public class ClusterNodeAttributeColocatedBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>>, Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** Attribute name. */
        private final String attributeName;

        /**
         * @param attributeName The attribute name for the attribute to compare.
         */
        public ClusterNodeAttributeColocatedBackupFilter(String attributeName) {
            this.attributeName = attributeName;
        }

        /**
         * Defines a predicate which returns {@code true} if a node is acceptable for a backup
         * or {@code false} otherwise. An acceptable node is one where its attribute value
         * is exact match with previously selected nodes.  If an attribute does not
         * exist on exactly one node of a pair, then the attribute does not match.  If the attribute
         * does not exist both nodes of a pair, then the attribute matches.
         *
         * @param candidate          A node that is a candidate for becoming a backup node for a partition.
         * @param previouslySelected A list of primary/backup nodes already chosen for a partition.
         *                           The primary is first.
         */
        @Override public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
            for (ClusterNode node : previouslySelected)
                return Objects.equals(candidate.attribute(attributeName), node.attribute(attributeName));

            return true;
        }
    }

    public static class UcpRecord implements Serializable {
        private static final long serialVersionUID = -9149281302946170855L;

        private long id;
        private int version;
        private byte[] data;
        private long flags;
        private String modelVersion;
        private Instant changeDate;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public long getFlags() {
            return flags;
        }

        public void setFlags(long flags) {
            this.flags = flags;
        }

        public String getModelVersion() {
            return modelVersion;
        }

        public void setModelVersion(String modelVersion) {
            this.modelVersion = modelVersion;
        }

        public Instant getChangeDate() {
            return changeDate;
        }

        protected void setChangeDate(Instant changeDate) {
            this.changeDate = changeDate;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private long id;
            private int version;
            private long collocationId;
            private byte[] data;
            private long flags;
            private String modelVersion;

            private Builder() {
            }

            public UcpRecord.Builder setId(long id) {
                this.id = id;
                return this;
            }

            public UcpRecord.Builder setVersion(int version) {
                this.version = version;
                return this;
            }

            public UcpRecord.Builder setCollocationId(long collocationId) {
                this.collocationId = collocationId;
                return this;
            }

            public UcpRecord.Builder setData(byte[] data) {
                this.data = data;
                return this;
            }

            public UcpRecord.Builder setFlags(long flags) {
                this.flags = flags;
                return this;
            }

            public UcpRecord.Builder setModelVersion(String modelVersion) {
                this.modelVersion = modelVersion;
                return this;
            }

            public UcpRecord build() {
                UcpRecord ucpEntity = new UcpRecord();
                ucpEntity.setId(id);
                ucpEntity.setVersion(version);
                ucpEntity.setData(data);
                ucpEntity.setFlags(flags);
                ucpEntity.setModelVersion(modelVersion);
                ucpEntity.setChangeDate(Instant.now());
                return ucpEntity;
            }
        }

        @Override
        public String toString() {
            return "UcpRecord{" +
                "id=" + id +
                ", version=" + version +
                ", flags=" + flags +
                ", modelVersion='" + modelVersion + '\'' +
                ", changeDate=" + changeDate +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UcpRecord ucpEntity = (UcpRecord) o;
            return id == ucpEntity.id &&
                version == ucpEntity.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version);
        }
    }
}
