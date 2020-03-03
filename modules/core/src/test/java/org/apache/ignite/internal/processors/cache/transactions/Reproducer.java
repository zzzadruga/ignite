package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

public class Reproducer extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static ListeningTestLogger logger;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName))
            .setGridLogger(logger);

        if (cfg.isClientMode()) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void failedToAcquireLockTest() throws Exception {
        logger = new ListeningTestLogger(false, log);

        startGridsMultiThreaded(2);

        Ignite client = startGrid("client");
        client.getOrCreateCache(CACHE_NAME);

        CountDownLatch readStartLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);
        CountDownLatch condition = new CountDownLatch(2);

        logger.registerListener(s -> {
            if ((s.contains("<" + CACHE_NAME + "> Failed to acquire lock for request: GridNearLockRequest") ||
                s.contains("Failed to acquire lock within provided timeout for transaction")))
                condition.countDown();
        });

        GridTestUtils.runAsync(() -> {
            try (final Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 1)) {
                client.cache(CACHE_NAME).put(0, 0); // Lock is owned.

                readStartLatch.countDown();

                U.awaitQuiet(commitLatch);

                tx.commit();
            }
        });

        GridTestUtils.runAsync(() -> {
            try (final Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 1000, 1)) {
                U.awaitQuiet(readStartLatch);

                client.cache(CACHE_NAME).put(0, 1); // Lock acquisition is queued.
            }
            catch (CacheException ignored) { /* No-op. */ }

            commitLatch.countDown();
        }).get();

        assertTrue(condition.await(5, TimeUnit.SECONDS));
    }
}
