package org.apache.ignite.internal.managers;

import java.util.regex.Pattern;

public class TestMatch {
    private static final String error = "javax.cache.CacheException: class org.apache.ignite.transactions.TransactionTimeoutException: 13Failed to acquire lock within provided timeout for transaction [timeout=2000, tx=GridNearTxLocal[xid=8e86ac9c071-00000000-0ba5-c5ca-0000-000000000002, xidVersion=GridCacheVersion [topVer=195413450, order=1583933450472, nodeOrder=2], nearXidVersion=GridCacheVersion [topVer=195413450, order=1583933450472, nodeOrder=2], concurrency=PESSIMISTIC, isolation=REPEATABLE_READ, state=ACTIVE, invalidate=false, rollbackOnly=false, nodeId=85792c5c-dcca-41e2-bd90-ce0e5a600001, timeout=2000, startTime=1583933454235, duration=2061, label=lock]], lock owner=[xid=9e86ac9c071-00000000-0ba5-c5ca-0000-000000000002, xidVer=GridCacheVersion [topVer=195413450, order=1583933450473, nodeOrder=2], nearXid=8e86ac9c071-00000000-0ba5-c5ca-0000-000000000001, nearXidVer=GridCacheVersion [topVer=195413450, order=1583933450472, nodeOrder=1], label=lock, nearNodeId=c03a1bbf-fb18-4bb9-814f-10756f600000], queue=[[xid=8e86ac9c071-00000000-0ba5-c5ca-0000-000000000002, xidVer=GridCacheVersion [topVer=195413450, order=1583933450472, nodeOrder=2], nearXid=8e86ac9c071-00000000-0ba5-c5ca-0000-000000000002, nearXidVer=GridCacheVersion [topVer=195413450, order=1583933450472, nodeOrder=2], label=lock, nearNodeId=85792c5c-dcca-41e2-bd90-ce0e5a600001]]\n" +
        "\tat org.apache.ignite.internal.processors.cache.GridCacheUtils.convertToCacheException(GridCacheUtils.java:1349)\n" +
        "\tat org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl.cacheException(IgniteCacheProxyImpl.java:2069)\n" +
        "\tat org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl.put(IgniteCacheProxyImpl.java:1305)\n" +
        "\tat org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy.put(GatewayProtectedCacheProxy.java:856)\n" +
        "\tat org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest$9.run(IgniteDiagnosticMessagesTest.java:783)\n" +
        "\tat org.apache.ignite.testframework.GridTestUtils.lambda$runAsync$1(GridTestUtils.java:1042)\n" +
        "\tat org.apache.ignite.testframework.GridTestUtils.lambda$runAsync$2(GridTestUtils.java:1098)\n" +
        "\tat org.apache.ignite.testframework.GridTestUtils$7.call(GridTestUtils.java:1419)\n" +
        "\tat org.apache.ignite.testframework.GridTestThread.run(GridTestThread.java:84)\n";

    private static final String xid0 = "8e86ac9c071-00000000-0ba5-c5ca-0000-000000000001";
    private static final String xid1 = "8e86ac9c071-00000000-0ba5-c5ca-0000-000000000002";
    private static final String nodeId0 = "c03a1bbf-fb18-4bb9-814f-10756f600000";
    private static final String nodeId1 = "85792c5c-dcca-41e2-bd90-ce0e5a600001";

    public static void main(String[] args) {
        String str = "lock owner=\\[xid=.*, xidVer=.*, nearXid=" +
            xid0 + ", nearXidVer=.*, label=lock, " + "nearNodeId=" + nodeId0 +
            "\\], queue=\\[\\[xid=" + xid1 + ", xidVer=.*, nearXid=" +
            xid1 + ", nearXidVer=.*, label=lock, nearNodeId=" +
            nodeId1 + "\\]\\]";

        Pattern ptrn = Pattern.compile(str);

        System.out.println(ptrn.matcher(error).find());
    }
}
