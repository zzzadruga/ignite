package org.apache.ignite;/*
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

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * TODO
 if (!cacheMsg.partitionExchangeMessage() && !(cacheMsg instanceof GridDhtPartitionDemandMessage) && !(cacheMsg instanceof GridDhtPartitionSupplyMessage))
 TestDebugLog.addMessage("Message: " + cacheMsg);
 */
public class TestDebugLog1 {
    public static volatile boolean DEBUG;

    public static void debugLog(IgniteLogger log, String msg) {
        if (DEBUG)
            log.info(msg);
    }

    /** */
    private static final List<Object> msgs = Collections.synchronizedList(new ArrayList<>(500_000));

    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    static class Message {
        String thread = Thread.currentThread().getName();

        String msg;

        long ts = U.currentTimeMillis();

        public Message(String msg) {
            this.msg = msg;
        }

        public String toString() {
            return "Msg [thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ", msg=" + msg + ']';
        }
    }

    static class PageMessage extends Message {
        long pageId;

        int bucket;

        public PageMessage(String msg, long pageId, int bucket) {
            super(msg);
            this.bucket = bucket;
            this.pageId = pageId;
        }

        public String toString() {
            return "EntryMsg [pageId=" + pageId + ", bucket=" + bucket + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static class BucketMessage extends Message {
        int bucket;

        public BucketMessage(String msg, int bucket) {
            super(msg);
            this.bucket = bucket;
        }

        public String toString() {
            return "BucketMsg [bucket=" + bucket + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static class EntryMessage extends Message {
        Object key;
        Object val;
        int cacheId;

        public EntryMessage(int cacheId, Object key, Object val, String msg) {
            super(msg);

            this.cacheId = cacheId;
            this.key = key;
            this.val = val;
        }

        public String toString() {
            return "EntryMsg [key=" + key + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) +
                    ", val=" + val +']';
        }
    }
    static class PartMessage extends Message {
        private final int cacheId;
        private final int partId;
        private Object val;

        public PartMessage(int cacheId, int partId, Object val, String msg) {
            super(msg);

            this.cacheId = cacheId;
            this.partId = partId;
            this.val = val;
            this.msg = msg;
        }

        boolean match(int cacheId, int partId) {
            return this.cacheId == cacheId && this.partId == partId;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartMessage partKey = (PartMessage) o;

            if (cacheId != partKey.cacheId) return false;
            return partId == partKey.partId;

        }

        @Override public int hashCode() {
            int result = cacheId;
            result = 31 * result + partId;
            return result;
        }

        public String toString() {
            return "PartMessage [partId=" + partId +
                    ", val=" + val +
                    ", msg=" + msg +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) +
                    ", cacheId=" + cacheId + ']';
        }
    }

    static final boolean out = false;

    public static void addPageMessage(long pageId, String msg) {
        //msgs.add(new PageMessage(msg, pageId, -1));
    }

    public static void addBucketMessage(int bucket, String msg) {
        //msgs.add(new BucketMessage(msg, bucket));
    }

    public static void addPageMessage(long pageId, int bucket, String msg) {
        //msgs.add(new PageMessage(msg, pageId, bucket));
    }

    static final boolean disabled = false;

    public static void addMessage(String msg) {
        if (disabled)
            return;

        msgs.add(new Message(msg));

        if (out)
            System.out.println(msg);
    }

    public static void addCacheEntryMessage(GridCacheMapEntry e, CacheObject val, String msg) {
        addEntryMessage(e.context().cacheId(), e.key().value(e.context().cacheObjectContext(), false),
                val != null ? val.value(e.context().cacheObjectContext(), false) : null, msg);
    }

    public static void addCacheEntryMessage(GridCacheMapEntry e, Object val, String msg) {
        addEntryMessage(e.context().cacheId(), e.key().value(e.context().cacheObjectContext(), false), val, msg);
    }

    public static void addEntryMessage(int cacheId, Object key, Object val, String msg) {
        if (cacheId != testCache)
            return;
        //        if (!(val instanceof TcpDiscoveryHeartbeatMessage || val instanceof TcpDiscoveryCustomEventMessage || val instanceof TcpDiscoveryCustomEventMessage))
//            return;

//        if (val instanceof TcpDiscoveryHeartbeatMessage || val instanceof TcpDiscoveryCustomEventMessage || val instanceof TcpDiscoveryCustomEventMessage)
//            return;
//
        if (disabled)
            return;

        if (key instanceof KeyCacheObject)
            key = ((KeyCacheObject) key).value(null, false);

        assert key != null;

//        if (val instanceof CacheObject)
//            val = ((CacheObject) val).value(null, false);

        EntryMessage msg0 = new EntryMessage(cacheId, key, val, msg);

        msgs.add(msg0);

        if (out)
            System.out.println(msg0.toString());
    }

    public static void printMessages(String fileName) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            // msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0)
                    w.println(msg.toString());

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0)
                System.out.println(msg);
        }
    }

    private static final int testCache = CU.cacheId("c8");

    public static void addPartMessage(int cacheId,
                                      int partId,
                                      Object val,
                                      String msg) {
        if (cacheId != testCache)
            return;

        msgs.add(new PartMessage(cacheId, partId, val, msg));
    }

    public static void printPartMessages(String fileName, int cacheId, int partId) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof PartMessage && !((PartMessage)msg).match(cacheId, partId))
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof PartMessage && !((PartMessage)msg).match(cacheId, partId))
                    continue;

                System.out.println(msg);
            }
        }
    }

    public static void printPageMessages(String fileName, long pageId) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof PageMessage && ((PageMessage)msg).pageId != pageId)
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof PageMessage && ((PageMessage)msg).pageId != pageId)
                    continue;

                System.out.println(msg);
            }
        }
    }

    public static void printBucketMessages(String fileName, int bucketId) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        try {
            FileOutputStream out = new FileOutputStream(fileName);

            PrintWriter w = new PrintWriter(out);

            for (Object msg : msgs0) {
                if (msg instanceof BucketMessage && ((BucketMessage)msg).bucket != bucketId)
                    continue;

                w.println(msg.toString());
            }

            w.close();

            out.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void printKeyMessages(String fileName, Object key) {
        printKeyMessages(fileName, Collections.singleton(key));
    }

    public static void printKeyMessages(String fileName, Set<Object> keys) {
        assert !F.isEmpty(keys);

        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !(keys.contains(((EntryMessage)msg).key)))
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof EntryMessage && !(keys.contains(((EntryMessage)msg).key)))
                    continue;

                EntryMessage msg0 = (EntryMessage)msg;

                if (msg0.val instanceof Exception) {
                    System.out.print(msg0.thread + ", msg=" + msg0.msg + ", ");

                    ((Exception)msg0.val).printStackTrace(System.out);
                }
                else
                    System.out.println(msg);
            }
        }
    }

    public static void printKeyAndPartMessages(String fileName, Object key, int partId, int cacheId) {
        assert key != null;

        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            //msgs.clear();
        }

        if (fileName != null) {
            try {
                FileOutputStream out = new FileOutputStream(fileName);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                        continue;

                    if (msg instanceof PartMessage) {
                        PartMessage pm = (PartMessage)msg;

                        if (pm.cacheId != cacheId || pm.partId != partId)
                            continue;
                    }

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                    continue;

                System.out.println(msg);
            }
        }
    }

    public static void clear() {
        msgs.clear();
    }

    public static void main(String[] args) {
    }
}