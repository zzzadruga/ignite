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

package org.apache.ignite.events;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid deployment event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * @see IgniteEventType#EVT_CLASS_DEPLOY_FAILED
 * @see IgniteEventType#EVT_CLASS_DEPLOYED
 * @see IgniteEventType#EVT_CLASS_UNDEPLOYED
 * @see IgniteEventType#EVT_TASK_DEPLOY_FAILED
 * @see IgniteEventType#EVT_TASK_DEPLOYED
 * @see IgniteEventType#EVT_TASK_UNDEPLOYED
 * @see IgniteEventType#EVTS_DEPLOYMENT
 */
public class IgniteDeploymentEvent extends IgniteEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String alias;

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + (alias != null ? ": " + alias : "");
    }

    /**
     * No-arg constructor.
     */
    public IgniteDeploymentEvent() {
        // No-op.
    }

    /**
     * Creates deployment event with given parameters.
     *
     * @param node Node.
     * @param msg Optional event message.
     * @param type Event type.
     */
    public IgniteDeploymentEvent(ClusterNode node, String msg, int type) {
        super(node, msg, type);
    }

    /**
     * Gets deployment alias for this event.
     *
     * @return Deployment alias.
     */
    public String alias() {
        return alias;
    }

    /**
     * Sets deployment alias for this event.
     *
     * @param alias Deployment alias.
     */
    public void alias(String alias) {
        this.alias = alias;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDeploymentEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
