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

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

/**
 * Tests error and warn messages throttling.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentMultiScannersErrorThrottlingTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testThrottling() throws Exception {
        LT.throttleTimeout(11000);

        // Sleep for 1 min.
        Thread.sleep(60 * 1000);
    }

    /**
     * @return URI list.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        List<String> uriList = new ArrayList<>();

        uriList.add("ftp://anonymous:111111;freq=5000@unknown.host:21/pub/gg-test");
        uriList.add("ftp://anonymous:111111;freq=5000@localhost:21/pub/gg-test");

        uriList.add("http://freq=5000@localhost/tasks");
        uriList.add("http://freq=5000@unknownhost.host/tasks");

        return uriList;
    }
}
