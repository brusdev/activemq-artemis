/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Test;

public class ClusterRedeployTest extends ClusterTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testRedeployConfigDelete() throws Throwable {
      Path brokerXML = getTestDirfile().toPath().resolve("broker.xml");
      URL redeployConfigDelete = ClusterRedeployTest.class.getClassLoader().getResource("reload-config-delete.xml");
      Files.copy(redeployConfigDelete.openStream(), brokerXML);

      // Setup servers.
      setupLiveServer(0, isFileStorage(), true, isNetty(), false, brokerXML.toUri().toURL());
      setupLiveServer(1, isFileStorage(), true, isNetty(), false);

      // Setup cluster.
      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);

      startServers(0, 1);

      ReusableLatch latch = new ReusableLatch(1);
      Runnable tick = latch::countDown;

      getServer(0).getReloadManager().setTick(tick);
      latch.await(10, TimeUnit.SECONDS);


      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      send(0, "queues.testaddress", 100, false, null);

      verifyReceiveAll(100, 0);

      brokerXML.toFile().setLastModified(System.currentTimeMillis() + 1000);

      latch.setCount(1);
      getServer(0).getReloadManager().setTick(tick);
      latch.await(10, TimeUnit.SECONDS);

      /*
      createQueue(0, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);
      */

      send(0, "queues.testaddress", 100, false, null);

      verifyReceiveAll(100, 0);
   }
}
