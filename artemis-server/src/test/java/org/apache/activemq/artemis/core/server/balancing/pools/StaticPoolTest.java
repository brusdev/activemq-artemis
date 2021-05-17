/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing.pools;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.server.balancing.targets.MockTargetFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class StaticPoolTest {
   private static final int CHECK_PERIOD = 500;


   private final ScheduledExecutorService scheduledExecutor =
      new ScheduledThreadPoolExecutor(0);

   @Test
   public void testNoTargets() throws Exception {
      MockTargetFactory targetFactory = new MockTargetFactory();

      List<TransportConfiguration> staticConnectors = new ArrayList<>();

      Pool pool = new StaticPool(targetFactory, scheduledExecutor, CHECK_PERIOD, staticConnectors);

      Assert.assertEquals(0, pool.getTargets().size());
      Assert.assertEquals(0, pool.getAllTargets().size());
      Assert.assertEquals(0, targetFactory.getCreatedTargets().size());

      pool.start();

      try {
         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(0, pool.getAllTargets().size());
         Assert.assertEquals(0, targetFactory.getCreatedTargets().size());
      } finally {
         pool.stop();
      }
   }

   @Test
   public void testSingleTargets() throws Exception {
      MockTargetFactory targetFactory = new MockTargetFactory();

      List<TransportConfiguration> staticConnectors = new ArrayList<>();
      staticConnectors.add(new TransportConfiguration());

      Pool pool = new StaticPool(targetFactory, scheduledExecutor, CHECK_PERIOD, staticConnectors);

      Assert.assertEquals(0, pool.getTargets().size());
      Assert.assertEquals(0, pool.getAllTargets().size());
      Assert.assertEquals(0, targetFactory.getCreatedTargets().size());

      pool.start();

      try {
         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());

         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());

         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(true));

         Wait.assertEquals(1, () -> pool.getTargets().size(), pool.getCheckPeriod());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());

         targetFactory.getCreatedTargets().forEach(mockTarget -> {
            mockTarget.setConnectable(false);
            try {
               mockTarget.disconnect();
            } catch (Exception ignore) {
            }
         });

         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());

         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

         Wait.assertEquals(1, () -> pool.getTargets().size(), pool.getCheckPeriod());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());

         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(false));

         Wait.assertEquals(0, () -> pool.getTargets().size(), pool.getCheckPeriod());
         Assert.assertEquals(1, pool.getAllTargets().size());
         Assert.assertEquals(1, targetFactory.getCreatedTargets().size());
      } finally {
         pool.stop();
      }
   }

}
