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

import org.apache.activemq.artemis.core.server.balancing.targets.MockTargetFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public abstract class BasePoolTest {
   private static final int DEFAULT_CHECK_PERIOD = 100;

   private final ScheduledExecutorService scheduledExecutor =
      new ScheduledThreadPoolExecutor(0);

   protected int getCheckPeriod() {
      return DEFAULT_CHECK_PERIOD;
   }

   protected ScheduledExecutorService getScheduledExecutor() {
      return scheduledExecutor;
   }

   protected abstract Pool createPool(TargetFactory targetFactory, int targets);

   @Test
   public void testTargetsWithNoTargets() throws Exception {
      testTargets(0);
   }

   @Test
   public void testTargetsWithSingleTarget() throws Exception {
      testTargets(1);
   }

   @Test
   public void testTargetsWithMultipleTargets() throws Exception {
      testTargets(10);
   }

   private void testTargets(int targets) throws Exception {
      MockTargetFactory targetFactory = new MockTargetFactory();

      Pool pool = createPool(targetFactory, targets);

      Assert.assertEquals(0, pool.getTargets().size());
      Assert.assertEquals(0, pool.getAllTargets().size());
      Assert.assertEquals(0, targetFactory.getCreatedTargets().size());

      pool.start();

      try {
         Assert.assertEquals(0, pool.getTargets().size());
         Assert.assertEquals(targets, pool.getAllTargets().size());
         Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());

         if (targets > 0) {
            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

            Assert.assertEquals(0, pool.getTargets().size());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());

            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(true));

            Wait.assertEquals(targets, () -> pool.getTargets().size(), pool.getCheckPeriod());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());

            targetFactory.getCreatedTargets().forEach(mockTarget -> {
               mockTarget.setConnectable(false);
               try {
                  mockTarget.disconnect();
               } catch (Exception ignore) {
               }
            });

            Assert.assertEquals(0, pool.getTargets().size());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());

            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

            Wait.assertEquals(targets, () -> pool.getTargets().size(), pool.getCheckPeriod());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());

            targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(false));

            Wait.assertEquals(0, () -> pool.getTargets().size(), pool.getCheckPeriod());
            Assert.assertEquals(targets, pool.getAllTargets().size());
            Assert.assertEquals(targets, targetFactory.getCreatedTargets().size());
         }
      } finally {
         pool.stop();
      }
   }
}
