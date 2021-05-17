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
import org.apache.activemq.artemis.core.server.balancing.targets.MockTargetTask;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DiscoveryPoolTest extends BasePoolTest {

   @Override
   protected Pool createPool(TargetFactory targetFactory, int targets) {
      MockDiscoveryService discoveryService = new MockDiscoveryService();

      for (int i = 0; i < targets; i++) {
         discoveryService.addEntry();
      }

      return createDiscoveryPool(targetFactory, discoveryService);
   }

   private Pool createDiscoveryPool(TargetFactory targetFactory, DiscoveryService discoveryService) {
      return new DiscoveryPool(targetFactory, getScheduledExecutor(), getCheckPeriod(), discoveryService);
   }


   @Test
   public void testTargetsAddingRemovingAllEntries() throws Exception {
      testTargetsChangingEntries(5, 10, 10);
   }

   @Test
   public void testTargetsAddingRemovingPartialEntries() throws Exception {
      testTargetsChangingEntries(5, 10, 5);
   }

   @Test
   public void testTargetsAddingRemovingAllEntriesAfterStart() throws Exception {
      testTargetsChangingEntries(0, 10, 10);
   }

   @Test
   public void testTargetsAddingRemovingPartialEntriesAfterStart() throws Exception {
      testTargetsChangingEntries(0, 10, 5);
   }

   private void testTargetsChangingEntries(int initialEntries, int addingEntries, int removingEntries) throws Exception {
      MockTargetFactory targetFactory = new MockTargetFactory();
      MockTargetTask targetTask = new MockTargetTask("TEST", true);
      MockDiscoveryService discoveryService = new MockDiscoveryService();

      targetTask.setExecutable(true);

      // Simulate initial entries.
      List<String> initialNodeIDs = new ArrayList<>();
      for (int i = 0; i < initialEntries; i++) {
         initialNodeIDs.add(discoveryService.addEntry());
      }

      Pool pool = createDiscoveryPool(targetFactory, discoveryService);

      pool.addTargetTask(targetTask);

      pool.start();

      try {
         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));
         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(true));

         Wait.assertEquals(initialEntries, () -> pool.getTargets().size(), getCheckTimeout());
         Assert.assertEquals(initialEntries, pool.getAllTargets().size());
         Assert.assertEquals(initialEntries, targetFactory.getCreatedTargets().size());
         initialNodeIDs.forEach(nodeID -> Assert.assertTrue(pool.checkTargetReady(nodeID)));

         // Simulate adding entries.
         List<String> addedNodeIDs = new ArrayList<>();
         for (int i = 0; i < addingEntries; i++) {
            addedNodeIDs.add(discoveryService.addEntry());
         }

         Assert.assertEquals(initialEntries, pool.getTargets().size());
         Assert.assertEquals(initialEntries + addingEntries, pool.getAllTargets().size());
         Assert.assertEquals(initialEntries + addingEntries, targetFactory.getCreatedTargets().size());
         initialNodeIDs.forEach(nodeID -> {
            Assert.assertTrue(pool.checkTargetReady(nodeID));
            Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
         });
         addedNodeIDs.forEach(nodeID -> {
            Assert.assertFalse(pool.checkTargetReady(nodeID));
            Assert.assertEquals(0, targetTask.getTargetExecutions(pool.getTarget(nodeID)));
         });


         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setConnectable(true));

         Assert.assertEquals(initialEntries, pool.getTargets().size());
         Assert.assertEquals(initialEntries + addingEntries, pool.getAllTargets().size());
         Assert.assertEquals(initialEntries + addingEntries, targetFactory.getCreatedTargets().size());
         initialNodeIDs.forEach(nodeID -> {
            Assert.assertTrue(pool.checkTargetReady(nodeID));
            Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
         });
         addedNodeIDs.forEach(nodeID -> {
            Assert.assertFalse(pool.checkTargetReady(nodeID));
            Assert.assertEquals(0, targetTask.getTargetExecutions(pool.getTarget(nodeID)));
         });

         targetFactory.getCreatedTargets().forEach(mockTarget -> mockTarget.setReady(true));

         Wait.assertEquals(initialEntries + addingEntries, () -> pool.getTargets().size(), getCheckTimeout());
         Assert.assertEquals(initialEntries + addingEntries, pool.getAllTargets().size());
         Assert.assertEquals(initialEntries + addingEntries, targetFactory.getCreatedTargets().size());
         initialNodeIDs.forEach(nodeID -> {
            Assert.assertTrue(pool.checkTargetReady(nodeID));
            Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
         });
         addedNodeIDs.forEach(nodeID -> {
            Assert.assertTrue(pool.checkTargetReady(nodeID));
            Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
         });

         if (removingEntries > 0) {
            // Simulate removing entries.
            List<String> entries = new ArrayList<>(discoveryService.getEntries().keySet());
            for (int i = 0; i < removingEntries; i++) {
               discoveryService.removeEntry(entries.get(0));
            }

            Assert.assertEquals(initialEntries + addingEntries, pool.getTargets().size());
            Assert.assertEquals(initialEntries + addingEntries, pool.getAllTargets().size());
            Assert.assertEquals(initialEntries + addingEntries, targetFactory.getCreatedTargets().size());
            initialNodeIDs.forEach(nodeID -> {
               Assert.assertTrue(pool.checkTargetReady(nodeID));
               Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
            });
            addedNodeIDs.forEach(nodeID -> {
               Assert.assertTrue(pool.checkTargetReady(nodeID));
               Assert.assertTrue(targetTask.getTargetExecutions(pool.getTarget(nodeID)) > 0);
            });
         }
      } finally {
         pool.stop();
      }
   }
}
