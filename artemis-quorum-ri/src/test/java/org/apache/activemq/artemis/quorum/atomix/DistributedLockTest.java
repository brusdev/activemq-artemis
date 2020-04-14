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
package org.apache.activemq.artemis.quorum.atomix;

import java.io.File;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DistributedLockTest {

   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();
   private static final Address[] NODES;

   static {
      NODES = new Address[3];
      NODES[0] = Address.from("localhost:7777");
      NODES[1] = Address.from("localhost:7778");
      NODES[2] = Address.from("localhost:7779");
   }

   private DistributedPrimitiveManager createManager(Address nodeId) throws Exception {
      File tmp = tmpFolder.newFolder(nodeId.toString());
      tmp.deleteOnExit();
      final HashMap<String, String> props = new HashMap<>();
      props.put("atomic-folder", nodeId.toString());
      props.put("nodes", Stream.concat(Stream.of(nodeId.toString()), Stream.of(NODES)
         .map(Object::toString).filter(node -> !node.equals(nodeId.toString()))).collect(Collectors.joining(",")));
      return DistributedPrimitiveManager.newInstanceOf(AtomixDistributedPrimitiveManager.class.getName(), props);
   }

   @Test(expected = TimeoutException.class)
   public void singleNodeCannotStartTest() throws Throwable {
      DistributedPrimitiveManager manager = createManager(NODES[0]);
      try {
         manager.start().get(2, TimeUnit.SECONDS);
      } finally {
         manager.stop().join();
      }
   }

   @Test
   public void canJoinAndLockWithQuorumTest() throws Throwable {
      DistributedPrimitiveManager[] managers = new DistributedPrimitiveManager[2];
      managers[0] = createManager(NODES[0]);
      managers[1] = createManager(NODES[1]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final String lockId = UUID.randomUUID().toString();
      DistributedLock[] distributedLocks = Stream.of(managers).map(manager -> manager.getDistributedLock(lockId)).toArray(DistributedLock[]::new);
      for (DistributedLock distributedLock : distributedLocks) {
         Assert.assertFalse(distributedLock.isHeld());
         Assert.assertFalse(distributedLock.isHeldByCaller());
      }
      Assert.assertTrue(distributedLocks[0].tryLock());
      Assert.assertTrue(distributedLocks[0].isHeld());
      Assert.assertTrue(distributedLocks[0].isHeldByCaller());
      Assert.assertFalse(distributedLocks[1].isHeldByCaller());
      Assert.assertTrue(distributedLocks[1].isHeld());
      distributedLocks[0].unlock();
      for (DistributedLock distributedLock : distributedLocks) {
         Assert.assertFalse(distributedLock.isHeld());
         Assert.assertFalse(distributedLock.isHeldByCaller());
      }
      for (int i = 0; i < managers.length; i++) {
         distributedLocks[i].close();
      }
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop()).join();
   }

   @Test
   public void expireOwnerOnRestartWhileUnavailableTest() throws Throwable {
      DistributedPrimitiveManager[] managers = new DistributedPrimitiveManager[2];
      managers[0] = createManager(NODES[0]);
      managers[1] = createManager(NODES[1]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final String lockId = UUID.randomUUID().toString();
      final DistributedLock distributedLock = managers[0].getDistributedLock(lockId);
      final CountDownLatch notAvailable = new CountDownLatch(1);
      final DistributedLock.LockListener listener = eventType -> {
         if (eventType == DistributedLock.LockListener.EventType.UNAVAILABLE) {
            notAvailable.countDown();
         }
      };
      distributedLock.addListener(listener);
      Assert.assertTrue(distributedLock.tryLock());
      managers[1].stop().join();
      long startNotAvailable = System.currentTimeMillis();
      notAvailable.await();
      long elapsedNotAvailable = System.currentTimeMillis() - startNotAvailable;
      System.out.println("Acknowledge not available quorum tooks " + elapsedNotAvailable + " ms");
      try {
         distributedLock.close();
         Assert.fail("this should not terminate");
      } catch (Throwable t) {
         Assert.assertNotNull(t);
      }
      managers[0].stop().join();
      CompletableFuture.allOf(managers[0].start(), managers[1].start()).get(2, TimeUnit.MINUTES);
      final DistributedLock distributedLock1Again = managers[1].getDistributedLock(lockId);
      Assert.assertFalse(distributedLock1Again.isHeldByCaller());
      final DistributedLock distributedLock0 = managers[0].getDistributedLock(lockId);
      // Assert.assertTrue(((AtomixLiveElection)election0).isWinner(winnerVersion));
      System.out.println("started waiting lock to be expired");
      final long started = System.currentTimeMillis();
      while (distributedLock0.isHeld() || distributedLock1Again.isHeld()) {
         TimeUnit.MILLISECONDS.sleep(200);
      }
      final long elapsed = System.currentTimeMillis() - started;
      System.out.println("Acknowledge live isn't there anymore took " + elapsed + " ms");
      // TODO search which parameters can affect this
      distributedLock0.close();
      distributedLock1Again.close();
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop()).join();
   }

   @Test
   public void ownerCloseCauseUnlockTest() throws Throwable {
      DistributedPrimitiveManager[] managers = new DistributedPrimitiveManager[3];
      managers[0] = createManager(NODES[0]);
      managers[1] = createManager(NODES[1]);
      managers[2] = createManager(NODES[2]);
      CompletableFuture.allOf(managers[0].start(), managers[1].start(), managers[2].start()).get(2, TimeUnit.MINUTES);
      final String lockId = UUID.randomUUID().toString();
      final DistributedLock distributedLock0 = managers[0].getDistributedLock(lockId);
      Assert.assertTrue(distributedLock0.tryLock());
      distributedLock0.close();
      final DistributedLock distributedLock1 = managers[1].getDistributedLock(lockId);
      Assert.assertFalse(distributedLock1.isHeld());
      distributedLock1.close();
      {
         final DistributedLock distributedLock = managers[0].getDistributedLock(lockId);
         Assert.assertFalse(distributedLock.isHeld());
         distributedLock.close();
      }
      CompletableFuture.allOf(managers[0].stop(), managers[1].stop(), managers[2].stop()).join();
   }

}
