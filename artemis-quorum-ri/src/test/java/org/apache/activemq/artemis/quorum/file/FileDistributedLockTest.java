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
package org.apache.activemq.artemis.quorum.file;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.activemq.artemis.quorum.DistributedLock;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileDistributedLockTest {

   @Rule
   public TemporaryFolder tmpFolder = new TemporaryFolder();

   private File locksFolder;
   private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

   @Before
   public void createLocksFolder() throws IOException {
      locksFolder = tmpFolder.newFolder("locks-folder");
   }

   @After
   public void disposeManagers() {
      closeables.forEach(closeables -> {
         try {
            closeables.close();
         } catch (Throwable t) {
            // silent here
         }
      });
   }

   @Test
   public void reflectiveManagerCreation() throws Exception {
      DistributedPrimitiveManager.newInstanceOf(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", locksFolder.toString()));
   }

   @Test(expected = InvocationTargetException.class)
   public void reflectiveManagerCreationFailWithoutLocksFolder() throws Exception {
      DistributedPrimitiveManager.newInstanceOf(FileBasedPrimitiveManager.class.getName(), Collections.emptyMap());
   }

   @Test(expected = InvocationTargetException.class)
   public void reflectiveManagerCreationFailIfLocksFolderIsNotFolder() throws Exception {
      DistributedPrimitiveManager.newInstanceOf(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", tmpFolder.newFile().toString()));
   }

   private DistributedPrimitiveManager createManagedDistributeManager() {
      try {
         final DistributedPrimitiveManager manager = DistributedPrimitiveManager.newInstanceOf(FileBasedPrimitiveManager.class.getName(), Collections.singletonMap("locks-folder", locksFolder.toString()));
         closeables.add(manager);
         return manager;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Test
   public void managerReturnsSameLockIfNotClosed() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertSame(manager.getDistributedLock("a"), manager.getDistributedLock("a"));
   }

   @Test(expected = IllegalStateException.class)
   public void managerCannotGetLockIfNotStarted() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.getDistributedLock("a");
   }

   @Test(expected = NullPointerException.class)
   public void managerCannotGetLockWithNullLockId() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      manager.getDistributedLock(null);
   }

   @Test
   public void managerStopUnlockLocks() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      Assert.assertTrue(manager.getDistributedLock("a").tryLock());
      Assert.assertTrue(manager.getDistributedLock("b").tryLock());
      manager.stop();
      manager.start();
      Assert.assertFalse(manager.getDistributedLock("a").isHeld());
      Assert.assertFalse(manager.getDistributedLock("b").isHeld());
   }

   @Test
   public void acquireAndReleaseLock() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertFalse(lock.isHeld());
      Assert.assertFalse(lock.isHeldByCaller());
      Assert.assertTrue(lock.tryLock());
      Assert.assertTrue(lock.isHeld());
      Assert.assertTrue(lock.isHeldByCaller());
      lock.unlock();
      Assert.assertFalse(lock.isHeld());
      Assert.assertFalse(lock.isHeldByCaller());
   }

   @Test(expected = IllegalStateException.class)
   public void cannotAcquireSameLockTwice() {
      DistributedPrimitiveManager manager = createManagedDistributeManager();
      manager.start();
      DistributedLock lock = manager.getDistributedLock("a");
      Assert.assertTrue(lock.tryLock());
      lock.tryLock();
   }

   @Test
   public void heldLockIsVisibleByDifferentManagers() {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeld());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertTrue(observerManager.getDistributedLock("a").isHeld());
      Assert.assertFalse(observerManager.getDistributedLock("a").isHeldByCaller());
   }

   @Test
   public void unlockedLockIsVisibleByDifferentManagers() {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager observerManager = createManagedDistributeManager();
      ownerManager.start();
      observerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      ownerManager.getDistributedLock("a").unlock();
      Assert.assertFalse(observerManager.getDistributedLock("a").isHeld());
      Assert.assertFalse(observerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertFalse(ownerManager.getDistributedLock("a").isHeld());
      Assert.assertFalse(ownerManager.getDistributedLock("a").isHeldByCaller());
   }

   @Test
   public void cannotAcquireSameLockFromDifferentManagers() {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      Assert.assertFalse(notOwnerManager.getDistributedLock("a").tryLock());
   }

   @Test
   public void cannotUnlockFromNotOwnerManager() {
      DistributedPrimitiveManager ownerManager = createManagedDistributeManager();
      DistributedPrimitiveManager notOwnerManager = createManagedDistributeManager();
      ownerManager.start();
      notOwnerManager.start();
      Assert.assertTrue(ownerManager.getDistributedLock("a").tryLock());
      notOwnerManager.getDistributedLock("a").unlock();
      Assert.assertTrue(notOwnerManager.getDistributedLock("a").isHeld());
      Assert.assertFalse(notOwnerManager.getDistributedLock("a").isHeldByCaller());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeld());
      Assert.assertTrue(ownerManager.getDistributedLock("a").isHeldByCaller());
   }

}
