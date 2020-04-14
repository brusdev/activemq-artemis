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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import io.atomix.core.Atomix;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.PrimitiveState;
import io.atomix.utils.time.Version;
import org.apache.activemq.artemis.quorum.DistributedLock;

final class AtomixDistributedLock implements DistributedLock {

   private final Consumer<String> onClosedLock;
   private final AtomicLock atomicLock;
   private final String lockId;
   private final CopyOnWriteArrayList<LockListener> listeners;
   private boolean closed;
   private final Consumer<PrimitiveState> primitiveStateListener;
   private Version lockVersion;

   public static CompletableFuture<AtomixDistributedLock> with(Consumer<String> onClosedLock,
                                                               Atomix atomix,
                                                               String lockId,
                                                               Duration sessionTimeout) {
      return AtomixFactory.createAtomicLock(atomix, lockId, sessionTimeout)
         .thenApply(atomicLock -> new AtomixDistributedLock(onClosedLock, atomicLock, lockId));
   }

   private AtomixDistributedLock(Consumer<String> onClosedLock, AtomicLock atomicLock, String lockId) {
      this.onClosedLock = onClosedLock;
      this.atomicLock = atomicLock;
      this.lockId = lockId;
      this.closed = false;
      this.listeners = new CopyOnWriteArrayList<>();
      this.primitiveStateListener = this::onStateChanged;
      this.atomicLock.async().addStateChangeListener(primitiveStateListener);
      this.lockVersion = null;
   }

   public boolean isClosed() {
      return closed;
   }

   private void onStateChanged(PrimitiveState state) {
      switch (state) {
         case SUSPENDED:
         case EXPIRED:
         case CLOSED:
            for (LockListener listener : listeners) {
               listener.stateChanged(LockListener.EventType.UNAVAILABLE);
            }
            break;
      }
   }

   public long version() {
      checkNotClosed();
      return this.lockVersion == null ? Long.MAX_VALUE : lockVersion.value();
   }

   public boolean isWinner(long lock) {
      checkNotClosed();
      final Version version = new Version(lock);
      return this.atomicLock.isLocked(version);
   }

   private void checkNotClosed() {
      if (closed) {
         throw new IllegalStateException("The election is closed");
      }
   }

   @Override
   public String getLockId() {
      checkNotClosed();
      return lockId;
   }

   @Override
   public boolean isHeldByCaller() {
      if (lockVersion == null) {
         return false;
      }
      return this.atomicLock.isLocked(lockVersion);
   }

   @Override
   public boolean isHeld() {
      return this.atomicLock.isLocked();
   }

   @Override
   public boolean tryLock() {
      if (this.lockVersion != null) {
         throw new IllegalStateException("unlock first");
      }
      final Optional<Version> version = this.atomicLock.tryLock();
      if (!version.isPresent()) {
         return false;
      }
      this.lockVersion = version.get();
      return true;
   }

   @Override
   public void unlock() {
      if (lockVersion != null) {
         lockVersion = null;
         atomicLock.unlock();
      }
   }

   @Override
   public void addListener(LockListener listener) {
      checkNotClosed();
      listeners.add(listener);
   }

   @Override
   public void removeListener(LockListener listener) {
      checkNotClosed();
      listeners.remove(listener);
   }

   @Override
   public void close() {
      if (!closed) {
         closed = true;
         onClosedLock.accept(lockId);
         listeners.clear();
         atomicLock.async().removeStateChangeListener(primitiveStateListener);
         try {
            if (lockVersion != null) {
               lockVersion = null;
               atomicLock.unlock();
            }
         } finally {
            atomicLock.close();
         }
      }
   }

}
