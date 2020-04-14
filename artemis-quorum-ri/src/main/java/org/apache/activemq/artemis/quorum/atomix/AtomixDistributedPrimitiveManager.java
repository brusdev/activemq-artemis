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
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import io.atomix.core.Atomix;
import io.atomix.utils.net.Address;
import org.apache.activemq.artemis.quorum.DistributedPrimitiveManager;
import org.apache.activemq.artemis.quorum.DistributedLock;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class AtomixDistributedPrimitiveManager implements DistributedPrimitiveManager {

   private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(10);
   private static final Duration DEFAULT_GET_LOCK_TIMEOUT = Duration.ofSeconds(4);
   private final File atomixFolder;
   private final Address address;
   private final Map<String, Address> nodes;
   private final String localMemberId;
   private final ConcurrentHashMap<String, AtomixDistributedLock> locks;
   private Atomix atomix;

   private static <T> BinaryOperator<T> throwingMerger() {
      return (u, v) -> {
         throw new IllegalStateException(String.format("Duplicate key %s", u));
      };
   }

   public AtomixDistributedPrimitiveManager(Map<String, String> args) {
      this(new File(args.get("atomic-folder")),
           Stream.of(args.get("nodes").split(","))
              .collect(toMap(
                 identity(),
                 Address::from,
                 throwingMerger(),
                 LinkedHashMap::new)));
   }

   public AtomixDistributedPrimitiveManager(File atomixFolder, LinkedHashMap<String, Address> nodes) {
      final Map.Entry<String, Address> localNode = nodes.entrySet().stream().findFirst().get();
      this.atomixFolder = atomixFolder;
      this.nodes = new HashMap<>(nodes);
      this.localMemberId = localNode.getKey();
      this.address = localNode.getValue();
      this.atomix = null;
      this.locks = new ConcurrentHashMap<>();
   }

   public AtomixDistributedPrimitiveManager(String localMemberId,
                                            Address address,
                                            File atomixFolder,
                                            Map<String, Address> nodes) {
      this.atomixFolder = atomixFolder;
      this.address = address;
      this.nodes = new HashMap<>(nodes);
      this.localMemberId = localMemberId;
      this.atomix = null;
      this.locks = new ConcurrentHashMap<>();
   }

   @Override
   public CompletableFuture<?> start() {
      if (this.atomix == null) {
         this.atomix = AtomixFactory.createAtomix(localMemberId, address, atomixFolder, nodes);
         return this.atomix.start();
      } else {
         return CompletableFuture.completedFuture(this.atomix);
      }
   }

   @Override
   public CompletableFuture<?> stop() {
      if (this.atomix != null) {
         this.locks.forEach((lockId, lock) -> {
            try {
               lock.close();
            } catch (Throwable t) {
               // TODO no op for now: log would be better!
            }
         });
         try {
            return this.atomix.stop();
         } finally {
            this.atomix = null;
         }
      } else {
         return CompletableFuture.completedFuture(null);
      }
   }

   @Override
   public DistributedLock getDistributedLock(String lockId) {
      if (this.atomix == null) {
         throw new IllegalStateException("the manager isn't started");
      }
      final AtomixDistributedLock lock = locks.get(lockId);
      if (lock != null && !lock.isClosed()) {
         return lock;
      }

      return locks.computeIfAbsent(lockId, id -> {
         try {
            return AtomixDistributedLock.with(locks::remove, atomix, id, DEFAULT_SESSION_TIMEOUT)
               .get(DEFAULT_GET_LOCK_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
         } catch (Throwable t) {
            throw new RuntimeException(t);
         }
      });
   }
}
