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

package org.apache.activemq.artemis.core.server.balancing.caches;

import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedKeyValuePair;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalCacheTest {
   private static final String CACHE_NAME = "TEST";
   private static final int CACHE_TIMEOUT = 500;
   private static final String CACHE_ENTRY_KEY = "TEST_KEY";
   private static final String CACHE_ENTRY_VALUE = "TEST_VALUE";

   @Test
   public void testValidEntry() {
      LocalCache cache = new LocalCache();
      cache.init(Collections.singletonMap(Cache.CACHE_ID_PROPERTY_NAME, CACHE_NAME));
      cache.start(null);

      try {
         cache.put(CACHE_ENTRY_KEY, CACHE_ENTRY_VALUE);
         Assert.assertEquals(CACHE_ENTRY_VALUE, cache.get(CACHE_ENTRY_KEY));
      } finally {
         cache.stop();
      }
   }

   @Test
   public void testExpiration() throws Exception {
      Map<String, String> cacheProperties = new HashMap<>();
      cacheProperties.put(Cache.CACHE_ID_PROPERTY_NAME, CACHE_NAME);
      cacheProperties.put(Cache.CACHE_TIMEOUT_PROPERTY_NAME, Integer.toString(CACHE_TIMEOUT));

      LocalCache cache = new LocalCache();
      cache.init(cacheProperties);
      cache.start(null);

      try {
         cache.put(CACHE_ENTRY_KEY, CACHE_ENTRY_VALUE);
         Assert.assertEquals(CACHE_ENTRY_VALUE, cache.get(CACHE_ENTRY_KEY));
         Wait.assertTrue(() -> cache.get(CACHE_ENTRY_KEY) == null, CACHE_TIMEOUT * 2, 100);
      } finally {
         cache.stop();
      }
   }

   @Test
   public void testPersistedEntry() {
      ActiveMQServer server = Mockito.mock(ActiveMQServer.class);
      StorageManager storageManager = new DummyKeyValuePairStorageManager();

      Mockito.when(server.getStorageManager()).thenReturn(storageManager);

      Map<String, String> cacheProperties = new HashMap<>();
      cacheProperties.put(Cache.CACHE_ID_PROPERTY_NAME, CACHE_NAME);
      cacheProperties.put(Cache.CACHE_PERSISTED_PROPERTY_NAME, Boolean.toString(true));

      LocalCache cacheBeforeStop = new LocalCache();
      cacheBeforeStop.init(cacheProperties);
      cacheBeforeStop.start(server);

      try {
         cacheBeforeStop.put(CACHE_ENTRY_KEY, CACHE_ENTRY_VALUE);
         Assert.assertEquals(CACHE_ENTRY_VALUE, cacheBeforeStop.get(CACHE_ENTRY_KEY));
      } finally {
         cacheBeforeStop.stop();
      }

      Assert.assertEquals(CACHE_ENTRY_VALUE, storageManager.getPersistedKeyValuePairs(CACHE_NAME).get(CACHE_ENTRY_KEY).getValue());

      LocalCache cacheAfterStop = new LocalCache();
      cacheAfterStop.init(cacheProperties);
      cacheAfterStop.start(server);

      try {
         Assert.assertEquals(CACHE_ENTRY_VALUE, cacheAfterStop.get(CACHE_ENTRY_KEY));
      } finally {
         cacheAfterStop.stop();
      }

      Assert.assertEquals(CACHE_ENTRY_VALUE, storageManager.getPersistedKeyValuePairs(CACHE_NAME).get(CACHE_ENTRY_KEY).getValue());
   }

   @Test
   public void testPersistedExpiration() throws Exception {
      ActiveMQServer server = Mockito.mock(ActiveMQServer.class);
      StorageManager storageManager = new DummyKeyValuePairStorageManager();

      Mockito.when(server.getStorageManager()).thenReturn(storageManager);

      Map<String, String> cacheProperties = new HashMap<>();
      cacheProperties.put(Cache.CACHE_ID_PROPERTY_NAME, CACHE_NAME);
      cacheProperties.put(Cache.CACHE_PERSISTED_PROPERTY_NAME, Boolean.toString(true));
      cacheProperties.put(Cache.CACHE_TIMEOUT_PROPERTY_NAME, Integer.toString(CACHE_TIMEOUT));

      LocalCache cacheBeforeStop = new LocalCache();
      cacheBeforeStop.init(cacheProperties);
      cacheBeforeStop.start(server);

      try {
         cacheBeforeStop.put(CACHE_ENTRY_KEY, CACHE_ENTRY_VALUE);
         Assert.assertEquals(CACHE_ENTRY_VALUE, cacheBeforeStop.get(CACHE_ENTRY_KEY));
      } finally {
         cacheBeforeStop.stop();
      }

      Assert.assertEquals(CACHE_ENTRY_VALUE, storageManager.getPersistedKeyValuePairs(CACHE_NAME).get(CACHE_ENTRY_KEY).getValue());

      LocalCache cacheAfterStop = new LocalCache();
      cacheAfterStop.init(cacheProperties);
      cacheAfterStop.start(server);

      try {
         Assert.assertEquals(CACHE_ENTRY_VALUE, cacheAfterStop.get(CACHE_ENTRY_KEY));
         Thread.sleep(CACHE_TIMEOUT * 2);
         Assert.assertNull(cacheAfterStop.get(CACHE_ENTRY_KEY));
      } finally {
         cacheAfterStop.stop();
      }

      Assert.assertNull(storageManager.getPersistedKeyValuePairs(CACHE_NAME).get(CACHE_ENTRY_KEY));
   }

   static class DummyKeyValuePairStorageManager extends NullStorageManager {
      private Map<String, Map<String, PersistedKeyValuePair>> mapPersistedKeyValuePairs = new ConcurrentHashMap<>();

      @Override
      public void storeKeyValuePair(PersistedKeyValuePair persistedKeyValuePair) throws Exception {
         Map<String, PersistedKeyValuePair> persistedKeyValuePairs = mapPersistedKeyValuePairs.get(persistedKeyValuePair.getMapId());
         if (persistedKeyValuePairs == null) {
            persistedKeyValuePairs = new HashMap<>();
            mapPersistedKeyValuePairs.put(persistedKeyValuePair.getMapId(), persistedKeyValuePairs);
         }
         persistedKeyValuePairs.put(persistedKeyValuePair.getKey(), persistedKeyValuePair);
      }

      @Override
      public void deleteKeyValuePair(String mapId, String key) throws Exception {
         Map<String, PersistedKeyValuePair> persistedKeyValuePairs = mapPersistedKeyValuePairs.get(mapId);
         if (persistedKeyValuePairs != null) {
            persistedKeyValuePairs.remove(key);
         }
      }

      @Override
      public Map<String, PersistedKeyValuePair> getPersistedKeyValuePairs(String mapId) {
         Map<String, PersistedKeyValuePair> persistedKeyValuePairs = mapPersistedKeyValuePairs.get(mapId);
         return persistedKeyValuePairs != null ? new HashMap<>(persistedKeyValuePairs) : new HashMap<>();
      }
   }
}
