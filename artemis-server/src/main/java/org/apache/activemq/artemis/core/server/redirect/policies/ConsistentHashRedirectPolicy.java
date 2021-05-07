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

package org.apache.activemq.artemis.core.server.redirect.policies;

import org.apache.activemq.artemis.core.server.redirect.RedirectController;
import org.apache.activemq.artemis.core.server.redirect.RedirectTarget;
import org.apache.activemq.artemis.core.server.redirect.RedirectingConnection;
import org.apache.activemq.artemis.core.server.redirect.pools.RedirectPool;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistentHashRedirectPolicy implements RedirectPolicy {
   private RedirectPool pool;

   @Override
   public void init(Map<String, String> properties) throws Exception {

   }

   @Override
   public void load(RedirectController controller) throws Exception {
      pool = controller.getPool();
   }

   @Override
   public void unload() throws Exception {

   }

   @Override
   public RedirectTarget selectTarget(RedirectingConnection connection) {
      if (pool.getTargets().size() > 0) {
         NavigableMap<Integer, RedirectTarget> targets = new TreeMap<>();

         for(RedirectTarget target : pool.getTargets()) {
            targets.put(target.getNodeID().hashCode(), target);
         }

         Map.Entry<Integer, RedirectTarget> redirectEntry = targets.floorEntry(connection.getUser().hashCode());

         if (redirectEntry == null) {
            redirectEntry = targets.firstEntry();
         }

         return redirectEntry != null ? redirectEntry.getValue() : null;
      }

      return null;
   }
}
