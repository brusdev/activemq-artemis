/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.routing.policies;

import org.apache.activemq.artemis.core.server.routing.targets.*;

import java.util.*;
import java.util.regex.*;

public class StaticRoutingPolicy extends AbstractPolicy {
   public static final String NAME = "STATIC_ROUTING";

   public static final String DEFAULT_ROUTE = "DEFAULT_ROUTE";
   public static final String LOCAL_ROUTE = "LOCAL_ROUTE";
   public static final String STATIC_ROUTE_PREFIX = "STATIC_ROUTE_";

   private String defaultRouteConnectorName;
   private Pattern localRoutePattern;
   private Map<Pattern, String> staticRoutes;

   public StaticRoutingPolicy() {
      super(NAME);
   }

   protected StaticRoutingPolicy(String name) {
      super(name);
   }

   @Override
   public void init(Map<String, String> properties) {
      super.init(properties);

      staticRoutes = new HashMap<>();
      localRoutePattern = null;
      defaultRouteConnectorName = null;

      for (Map.Entry<String, String> property : properties.entrySet()) {
         if (property.getKey().startsWith(STATIC_ROUTE_PREFIX)) {
            staticRoutes.put(Pattern.compile(property.getValue()), property.getKey().substring(STATIC_ROUTE_PREFIX.length()));
         } else if (property.getKey().equals(LOCAL_ROUTE)) {
            localRoutePattern = Pattern.compile(property.getValue());
         } else if (property.getKey().equals(DEFAULT_ROUTE)) {
            defaultRouteConnectorName = property.getValue();
         }
      }
   }

   @Override
   public Target selectTarget(List<Target> targets, String key) {
      String staticRouterConnectorName = null;

      if (!targets.isEmpty()) {
         for (Map.Entry<Pattern, String> staticRoute : staticRoutes.entrySet()) {
            Matcher staticRouteMatcher = staticRoute.getKey().matcher(key);
            if (staticRouteMatcher.matches()) {
               staticRouterConnectorName = staticRoute.getValue();
               break;
            }
         }

         if (localRoutePattern != null) {
            Matcher matcher = localRoutePattern.matcher(key);
            if (matcher.matches()) {
               staticRouterConnectorName = LOCAL_ROUTE;
            }
         }

         if (staticRouterConnectorName == null) {
            staticRouterConnectorName = defaultRouteConnectorName;
         }

         if (staticRouterConnectorName != null) {
            for (Target target : targets) {
               if ((staticRouterConnectorName == LOCAL_ROUTE && target.isLocal()) ||
                       (staticRouterConnectorName.equals(target.getConnector().getName()))) {
                  return target;
               }
            }
         }
      }

      return null;
   }
}
