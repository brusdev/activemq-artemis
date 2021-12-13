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

package org.apache.activemq.artemis.core.server.service;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;

public class ActiveMQServiceResolver {
   private static ActiveMQServiceResolver instance;

   public static ActiveMQServiceResolver getInstance() {
      if (instance == null) {
         instance = new ActiveMQServiceResolver();
      }
      return instance;
   }

   private final Map<String, ActiveMQServiceFactory> serviceFactories = new HashMap<>();

   private ActiveMQServiceResolver() {
      ServiceLoader<ActiveMQServiceFactory> serviceLoader = ServiceLoader.load(
         ActiveMQServiceFactory.class, ActiveMQServiceResolver.class.getClassLoader());

      for (ActiveMQServiceFactory serviceFactory : serviceLoader) {
         registerService(serviceFactory);
      }
   }

   public ActiveMQService create(String serviceName) throws ClassNotFoundException {
      ActiveMQServiceFactory serviceFactory = resolve(serviceName);

      return serviceFactory.createService();
   }

   public ActiveMQServiceFactory resolve(String serviceName) throws ClassNotFoundException {
      ActiveMQServiceFactory serviceFactory = serviceFactories.get(serviceName);

      if (serviceFactory == null) {
         throw new ClassNotFoundException("No service factory found for the service " + serviceName);
      }

      return serviceFactory;
   }

   public void registerService(String serviceName, Supplier<ActiveMQService> serviceSupplier) {
      registerService(new ActiveMQServiceFactory() {
         @Override
         public String getServiceName() {
            return serviceName;
         }

         @Override
         public ActiveMQService createService() {
            return serviceSupplier.get();
         }
      });
   }

   public void registerService(ActiveMQServiceFactory serviceFactory) {
      serviceFactories.put(serviceFactory.getServiceName(), serviceFactory);
   }

   public void unregisterService(String serviceNam) {
      serviceFactories.remove(serviceNam);
   }
}
