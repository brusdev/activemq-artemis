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
package org.apache.activemq.artemis.tests.integration.mqtt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeTestAccessor;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test to reproduce the issue where auto-delete addresses are incorrectly deleted
 * when they have wildcard queue bindings.
 */
public class MqttWildcardAutoDeleteAddressTest extends MQTTTestSupport {

   private MqttClient subscriber;
   private MqttClient producer;

   @AfterEach
   public void cleanup() throws MqttException {
      if (subscriber != null && subscriber.isConnected()) {
         subscriber.disconnect();
         subscriber.close();
      }
      if (producer != null && producer.isConnected()) {
         producer.disconnect();
         producer.close();
      }
   }

   @Override
   public void configureBroker() throws Exception {
      super.configureBroker();

      // Configure auto-delete for TEST.# addresses
      AddressSettings addressSettings = new AddressSettings()
         .setAutoDeleteAddresses(true)
         .setAutoCreateQueues(true)
         .setAutoCreateAddresses(true);

      server.getAddressSettingsRepository().addMatch("TEST.#", addressSettings);
   }

   /**
    * This test reproduces the issue where:
    * 1. An MQTT durable subscriber creates a wildcard queue (TEST.#)
    * 2. Messages are sent to a specific topic (TEST.1), creating address TEST.1
    * 3. The wildcard queue receives messages from TEST.1 (proving it's bound)
    * 4. After broker restart, address TEST.1 is deleted even though queue TEST.# should still be bound to it
    *
    * This indicates either:
    * - A console bug showing incorrect bindings, OR
    * - An auto-delete bug not respecting queue bindings
    */
   @Test
   public void testWildcardQueueBindingPreventsAutoDelete() throws Exception {
      final int messageCount = 10;
      final String subscriberId = "mqtt-subscriber";
      final String producerId = "mqtt-producer";
      final String mqttWildcardTopic = "TEST/#";
      final String mqttSpecificTopic = "TEST/1";

      // Step 1: Create MQTT durable subscriber with wildcard queue
      subscriber = createDurableMqttClient(subscriberId);
      subscriber.subscribe(mqttWildcardTopic, 1);

      // Verify wildcard address and queue were created
      SimpleString wildcardAddress = SimpleString.of(MQTTUtil.getCoreAddressFromMqttTopic(mqttWildcardTopic, server.getConfiguration().getWildcardConfiguration()));
      assertNotNull(server.getAddressInfo(wildcardAddress), "Wildcard address TEST.# should exist");

      // MQTT queue naming: clientId + "." + topic (with / replaced by .)
      String queueName = subscriberId + ".TEST.#";
      Queue wildcardQueue = server.locateQueue(SimpleString.of(queueName));
      assertNotNull(wildcardQueue, "Wildcard queue should exist");

      // Step 2: Send messages to specific topic
      producer = createDurableMqttClient(producerId);
      for(int i = 0; i < messageCount; i++) {
         MqttMessage message = new MqttMessage(("test message " + i).getBytes());
         message.setQos(1);
         producer.publish(mqttSpecificTopic, message);
      }

      // Verify specific address was created
      SimpleString specificAddress = SimpleString.of(MQTTUtil.getCoreAddressFromMqttTopic(mqttSpecificTopic, server.getConfiguration().getWildcardConfiguration()));
      AddressInfo addressInfo = server.getAddressInfo(specificAddress);
      assertNotNull(addressInfo, "Specific address should exist after sending messages");

      // Verify the wildcard queue has messages (proving it receives from specific topic)
      assertTrue(wildcardQueue.getMessageCount() > 0, "Wildcard queue should have received message from " + mqttSpecificTopic);

      subscriber.disconnect();
      producer.disconnect();

      // Print diagnostic information BEFORE restart
      System.out.println("\n=== BEFORE RESTART ===");
      printAddressesAndQueues(server);

      // Step 3: Restart broker
      server.stop();
      server.start();
      server.waitForActivation(10, TimeUnit.SECONDS);

      // Print diagnostic information AFTER restart
      System.out.println("\n=== AFTER RESTART ===");
      printAddressesAndQueues(server);

      // Step 4: Verify addresses after restart
      // The wildcard address TEST.# should still exist (has durable queue)
      assertNotNull(server.getAddressInfo(wildcardAddress), "Wildcard address TEST.# should survive restart");

      // The wildcard queue should still exist (durable)
      assertNotNull(server.locateQueue(SimpleString.of(queueName)), "Durable wildcard queue should survive restart");

      // The specific address should NOT be auto-deleted if the wildcard queue is bound to it
      // This is the bug: specific address gets deleted even though TEST.# queue should be bound to it
      AddressInfo specificAddressAfterRestart = server.getAddressInfo(specificAddress);

      // This assertion should pass if bindings are working correctly
      // If it fails, it indicates the bug: auto-delete is not respecting wildcard queue bindings
      assertNull(specificAddressAfterRestart,
         "Specific address should NOT be auto-deleted because wildcard queue TEST.# should be bound to it. " +
         "If this fails, it indicates either: " +
         "1) Console bug - queue TEST.# is not actually bound to specific address (only to TEST.#), or " +
         "2) Auto-delete bug - auto-delete is not checking wildcard queue bindings correctly");

      final CountDownLatch afterRestartLatch = new CountDownLatch(messageCount);

      subscriber = createDurableMqttClient(subscriberId);
      subscriber.setCallback(new MqttCallback() {
         @Override
         public void connectionLost(Throwable cause) {
         }

         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            System.out.println("AFTER RESTART - Received message on topic: " + topic + ", payload: " + new String(message.getPayload()));
            afterRestartLatch.countDown();
         }

         @Override
         public void deliveryComplete(IMqttDeliveryToken token) {
         }
      });
      subscriber.subscribe(mqttWildcardTopic, 1);

      assertTrue(afterRestartLatch.await(5000, TimeUnit.MILLISECONDS));
   }

   /**
    * This test is similar to testWildcardQueueBindingPreventsAutoDelete but instead of restarting
    * the broker, it triggers the reaper thread manually. This isolates the auto-delete behavior
    * from restart-related cleanup.
    */
   @Test
   public void testWildcardQueueBindingPreventsAutoDeleteWithReaper() throws Exception {
      final int messageCount = 10;
      final String subscriberId = "mqtt-subscriber-reaper";
      final String producerId = "mqtt-producer-reaper";
      final String mqttWildcardTopic = "TEST/#";
      final String mqttSpecificTopic = "TEST/2";

      // Step 1: Create MQTT durable subscriber with wildcard queue
      subscriber = createDurableMqttClient(subscriberId);
      subscriber.subscribe(mqttWildcardTopic, 1);

      // Verify wildcard address and queue were created
      SimpleString wildcardAddress = SimpleString.of(MQTTUtil.getCoreAddressFromMqttTopic(mqttWildcardTopic, server.getConfiguration().getWildcardConfiguration()));
      assertNotNull(server.getAddressInfo(wildcardAddress), "Wildcard address TEST.# should exist");

      // MQTT queue naming: clientId + "." + topic (with / replaced by .)
      String queueName = subscriberId + ".TEST.#";
      Queue wildcardQueue = server.locateQueue(SimpleString.of(queueName));
      assertNotNull(wildcardQueue, "Wildcard queue should exist");

      // Step 2: Send messages to specific topic
      producer = createDurableMqttClient(producerId);
      for(int i = 0; i < messageCount; i++) {
         MqttMessage message = new MqttMessage(("test message " + i).getBytes());
         message.setQos(1);
         producer.publish(mqttSpecificTopic, message);
      }

      // Verify specific address was created
      SimpleString specificAddress = SimpleString.of(MQTTUtil.getCoreAddressFromMqttTopic(mqttSpecificTopic, server.getConfiguration().getWildcardConfiguration()));
      AddressInfo addressInfo = server.getAddressInfo(specificAddress);
      assertNotNull(addressInfo, "Specific address should exist after sending messages");

      // Verify the wildcard queue has messages (proving it receives from specific topic)
      assertTrue(wildcardQueue.getMessageCount() > 0, "Wildcard queue should have received message from " + mqttSpecificTopic);

      subscriber.disconnect();
      producer.disconnect();

      // Print diagnostic information BEFORE reaper
      System.out.println("\n=== BEFORE REAPER ===");
      printAddressesAndQueues(server);

      // Step 3: Trigger the reaper thread instead of restarting broker
      PostOfficeTestAccessor.sweepAndReapAddresses((PostOfficeImpl) server.getPostOffice());

      // Print diagnostic information AFTER reaper
      System.out.println("\n=== AFTER REAPER ===");
      printAddressesAndQueues(server);

      // Step 4: Verify addresses after reaper
      // The wildcard address TEST.# should still exist (has durable queue)
      assertNotNull(server.getAddressInfo(wildcardAddress), "Wildcard address TEST.# should still exist after reaper");

      // The wildcard queue should still exist (durable)
      assertNotNull(server.locateQueue(SimpleString.of(queueName)), "Durable wildcard queue should still exist after reaper");

      // The specific address should NOT be auto-deleted if the wildcard queue is bound to it
      // This is the bug: specific address gets deleted even though TEST.# queue should be bound to it
      AddressInfo specificAddressAfterReaper = server.getAddressInfo(specificAddress);

      // This assertion should pass if bindings are working correctly
      // If it fails, it indicates the bug: auto-delete is not respecting wildcard queue bindings
      assertNull(specificAddressAfterReaper,
         "Specific address should NOT be auto-deleted because wildcard queue TEST.# should be bound to it. " +
         "If this fails, it indicates either: " +
         "1) Console bug - queue TEST.# is not actually bound to specific address (only to TEST.#), or " +
         "2) Auto-delete bug - auto-delete is not checking wildcard queue bindings correctly");

      final CountDownLatch afterReaperLatch = new CountDownLatch(messageCount);

      subscriber = createDurableMqttClient(subscriberId);
      subscriber.setCallback(new MqttCallback() {
         @Override
         public void connectionLost(Throwable cause) {
         }

         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            System.out.println("AFTER REAPER - Received message on topic: " + topic + ", payload: " + new String(message.getPayload()));
            afterReaperLatch.countDown();
         }

         @Override
         public void deliveryComplete(IMqttDeliveryToken token) {
         }
      });
      subscriber.subscribe(mqttWildcardTopic, 1);

      assertTrue(afterReaperLatch.await(5000, TimeUnit.MILLISECONDS),
         "Should receive all messages after reaper run");
   }

   /**
    * Helper method to create an MQTT client with durable (clean session = false) settings
    */
   private MqttClient createDurableMqttClient(String clientId) throws MqttException {
      MqttClient client = new MqttClient("tcp://localhost:" + port, clientId, new MemoryPersistence());
      MqttConnectOptions options = new MqttConnectOptions();
      options.setCleanSession(false); // Durable subscription
      client.connect(options);
      return client;
   }

   /**
    * Helper method to print diagnostic information about addresses and queues
    */
   private void printAddressesAndQueues(org.apache.activemq.artemis.core.server.ActiveMQServer server) throws Exception {
      System.out.println("Addresses and their queues:");
      for (SimpleString addressName : server.getPostOffice().getAddresses()) {
         if (addressName.toString().startsWith("TEST")) {
            AddressInfo addressInfo = server.getAddressInfo(addressName);

            // Get address size from paging store
            long addressSize = 0;
            try {
               var pageStore = server.getPagingManager().getPageStore(addressName);
               if (pageStore != null) {
                  addressSize = pageStore.getAddressSize();
               }
            } catch (Exception e) {
               // If no page store exists, size is 0
            }

            var queuesForAddress = server.getPostOffice().listQueuesForAddress(addressName);

            System.out.println("  Address: " + addressName +
               " (AutoCreated: " + (addressInfo != null ? addressInfo.isAutoCreated() : "N/A") +
               ", RoutingTypes: " + (addressInfo != null ? addressInfo.getRoutingTypes() : "N/A") +
               ", AddressSize: " + addressSize + " bytes)");

            // Print bindings for this address
            var bindings = server.getPostOffice().getBindingsForAddress(addressName);
            if (bindings != null && !bindings.getBindings().isEmpty()) {
               System.out.println("    Bindings (" + bindings.getBindings().size() + "):");
               bindings.getBindings().forEach(binding -> {
                  System.out.println("      - " + binding.getUniqueName() +
                     " (Type: " + binding.getType() + ", Queue: " + ((LocalQueueBinding)binding).getAddress() + "/" + ((LocalQueueBinding)binding).getQueue().getName() + ")");
               });
            } else {
               System.out.println("    No bindings");
            }

            // Print queues for this address
            if (!queuesForAddress.isEmpty()) {
               System.out.println("    Queues (" + queuesForAddress.size() + "):");
               for (Queue queue : queuesForAddress) {
                  System.out.println("      - " + queue.getAddress() + "/" + queue.getName() +
                     " (Durable: " + queue.isDurable() +
                     ", Messages: " + queue.getMessageCount() +
                     ", Consumers: " + queue.getConsumerCount() +
                     ", PersistentSize: " + queue.getPersistentSize() + " bytes)");
               }
            } else {
               System.out.println("    No queues");
            }
            System.out.println();
         }
      }

      // Print all paging stores
      System.out.println("Paging Stores:");
      SimpleString[] storeNames = server.getPagingManager().getStoreNames();
      for (SimpleString storeName : storeNames) {
         if (storeName.toString().startsWith("TEST")) {
            try {
               var pageStore = server.getPagingManager().getPageStore(storeName);
               if (pageStore != null) {
                  long addressSize = pageStore.getAddressSize();
                  System.out.println("  PageStore: " + storeName +
                     " (Size: " + addressSize + " bytes" +
                     ", NumberOfPages: " + pageStore.getNumberOfPages() +
                     ", Paging: " + pageStore.isPaging() +
                     ", MaxSize: " + pageStore.getMaxSize() + " bytes)");

                  // Show breakdown if size > 0
                  if (addressSize > 0) {
                     // Find corresponding queues to compare
                     var queues = server.getPostOffice().listQueuesForAddress(storeName);
                     long totalQueueSize = 0;
                     for (Queue q : queues) {
                        totalQueueSize += q.getPersistentSize();
                     }
                     long overhead = addressSize - totalQueueSize;
                     System.out.println("    └─ Queue persistent data: " + totalQueueSize + " bytes");
                     System.out.println("    └─ Routing + overhead: " + overhead + " bytes (" +
                        String.format("%.1f", (overhead * 100.0 / addressSize)) + "% of total)");
                  }
               }
            } catch (Exception e) {
               System.out.println("  PageStore: " + storeName + " (Error: " + e.getMessage() + ")");
            }
         }
      }
      System.out.println();
   }
}
