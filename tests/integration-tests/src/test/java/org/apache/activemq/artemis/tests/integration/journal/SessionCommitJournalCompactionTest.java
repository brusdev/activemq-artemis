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
package org.apache.activemq.artemis.tests.integration.journal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that verifies the system handles concurrent journal compaction
 * while processing SessionCommitMessage from ClientConsumer session commits using 
 * NIO journal type with durable messages. This test ensures that compaction can be 
 * triggered while a consumer session commit (message acknowledgments) is being 
 * processed without causing data corruption or system instability.
 */
public class SessionCommitJournalCompactionTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(SessionCommitJournalCompactionTest.class);
   
   private static final SimpleString QUEUE_NAME = SimpleString.of("testQueue");
   private static final int MESSAGE_COUNT = 50;
   
   private ServerLocator locator;
   private ActiveMQServer server;
   private ClientSessionFactory sessionFactory;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      
      // Create and configure server with journal settings that encourage compaction
      server = createServer(true, true);  // persistence enabled, journal sync enabled
      server.getConfiguration().setJournalType(JournalType.NIO); // Use NIO journal type
      server.getConfiguration().setJournalCompactMinFiles(2);
      server.getConfiguration().setJournalCompactPercentage(10);
      server.getConfiguration().setJournalFileSize(1024 * 1024); // 1MB files to trigger compaction
      
      server.start();
      waitForServerToStart(server);
      
      locator = createInVMNonHALocator();
      locator.setCallTimeout(30000);
      sessionFactory = createSessionFactory(locator);
   }

   @Test
   public void testJournalCompactionDuringConsumerSessionCommit() throws Exception {
      logger.info("Starting testJournalCompactionDuringConsumerSessionCommit with NIO journal type and durable messages");
      
      // Setup: Create queue and populate it with messages for consumption
      try (ClientSession setupSession = sessionFactory.createSession()) {
         setupSession.createQueue(QueueConfiguration.of(QUEUE_NAME));
         
         // Create initial data in the journal to make compaction worthwhile (durable messages)
         ClientProducer producer = setupSession.createProducer(QUEUE_NAME);
         for (int i = 0; i < MESSAGE_COUNT * 2; i++) {
            ClientMessage message = setupSession.createMessage(true); // Durable messages
            message.getBodyBuffer().writeString("Setup message " + i);
            producer.send(message);
         }
         setupSession.commit();
         
         // Consume and delete some messages to create "holes" in the journal for compaction
         ClientConsumer consumer = setupSession.createConsumer(QUEUE_NAME);
         setupSession.start();
         for (int i = 0; i < MESSAGE_COUNT / 2; i++) {
            ClientMessage receivedMessage = consumer.receive(5000);
            assertNotNull(receivedMessage, "Should receive message " + i);
            receivedMessage.acknowledge();
         }
         setupSession.commit();
         consumer.close();
         producer.close();
      }
      
      // Force some file turnover to create conditions where compaction is beneficial
      server.getStorageManager().getMessageJournal().forceMoveNextFile();
      
      // Test: Create consumer session that will commit acknowledgments while compaction runs
      AtomicReference<Throwable> consumerError = new AtomicReference<>();
      AtomicReference<Throwable> compactionError = new AtomicReference<>();
      CountDownLatch consumerStarted = new CountDownLatch(1);
      CountDownLatch compactionStarted = new CountDownLatch(1);
      CountDownLatch testCompleted = new CountDownLatch(2);
      AtomicBoolean consumerCommitInProgress = new AtomicBoolean(false);
      
      ExecutorService executor = Executors.newFixedThreadPool(2);
      
      try {
         // Thread 1: Consumer session operations with acknowledgment commits
         executor.submit(() -> {
            try (ClientSession consumerSession = sessionFactory.createSession(false, false, false)) {
               consumerSession.start();
               
               ClientConsumer consumer = consumerSession.createConsumer(QUEUE_NAME);
               
               consumerStarted.countDown();
               logger.info("Consumer session thread started, waiting for compaction to start");

               // Receive and acknowledge messages - this will require commit
               logger.info("Starting message consumption and acknowledgment");
               for (int i = 0; i < MESSAGE_COUNT; i++) {
                  ClientMessage receivedMessage = consumer.receive(10000);
                  assertNotNull(receivedMessage, "Should receive message " + i);
                  receivedMessage.acknowledge();
                  
                  if ((i + 1) % 10 == 0) {
                     logger.debug("Acknowledged {} messages", i + 1);
                  }
               }
               
               logger.info("Starting consumer session commit (acknowledgments) while compaction is in progress");
               consumerCommitInProgress.set(true);

               // Wait for compaction to start, then begin consuming and committing
               assertTrue(compactionStarted.await(30, TimeUnit.SECONDS),
                       "Compaction should have started");

               // This commit will trigger SessionCommitMessage processing for acknowledgments while compaction is running
               consumerSession.commit();
               
               logger.info("Consumer session commit completed successfully");
               consumer.close();
               
            } catch (Exception e) {
               logger.error("Error in consumer session thread", e);
               consumerError.set(e);
            } finally {
               testCompleted.countDown();
            }
         });
         
         // Thread 2: Journal compaction
         executor.submit(() -> {
            try {
               assertTrue(consumerStarted.await(30, TimeUnit.SECONDS), 
                         "Consumer session should have started");
               
               logger.info("Starting journal compaction");
               compactionStarted.countDown();
               
               // Start compaction - this will run concurrently with the session commit
               server.getStorageManager().getMessageJournal().scheduleCompactAndBlock(30_000);
               
               logger.info("Journal compaction completed successfully");
               
            } catch (Exception e) {
               logger.error("Error in compaction thread", e);
               compactionError.set(e);
            } finally {
               testCompleted.countDown();
            }
         });
         
         // Wait for both operations to complete
         assertTrue(testCompleted.await(60, TimeUnit.SECONDS), 
                   "Both consumer session commit and journal compaction should complete");
         
         // Verify no errors occurred
         if (consumerError.get() != null) {
            throw new AssertionError("Consumer session operation failed", consumerError.get());
         }
         if (compactionError.get() != null) {
            throw new AssertionError("Compaction operation failed", compactionError.get());
         }
         
         logger.info("Both consumer session commit and compaction operations completed successfully");
         
         // Verification: Ensure data integrity after concurrent operations
         verifyConsumerCommitDataIntegrity();
         
      } finally {
         executor.shutdown();
         assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), 
                   "Executor should terminate");
      }
   }
   
   private void verifyConsumerCommitDataIntegrity() throws Exception {
      logger.info("Verifying durable message data integrity after concurrent consumer commit and compaction operations");
      
      try (ClientSession verifySession = sessionFactory.createSession()) {
         verifySession.start();
         
         ClientConsumer consumer = verifySession.createConsumer(QUEUE_NAME);
         
         // Count remaining durable messages
         int messageCount = 0;
         ClientMessage message;
         while ((message = consumer.receiveImmediate()) != null) {
            message.acknowledge();
            messageCount++;
            logger.debug("Verified remaining durable message: {}", message.getBodyBuffer().readString());
         }
         
         verifySession.commit();
         consumer.close();
         
         // We should have the remaining durable messages after consumer committed acknowledgments
         // Started with MESSAGE_COUNT * 2, consumed MESSAGE_COUNT / 2 in setup, consumed MESSAGE_COUNT in test
         int expectedCount = (MESSAGE_COUNT * 2) - (MESSAGE_COUNT / 2) - MESSAGE_COUNT;
         expectedCount = MESSAGE_COUNT - MESSAGE_COUNT / 2; // Simplified calculation
         assertEquals(expectedCount, messageCount, 
                     "Should have correct number of remaining durable messages after consumer commit and compaction");
         
         logger.info("Durable message data integrity verification passed. Found {} remaining messages", messageCount);
      }
   }
   
   @Test
   public void testMultipleCompactionsDuringConsumerCommits() throws Exception {
      logger.info("Starting testMultipleCompactionsDuringConsumerCommits with NIO journal type and durable messages");
      
      try (ClientSession setupSession = sessionFactory.createSession()) {
         setupSession.createQueue(QueueConfiguration.of(QUEUE_NAME));
      }
      
      // Create conditions for multiple compactions by populating queue with messages
      try (ClientSession producerSession = sessionFactory.createSession()) {
         ClientProducer producer = producerSession.createProducer(QUEUE_NAME);
         
         // Send many durable messages to create journal content
         for (int cycle = 0; cycle < 3; cycle++) {
            logger.info("Cycle {}: Creating durable messages for consumption", cycle);
            
            for (int i = 0; i < MESSAGE_COUNT; i++) {
               ClientMessage message = producerSession.createMessage(true); // Durable messages
               message.getBodyBuffer().writeString("Cycle " + cycle + " message " + i);
               producer.send(message);
            }
            producerSession.commit();
            
            // Force file turnover to create compaction opportunities
            server.getStorageManager().getMessageJournal().forceMoveNextFile();
         }
         producer.close();
      }
      
      AtomicReference<Throwable> error = new AtomicReference<>();
      CountDownLatch operationsCompleted = new CountDownLatch(2);
      
      ExecutorService executor = Executors.newFixedThreadPool(2);
      
      try {
         // Concurrent consumer session commits
         executor.submit(() -> {
            try {
               for (int i = 0; i < 5; i++) {
                  try (ClientSession consumerSession = sessionFactory.createSession(false, false, false)) {
                     consumerSession.start();
                     ClientConsumer consumer = consumerSession.createConsumer(QUEUE_NAME);
                     
                     // Consume and acknowledge messages in small batches
                     for (int j = 0; j < 10; j++) {
                        ClientMessage receivedMessage = consumer.receive(5000);
                        if (receivedMessage != null) {
                           receivedMessage.acknowledge();
                        } else {
                           break; // No more messages
                        }
                     }
                     
                     // Commit the acknowledgments - this triggers SessionCommitMessage
                     consumerSession.commit();
                     consumer.close();
                  }
                  Thread.sleep(100); // Small delay between commits
               }
            } catch (Exception e) {
               error.set(e);
            } finally {
               operationsCompleted.countDown();
            }
         });
         
         // Concurrent compactions
         executor.submit(() -> {
            try {
               for (int i = 0; i < 3; i++) {
                  logger.info("Starting compaction {}", i);
                  server.getStorageManager().getMessageJournal().scheduleCompactAndBlock(15_000);
                  Thread.sleep(200); // Small delay between compactions
               }
            } catch (Exception e) {
               error.set(e);
            } finally {
               operationsCompleted.countDown();
            }
         });
         
         assertTrue(operationsCompleted.await(60, TimeUnit.SECONDS), 
                   "All operations should complete");
         
         if (error.get() != null) {
            throw new AssertionError("Concurrent operations failed", error.get());
         }
         
         logger.info("Multiple compactions during consumer commits test with durable messages completed successfully");
         
      } finally {
         executor.shutdown();
         assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      }
   }
}
