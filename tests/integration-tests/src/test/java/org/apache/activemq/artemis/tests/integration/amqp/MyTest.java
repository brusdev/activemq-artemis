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
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;

import com.dsect.jvmti.JVMTIInterface;
import com.sun.management.HotSpotDiagnosticMXBean;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MyTest extends ActiveMQTestBase {

   ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      server = createServer(true, createDefaultConfig(1, true));
      server.start();
   }

   @Test
   public void simpleTest() throws Exception {
      JVMTIInterface jvmtiInterface = new JVMTIInterface();
      jvmtiInterface.forceGC();
      assertMemoryStats(jvmtiInterface);

      ConnectionFactory cf = new JmsConnectionFactory("amqp://localhost:61616");

      try (Connection producerConnection = cf.createConnection(); Connection consumerConnection = cf.createConnection()) {

         Session producerSession = producerConnection.createSession();

         Session consumerSession = consumerConnection.createSession(Session.SESSION_TRANSACTED);
         consumerConnection.start();

         for (int i = 0; i < 10; i++) {
            {
               Destination source = producerSession.createQueue("source");
               MessageProducer sourceProducer = producerSession.createProducer(source);
               sourceProducer.send(producerSession.createMessage());
               sourceProducer.close();
            }
            {
               Destination source = consumerSession.createQueue("source");
               Destination target = consumerSession.createQueue("target");
               MessageConsumer sourceConsumer = consumerSession.createConsumer(source);
               MessageProducer targetProducer = consumerSession.createProducer(target);
               Message m = sourceConsumer.receive();
               targetProducer.send(m);
               consumerSession.commit();
               sourceConsumer.close();
               //targetProducer.close(); // this line fixes the leak on the broker
            }
         }
      }
      jvmtiInterface.forceGC();
      assertMemoryStats(jvmtiInterface);
   }

   @Test
   public void longRunningTest() throws Exception {
      JVMTIInterface jvmtiInterface = new JVMTIInterface();
      jvmtiInterface.forceGC();
      assertMemoryStats(jvmtiInterface);

      ConnectionFactory cf = new JmsConnectionFactory("amqp://localhost:61616");

      try (Connection producerConnection = cf.createConnection(); Connection consumerConnection = cf.createConnection()) {

         for (int s = 0; s < 10; s++) {
            Session producerSession = producerConnection.createSession();
            Session consumerSession = consumerConnection.createSession();
            consumerConnection.start();

            for (int i = 0; i < 10; i++) {
               {
                  Destination source = producerSession.createQueue("source" + s + "-" + i);
                  MessageProducer sourceProducer = producerSession.createProducer(source);
                  sourceProducer.send(producerSession.createMessage());
                  //sourceProducer.close();
               }
               {
                  Destination source = consumerSession.createQueue("source" + s + "-" + i);
                  MessageConsumer sourceConsumer = consumerSession.createConsumer(source);
                  Message m = sourceConsumer.receive();
                  Assert.assertNotNull(m);
                  //sourceConsumer.close(); // this line fixes the leak on org.apache.qpid.proton.engine.impl.CollectorImpl
               }
            }

            producerSession.close();
            consumerSession.close();
         }

         jvmtiInterface.forceGC();

         Object[] protonServerSenderContexts = jvmtiInterface
            .getAllObjects("org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext");

         if (protonServerSenderContexts.length > 0) {
            // The session is closed protonServerSenderContexts.length should be zero

            //Maybe they are closed
            Field closedField = ProtonServerSenderContext.class.getDeclaredField("closed");
            closedField.setAccessible(true);
            for (Object protonServerSenderContext : protonServerSenderContexts) {
               if (closedField.getBoolean(protonServerSenderContext)) {
                  // The ProtonServerSenderContext is closed it should not be live, who is keeping it alive?
                  dumpHeap("long-test.hprof", true);

                  //It is reached from org.apache.qpid.proton.engine.impl.CollectorImpl
               }
            }
         }

         assertMemoryStats(jvmtiInterface);
      }
   }

   public static void dumpHeap(String filePath, boolean live) throws IOException {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
         server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
      mxBean.dumpHeap(filePath, live);
   }

   private void assertMemoryStats(JVMTIInterface jvmtiInterface) {
      assertEquals(0, jvmtiInterface.getAllObjects("org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext").length);
      assertEquals(0, jvmtiInterface.getAllObjects("org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext").length);
      assertEquals(0, jvmtiInterface.getAllObjects("org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback").length);
      assertEquals(0, jvmtiInterface.getAllObjects("org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl").length);
      assertEquals(0, jvmtiInterface.getAllObjects("org.apache.activemq.artemis.core.server.impl.RoutingContextImpl").length);
   }
}