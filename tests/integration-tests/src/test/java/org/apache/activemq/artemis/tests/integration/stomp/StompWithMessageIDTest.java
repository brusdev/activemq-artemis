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
package org.apache.activemq.artemis.tests.integration.stomp;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.TextMessage;
import java.util.Enumeration;

import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.integration.stomp.util.ClientStompFrame;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnection;
import org.apache.activemq.artemis.tests.integration.stomp.util.StompClientConnectionFactory;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class StompWithMessageIDTest extends StompTestBase {

   @Override
   public boolean isEnableStompMessageId() {
      return true;
   }

   @TestTemplate
   public void testEnableMessageID() throws Exception {
      StompClientConnection conn = StompClientConnectionFactory.createClientConnection(uri);
      conn.connect(defUser, defPass);

      ClientStompFrame frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World 1");
      conn.sendFrame(frame);

      frame = conn.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.setBody("Hello World 2");
      conn.sendFrame(frame);

      QueueBrowser browser = session.createBrowser(queue);

      Enumeration enu = browser.getEnumeration();

      while (enu.hasMoreElements()) {
         Message msg = (Message) enu.nextElement();
         String msgId = msg.getStringProperty("amqMessageId");
         assertNotNull(msgId);
         assertTrue(msgId.indexOf("STOMP") == 0);
      }

      browser.close();

      MessageConsumer consumer = session.createConsumer(queue);

      TextMessage message = (TextMessage) consumer.receive(1000);
      assertNotNull(message);

      message = (TextMessage) consumer.receive(1000);
      assertNotNull(message);

      message = (TextMessage) consumer.receive(100);
      assertNull(message);

      conn.disconnect();
   }
}
