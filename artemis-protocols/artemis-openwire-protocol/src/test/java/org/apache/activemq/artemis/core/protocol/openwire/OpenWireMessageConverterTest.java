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
package org.apache.activemq.artemis.core.protocol.openwire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.openwire.OpenWireFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/*
 mvn clean -Ptests -DfailIfNoTests=false -Dtest=OpenWireMessageConverterTest test \
    -pl artemis-protocols/artemis-openwire-protocol,artemis-commons
*/
public class OpenWireMessageConverterTest {

   @Rule
   public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

   private WireFormat format;
   private MessageId messageId;
   private ProducerId producerId;
   private FooDestination fooDestination;

   @Before
   public void before() {
      format = new OpenWireFormat();
      messageId = new MessageId("ID:fvaleri-mac-54067-1584812479127-1:1:1:1:1");
      producerId = new ProducerId("ID:fvaleri-mac-54067-1584812479127-1:1:1:1");
      fooDestination = new FooDestination();
   }

   @Test
   public void amqToCore() throws Exception {
      org.apache.activemq.command.Message amqMessage = new ActiveMQTextMessage();
      amqMessage.setPersistent(true);
      amqMessage.setExpiration(0);
      amqMessage.setPriority((byte) javax.jms.Message.DEFAULT_PRIORITY);
      amqMessage.setTimestamp(System.currentTimeMillis());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      MarshallingSupport.writeUTF8(dos, "test");
      dos.close();
      amqMessage.setContent(baos.toByteSequence());

      amqMessage.setCompressed(false);
      amqMessage.setArrival(System.currentTimeMillis());
      amqMessage.setBrokerInTime(System.currentTimeMillis());
      amqMessage.setBrokerPath(new BrokerId[]{new BrokerId("broker1"), new BrokerId("broker2")});
      amqMessage.setCluster(new BrokerId[]{new BrokerId("broker1"), new BrokerId("broker2")});
      amqMessage.setCommandId(1);
      amqMessage.setGroupID("group1");
      amqMessage.setGroupSequence(1);
      amqMessage.setMessageId(messageId);
      amqMessage.setUserID("user1");
      amqMessage.setProducerId(producerId);
      amqMessage.setReplyTo(new ActiveMQQueue("bar"));
      amqMessage.setDroppable(false);
      amqMessage.setOriginalDestination(fooDestination);

      org.apache.activemq.artemis.api.core.Message coreMessage = OpenWireMessageConverter.inbound(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);

      ByteSequence seq = new ByteSequence((byte[]) coreMessage.getObjectProperty("__HDR_MESSAGE_ID"));
      assertEquals(messageId, (MessageId) format.unmarshal(seq));
   }

   @Test
   public void coreToAmq() throws Exception {
      AMQConsumer consumer = Mockito.mock(AMQConsumer.class);
      Mockito.when(consumer.getId()).thenReturn(new ConsumerId("123"));
      Mockito.when(consumer.getOpenwireDestination()).thenReturn(fooDestination);

      final String content = "test";
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.getBodyBuffer().writeString(content);

      putObjectProperty(coreMessage, "__HDR_DATASTRUCTURE", fooDestination);
      putObjectProperty(coreMessage, "__HDR_MESSAGE_ID", messageId);
      putObjectProperty(coreMessage, "__HDR_ORIG_DESTINATION", fooDestination);
      putObjectProperty(coreMessage, "__HDR_PRODUCER_ID", producerId);

      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = OpenWireMessageConverter.createMessageDispatch(reference, coreMessage, format, consumer);

      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(messageId, amqDispatch.getMessage().getMessageId());
   }

   private void putObjectProperty(ICoreMessage coreMessage, String propertyName, Object object) throws IOException {
      final ByteSequence objectBytes = format.marshal(object);
      objectBytes.compact();
      coreMessage.putObjectProperty(propertyName, objectBytes.data);
   }

   class FooDestination extends ActiveMQDestination {
      @Override
      public String getPhysicalName() {
         return "foo";
      }

      @Override
      public byte getDataStructureType() {
         return CommandTypes.ACTIVEMQ_QUEUE;
      }

      @Override
      protected String getQualifiedPrefix() {
         return "queue://";
      }

      @Override
      public byte getDestinationType() {
         return QUEUE_TYPE;
      }
   }

}
