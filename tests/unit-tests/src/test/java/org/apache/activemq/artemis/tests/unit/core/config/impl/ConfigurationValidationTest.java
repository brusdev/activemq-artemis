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
package org.apache.activemq.artemis.tests.unit.core.config.impl;

import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;

import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPFederatedBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.XMLUtil;
import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Element;

public class ConfigurationValidationTest extends ActiveMQTestBase {

   static {
      System.setProperty("a2Prop", "a2");
      System.setProperty("falseProp", "false");
      System.setProperty("trueProp", "true");
      System.setProperty("ninetyTwoProp", "92");
   }

   /**
    * test does not pass in eclipse (because it can not find artemis-configuration.xsd).
    * It runs fine on the CLI with the proper env setting.
    */
   @Test
   public void testMinimalConfiguration() throws Exception {
      String xml = "<core xmlns='urn:activemq:core'>" + "</core>";
      Element element = XMLUtil.stringToElement(xml);
      Assert.assertNotNull(element);
      XMLUtil.validate(element, "schema/artemis-configuration.xsd");
   }

   @Test
   public void testFullConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals(true, fc.isPersistDeliveryCountBeforeDelivery());
   }

   @Test
   public void testAMQPConnectParsing() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals(4, fc.getAMQPConnection().size());

      AMQPBrokerConnectConfiguration amqpBrokerConnectConfiguration = fc.getAMQPConnection().get(0);
      Assert.assertEquals("testuser", amqpBrokerConnectConfiguration.getUser());
      Assert.assertEquals("testpassword", amqpBrokerConnectConfiguration.getPassword());
      Assert.assertEquals(33, amqpBrokerConnectConfiguration.getReconnectAttempts());
      Assert.assertEquals(333, amqpBrokerConnectConfiguration.getRetryInterval());
      Assert.assertEquals("test1", amqpBrokerConnectConfiguration.getName());
      Assert.assertEquals("tcp://test1:111", amqpBrokerConnectConfiguration.getUri());

      Assert.assertEquals("TEST-SENDER", amqpBrokerConnectConfiguration.getConnectionElements().get(0).getMatchAddress().toString());
      Assert.assertEquals(AMQPBrokerConnectionAddressType.SENDER, amqpBrokerConnectConfiguration.getConnectionElements().get(0).getType());
      Assert.assertEquals("TEST-RECEIVER", amqpBrokerConnectConfiguration.getConnectionElements().get(1).getMatchAddress().toString());
      Assert.assertEquals(AMQPBrokerConnectionAddressType.RECEIVER, amqpBrokerConnectConfiguration.getConnectionElements().get(1).getType());
      Assert.assertEquals("TEST-PEER", amqpBrokerConnectConfiguration.getConnectionElements().get(2).getMatchAddress().toString());
      Assert.assertEquals(AMQPBrokerConnectionAddressType.PEER, amqpBrokerConnectConfiguration.getConnectionElements().get(2).getType());
      Assert.assertEquals("TEST-WITH-QUEUE-NAME", amqpBrokerConnectConfiguration.getConnectionElements().get(3).getQueueName().toString());
      Assert.assertEquals(null, amqpBrokerConnectConfiguration.getConnectionElements().get(3).getMatchAddress());
      Assert.assertEquals(AMQPBrokerConnectionAddressType.RECEIVER, amqpBrokerConnectConfiguration.getConnectionElements().get(3).getType());

      Assert.assertEquals(AMQPBrokerConnectionAddressType.MIRROR, amqpBrokerConnectConfiguration.getConnectionElements().get(4).getType());
      AMQPMirrorBrokerConnectionElement mirrorConnectionElement = (AMQPMirrorBrokerConnectionElement) amqpBrokerConnectConfiguration.getConnectionElements().get(4);
      Assert.assertFalse(mirrorConnectionElement.isMessageAcknowledgements());
      Assert.assertFalse(mirrorConnectionElement.isQueueCreation());
      Assert.assertFalse(mirrorConnectionElement.isQueueRemoval());
      Assert.assertFalse(mirrorConnectionElement.isDurable());
      Assert.assertTrue(mirrorConnectionElement.isSync());

      Assert.assertEquals(AMQPBrokerConnectionAddressType.FEDERATION, amqpBrokerConnectConfiguration.getConnectionElements().get(5).getType());
      AMQPFederatedBrokerConnectionElement federationConnectionElement = (AMQPFederatedBrokerConnectionElement) amqpBrokerConnectConfiguration.getConnectionElements().get(5);
      Assert.assertEquals("test1", federationConnectionElement.getName());
      Assert.assertEquals(1, federationConnectionElement.getLocalQueuePolicies().size());
      federationConnectionElement.getLocalQueuePolicies().forEach((p) -> {
         Assert.assertEquals("composite", p.getName());
         Assert.assertEquals(1, p.getIncludes().size());
         Assert.assertEquals(0, p.getExcludes().size());
         Assert.assertNull(p.getTransformerConfiguration());
      });

      amqpBrokerConnectConfiguration = fc.getAMQPConnection().get(1);
      Assert.assertEquals(null, amqpBrokerConnectConfiguration.getUser());
      mirrorConnectionElement = (AMQPMirrorBrokerConnectionElement) amqpBrokerConnectConfiguration.getConnectionElements().get(0);
      Assert.assertEquals(null, amqpBrokerConnectConfiguration.getPassword());
      Assert.assertEquals("test2", amqpBrokerConnectConfiguration.getName());
      Assert.assertEquals("tcp://test2:222", amqpBrokerConnectConfiguration.getUri());
      Assert.assertTrue(mirrorConnectionElement.isMessageAcknowledgements());
      Assert.assertFalse(mirrorConnectionElement.isDurable());
      Assert.assertTrue(mirrorConnectionElement.isQueueCreation());
      Assert.assertTrue(mirrorConnectionElement.isQueueRemoval());
      Assert.assertFalse(mirrorConnectionElement.isSync()); // checking the default

      amqpBrokerConnectConfiguration = fc.getAMQPConnection().get(2);
      Assert.assertFalse(amqpBrokerConnectConfiguration.isAutostart());

      amqpBrokerConnectConfiguration = fc.getAMQPConnection().get(3);
      Assert.assertFalse(amqpBrokerConnectConfiguration.isAutostart());
      AMQPFederatedBrokerConnectionElement federationElement = (AMQPFederatedBrokerConnectionElement) amqpBrokerConnectConfiguration.getConnectionElements().get(0);
      Assert.assertEquals(1, federationElement.getLocalAddressPolicies().size());
      Assert.assertEquals(2, federationElement.getLocalQueuePolicies().size());
      Assert.assertEquals(1, federationElement.getRemoteAddressPolicies().size());
      Assert.assertEquals(1, federationElement.getRemoteQueuePolicies().size());
      Assert.assertTrue(federationElement.getProperties().containsKey("amqpCredits"));
      Assert.assertEquals("7", federationElement.getProperties().get("amqpCredits"));
      Assert.assertTrue(federationElement.getProperties().containsKey("amqpLowCredits"));
      Assert.assertEquals("1", federationElement.getProperties().get("amqpLowCredits"));

      federationElement.getLocalAddressPolicies().forEach(p -> {
         Assert.assertEquals("lap1", p.getName());
         Assert.assertEquals(1, p.getIncludes().size());
         p.getIncludes().forEach(match -> Assert.assertEquals("orders", match.getAddressMatch()));
         Assert.assertEquals(1, p.getExcludes().size());
         p.getExcludes().forEach(match -> Assert.assertEquals("all.#", match.getAddressMatch()));
         Assert.assertFalse(p.getAutoDelete());
         Assert.assertEquals(1L, (long) p.getAutoDeleteDelay());
         Assert.assertEquals(12L, (long) p.getAutoDeleteMessageCount());
         Assert.assertEquals(2, (int) p.getMaxHops());
         Assert.assertNotNull(p.getTransformerConfiguration());
         Assert.assertEquals("class-name", p.getTransformerConfiguration().getClassName());
      });
      federationElement.getLocalQueuePolicies().forEach((p) -> {
         if (p.getName().endsWith("lqp1")) {
            Assert.assertEquals(1, (int) p.getPriorityAdjustment());
            Assert.assertFalse(p.isIncludeFederated());
            Assert.assertEquals("class-another", p.getTransformerConfiguration().getClassName());
            Assert.assertNotNull(p.getProperties());
            Assert.assertFalse(p.getProperties().isEmpty());
            Assert.assertTrue(p.getProperties().containsKey("amqpCredits"));
            Assert.assertEquals("1", p.getProperties().get("amqpCredits"));
         } else if (p.getName().endsWith("lqp2")) {
            Assert.assertNull(p.getPriorityAdjustment());
            Assert.assertFalse(p.isIncludeFederated());
         } else {
            Assert.fail("Should only be two local queue policies");
         }
      });
      federationElement.getRemoteAddressPolicies().forEach((p) -> {
         Assert.assertEquals("rap1", p.getName());
         Assert.assertEquals(1, p.getIncludes().size());
         p.getIncludes().forEach(match -> Assert.assertEquals("support", match.getAddressMatch()));
         Assert.assertEquals(0, p.getExcludes().size());
         Assert.assertTrue(p.getAutoDelete());
         Assert.assertEquals(2L, (long) p.getAutoDeleteDelay());
         Assert.assertEquals(42L, (long) p.getAutoDeleteMessageCount());
         Assert.assertEquals(1, (int) p.getMaxHops());
         Assert.assertNotNull(p.getTransformerConfiguration());
         Assert.assertEquals("something", p.getTransformerConfiguration().getClassName());
         Assert.assertEquals(2, p.getTransformerConfiguration().getProperties().size());
         Assert.assertEquals("value1", p.getTransformerConfiguration().getProperties().get("key1"));
         Assert.assertEquals("value2", p.getTransformerConfiguration().getProperties().get("key2"));
         Assert.assertNotNull(p.getProperties());
         Assert.assertFalse(p.getProperties().isEmpty());
         Assert.assertTrue(p.getProperties().containsKey("amqpCredits"));
         Assert.assertEquals("2", p.getProperties().get("amqpCredits"));
         Assert.assertTrue(p.getProperties().containsKey("amqpLowCredits"));
         Assert.assertEquals("1", p.getProperties().get("amqpLowCredits"));
      });
      federationElement.getRemoteQueuePolicies().forEach((p) -> {
         Assert.assertEquals("rqp1", p.getName());
         Assert.assertEquals(-1, (int) p.getPriorityAdjustment());
         Assert.assertTrue(p.isIncludeFederated());
         p.getIncludes().forEach(match -> {
            Assert.assertEquals("#", match.getAddressMatch());
            Assert.assertEquals("tracking", match.getQueueMatch());
         });
         Assert.assertNotNull(p.getProperties());
         Assert.assertFalse(p.getProperties().isEmpty());
         Assert.assertTrue(p.getProperties().containsKey("amqpCredits"));
         Assert.assertEquals("2", p.getProperties().get("amqpCredits"));
         Assert.assertTrue(p.getProperties().containsKey("amqpLowCredits"));
         Assert.assertEquals("1", p.getProperties().get("amqpLowCredits"));
      });
   }

   @Test
   public void testChangeConfiguration() throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config.xml");
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();
      deploymentManager = new FileDeploymentManager("ConfigurationTest-full-config-wrong-address.xml");
      deploymentManager.addDeployable(fc);

      try {
         deploymentManager.readConfiguration();
         fail("Exception expected");
      } catch (Exception ignored) {
      }
   }
}
