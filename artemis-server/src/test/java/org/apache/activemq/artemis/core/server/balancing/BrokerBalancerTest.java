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

package org.apache.activemq.artemis.core.server.balancing;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.balancing.targets.LocalTarget;
import org.apache.activemq.artemis.core.server.balancing.targets.Target;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetResult;
import org.apache.activemq.artemis.core.server.balancing.transformers.KeyTransformer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BrokerBalancerTest {

   Target localTarget;
   BrokerBalancer underTest;

   @Before
   public void setUp() {
      ActiveMQServer mockServer = mock(ActiveMQServer.class);
      Mockito.when(mockServer.getNodeID()).thenReturn(SimpleString.toSimpleString("UUID"));
      localTarget = new LocalTarget(null, mockServer);
   }

   @Test
   public void getTarget() {
      underTest  = new BrokerBalancer("test", ConnectionKey.CLIENT_ID, "^.{3}", null,
                                      localTarget, "^FOO.*", null, null, null);
      assertEquals( localTarget, underTest.getTarget("FOO_EE").getTarget());
      assertEquals(TargetResult.REFUSED_USE_ANOTHER_RESULT, underTest.getTarget("BAR_EE"));
   }

   @Test
   public void getLocalTargetWithTransformer() throws Exception {
      KeyTransformer keyTransformer = key -> key.substring("TRANSFORM_TO".length() + 1);
      underTest  = new BrokerBalancer("test", ConnectionKey.CLIENT_ID, "^.{3}", keyTransformer,
                                      localTarget, "^FOO.*", null, null, null);
      assertEquals( localTarget, underTest.getTarget("TRANSFORM_TO_FOO_EE").getTarget());
   }

}