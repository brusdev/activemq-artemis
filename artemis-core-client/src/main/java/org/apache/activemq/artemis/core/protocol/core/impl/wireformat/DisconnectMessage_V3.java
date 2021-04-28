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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.DisconnectReason;
import org.apache.activemq.artemis.api.core.SimpleString;

public class DisconnectMessage_V3 extends DisconnectMessage {

   private DisconnectReason disconnectReason;
   private SimpleString handoverReference;

   public DisconnectMessage_V3(final SimpleString nodeID, final DisconnectReason disconnectReason, final SimpleString handoverReference) {
      super(DISCONNECT_V3);

      this.nodeID = nodeID;

      this.disconnectReason = disconnectReason;

      this.handoverReference = handoverReference;
   }

   public DisconnectMessage_V3() {
      super(DISCONNECT_V3);
   }

   // Public --------------------------------------------------------

   public DisconnectReason getDisconnectReason() {
      return disconnectReason;
   }

   public SimpleString getHandoverReference() {
      return handoverReference;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      super.encodeRest(buffer);
      buffer.writeByte(disconnectReason == null ? -1 : disconnectReason.getType());
      buffer.writeNullableSimpleString(handoverReference);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      super.decodeRest(buffer);
      disconnectReason = DisconnectReason.getType(buffer.readByte());
      handoverReference = buffer.readNullableSimpleString();
   }

   @Override
   public String toString() {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", nodeID=" + nodeID);
      buf.append(", disconnectReason=" + disconnectReason);
      buf.append(", handoverReference=" + handoverReference);
      buf.append("]");
      return buf.toString();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (disconnectReason.getType());
      result = prime * result + ((handoverReference == null) ? 0 : handoverReference.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof DisconnectMessage_V3)) {
         return false;
      }
      DisconnectMessage_V3 other = (DisconnectMessage_V3) obj;
      if (disconnectReason == null) {
         if (other.disconnectReason != null)
            return false;
      } else if (!disconnectReason.equals(other.disconnectReason))
         return false;
      if (handoverReference == null) {
         if (other.handoverReference != null) {
            return false;
         }
      } else if (!handoverReference.equals(other.handoverReference)) {
         return false;
      }
      return true;
   }
}
