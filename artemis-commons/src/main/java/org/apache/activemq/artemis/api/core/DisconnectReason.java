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
package org.apache.activemq.artemis.api.core;

public enum DisconnectReason {
   REDIRECT, SCALE_DOWN, SHOUT_DOWN;

   public byte getType() {
      switch (this) {
         case REDIRECT:
            return 0;
         case SCALE_DOWN:
            return 2;
         case SHOUT_DOWN:
            return 3;
         default:
            return -1;
      }
   }

   public static DisconnectReason getType(byte type) {
      switch (type) {
         case 0:
            return REDIRECT;
         case 1:
            return SCALE_DOWN;
         case 2:
            return SHOUT_DOWN;
         default:
            return null;
      }
   }
}
