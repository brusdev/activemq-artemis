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
package org.apache.activemq.artemis.core.config.impl;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.settings.Mergeable;

import java.util.HashSet;
import java.util.Set;

public class RoleSet extends HashSet<Role> implements Mergeable<RoleSet> {

   private String name;

   private boolean exclusive;

   public RoleSet() {
      super();
   }

   public RoleSet(String key, boolean exclusive) {
      setName(key);
      setExclusive(exclusive);
   }

   public RoleSet(String key, Set<Role> value) {
      setName(key);
      if (value != null) {
         addAll(value);
      }
   }

   // provide a helper add method with the type
   public void add(String name, Role value) {
      super.add(value);
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public boolean isExclusive() {
      return exclusive;
   }

   public void setExclusive(boolean exclusive) {
      this.exclusive = exclusive;
   }

   @Override
   public void merge(RoleSet merged) {
      if (this.exclusive && merged.exclusive) {
         for (Role mergedRole : merged) {
            Role mergingRole = this.stream().filter(role -> role.getName().equals(mergedRole.getName())).findFirst().orElse(null);
            if (mergingRole == null) {
               this.add(mergedRole);
            } else {
               mergingRole.merge(mergedRole);
            }
         }
      }
   }

   @Override
   public RoleSet mergeCopy(RoleSet merged) {
      if (!this.exclusive || !merged.exclusive) {
         return this;
      }

      RoleSet result = new RoleSet(this.name, true);
      result.addAll(this);

      for (Role mergedRole : merged) {
         Role resultRole = new Role(mergedRole.getName(),
            mergedRole.isSend(),
            mergedRole.isConsume(),
            mergedRole.isCreateDurableQueue(),
            mergedRole.isDeleteDurableQueue(),
            mergedRole.isCreateNonDurableQueue(),
            mergedRole.isDeleteNonDurableQueue(),
            mergedRole.isManage(),
            mergedRole.isBrowse(),
            mergedRole.isCreateAddress(),
            mergedRole.isDeleteAddress(),
            mergedRole.isView(),
            mergedRole.isEdit());
         resultRole.setName(mergedRole.getName());

         Role mergingRole = result.stream().filter(role -> role.getName().equals(mergedRole.getName())).findFirst().orElse(null);
         if (mergingRole != null) {
            resultRole.merge(mergingRole);
         }

         result.add(resultRole);
      }

      return result;
   }
}
