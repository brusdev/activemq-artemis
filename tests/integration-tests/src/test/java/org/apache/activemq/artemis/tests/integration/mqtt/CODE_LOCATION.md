# Code Location Causing Auto-Delete Bug

## Call Stack During Auto-Delete

### 1. Entry Point (During Broker Startup/Sweep)
**File**: `artemis-server/src/main/java/org/apache/activemq/artemis/core/postoffice/impl/PostOfficeImpl.java`  
**Method**: `reapAddresses()`  
**Lines**: 2090-2100

```java
Set<SimpleString> addresses = addressManager.getAddresses();

for (SimpleString address : addresses) {
   AddressInfo addressInfo = getAddressInfo(address);
   AddressSettings settings = addressSettingsRepository.getMatch(address.toString());

   try {
      if (addressManager.checkAutoRemoveAddress(addressInfo, settings, initialCheck)) {
         if (initialCheck || addressInfo.isSwept()) {
            // THIS LINE TRIGGERS THE DELETION
            server.autoRemoveAddressInfo(address, null);  // LINE 2100
```

### 2. Auto-Delete Decision Logic (THE BUG IS HERE)
**File**: `artemis-server/src/main/java/org/apache/activemq/artemis/core/postoffice/impl/SimpleAddressManager.java`  
**Method**: `checkAutoRemoveAddress()`  
**Line**: 414

```java
@Override
public boolean checkAutoRemoveAddress(AddressInfo addressInfo,
                                      AddressSettings settings,
                                      boolean ignoreDelay) throws Exception {
   return settings.isAutoDeleteAddresses() && 
          addressInfo != null && 
          addressInfo.isAutoCreated() && 
          !bindingsFactory.isAddressBound(addressInfo.getName()) &&  // BUG: This check is incomplete
          (ignoreDelay || addressWasUsed(addressInfo, settings)) && 
          (ignoreDelay || delayCheck(addressInfo, settings));
}
```

**The Problem**: The check `!bindingsFactory.isAddressBound(addressInfo.getName())` returns `true` (meaning "no bindings, safe to delete") even when wildcard queues are bound to the address.

### 3. Binding Check (Root Cause)
**File**: `artemis-server/src/main/java/org/apache/activemq/artemis/core/postoffice/impl/PostOfficeImpl.java`  
**Method**: `isAddressBound()`  
**Lines**: 1069-1072

```java
@Override
public boolean isAddressBound(final SimpleString address) throws Exception {
   Collection<Binding> bindings = getDirectBindings(address);  // LINE 1070 - BUG!
   return bindings != null && !bindings.isEmpty();             // LINE 1071
}
```

**The Root Cause**: 
- Line 1070 uses `getDirectBindings(address)` 
- This method **ONLY returns direct bindings** to the address
- It **DOES NOT return wildcard queue bindings**
- For address `TEST.1`, it won't find the wildcard queue `TEST.#` even though that queue is actively bound to and consuming from `TEST.1`

### 4. Actual Deletion
**File**: `artemis-server/src/main/java/org/apache/activemq/artemis/core/server/impl/ActiveMQServerImpl.java`  
**Method**: `autoRemoveAddressInfo()`  
**Lines**: 4047-4050

```java
public void autoRemoveAddressInfo(SimpleString address, SecurityAuth auth) throws Exception {
   removeAddressInfo(address, auth, true);
   // THIS IS WHERE THE LOG MESSAGE IS GENERATED
   ActiveMQServerLogger.LOGGER.autoRemoveAddress(String.valueOf(address));  // LINE 4050
}
```

This generates the log: `AMQ224113: Auto removing address TEST.1`

## The Fix Needed

The `isAddressBound()` method in PostOfficeImpl.java needs to be updated to check **ALL** bindings, not just direct bindings.

### Current (Buggy) Code:
```java
public boolean isAddressBound(final SimpleString address) throws Exception {
   Collection<Binding> bindings = getDirectBindings(address);  // Only checks direct bindings
   return bindings != null && !bindings.isEmpty();
}
```

### Proposed Fix:
```java
public boolean isAddressBound(final SimpleString address) throws Exception {
   // Check direct bindings
   Collection<Binding> bindings = getDirectBindings(address);
   if (bindings != null && !bindings.isEmpty()) {
      return true;
   }
   
   // Also check for wildcard bindings
   Bindings allBindings = getBindingsForAddress(address);
   return allBindings != null && !allBindings.getBindings().isEmpty();
}
```

## Why This Matters

For MQTT wildcard subscriptions:
1. Client subscribes to `TEST/#` → creates queue `client.TEST.#`
2. Message sent to `TEST.1` → queue receives it (wildcard binding works)
3. Broker restart → `isAddressBound("TEST.1")` returns `false` (doesn't see wildcard binding)
4. Address `TEST.1` is deleted despite having an active wildcard queue binding

## Test Evidence

From `MqttWildcardAutoDeleteAddressTest`:
- **BEFORE restart**: Queue `mqtt-subscriber.TEST.#` is listed in bindings for `TEST.1` ✓
- **AFTER restart**: `TEST.1` deleted, log shows `AMQ224113: Auto removing address TEST.1` ✗

The diagnostic output proves the wildcard queue **IS** bound before restart but the auto-delete logic **DOESN'T DETECT** it.
