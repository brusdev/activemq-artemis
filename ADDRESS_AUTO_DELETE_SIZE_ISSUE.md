# Address Auto-Delete Issue: Addresses Without Bindings Can Have Size > 0

## Issue Summary

The broker's auto-delete address logic only checks if an address has direct queue bindings (`!bindingsFactory.isAddressBound(addressInfo.getName())`), but does NOT check if the address has messages (size > 0). This can lead to addresses being incorrectly auto-deleted even when they contain messages, resulting in potential data loss and inconsistent state.

**Confirmed Cases Where This Issue Occurs:**
1. **Wildcard Queue Bindings** - Queue bound via wildcard pattern receives messages from specific addresses
2. **Mirror SNF Queues (Non-Paged Messages Only)** - Mirror replication uses references for non-paged messages, causing size inflation on SNF address

## Root Cause

The auto-delete check in `SimpleAddressManager.checkAutoRemoveAddress()` (line 414):

```java
return settings.isAutoDeleteAddresses() && 
       addressInfo != null && 
       addressInfo.isAutoCreated() && 
       !bindingsFactory.isAddressBound(addressInfo.getName()) &&  // Only checks bindings
       (ignoreDelay || addressWasUsed(addressInfo, settings)) && 
       (ignoreDelay || delayCheck(addressInfo, settings));
```

**Missing Check**: The method does NOT verify `addressInfo.getAddressSize() == 0` or check the paging store size before allowing deletion.

## Key Concept: Message References vs Message Copies

Understanding the difference between message **references** and message **copies** is critical to understanding this issue:

- **Message Copy**: A new message object with a new ID and its own address. Size counted only on the new address.
  - Example: Diverts use `message.copy(newID)` and `copy.setAddress(forwardAddress)`
  
- **Message Reference**: Points to the SAME message object, but can add size to multiple addresses.
  - Example: Wildcard queues, Mirror SNF queues (for non-paged messages only)
  - The original message's owner (PagingStore) remains the original address
  - Special logic can add size to additional addresses even though it's the same message

**This is why wildcard queues and mirror SNF queues (for non-paged messages) cause the issue, but diverts and cluster bridges do not.**

### Visual Comparison

```
DIVERT (Message Copy - NO ISSUE):
Message M → Address "orders" → Queue "ordersQueue"
                ↓ (divert copies)
              Copy C (new ID) → Address "audit" → Queue "auditQueue"
              
Size Accounting:
  Address "orders": size of M
  Address "audit": size of C
  ✓ Each address counts only its own messages

MIRROR - Non-Paged Messages (Message Reference - CAUSES ISSUE):
Message M (NOT paged) → Address "orders" → Queue "ordersQueue" (M.owner = "orders")
                          ↓ (mirror references same message - no copy!)
                        Ref to M → Address "mirrorSNF" → Queue "snfQueue" (M.owner STILL "orders")
              
Size Accounting:
  Address "orders": size of M (from M.owner)
  Address "mirrorSNF": ALSO size of M (from mirror special logic)
  ✗ Same message counted in TWO addresses!

MIRROR - Paged Messages (Message Copy - NO ISSUE):
Message M (paged) → Address "orders" → Queue "ordersQueue" (M.owner = "orders")
                      ↓ (mirror COPIES the message)
                    Copy C (new ID) → Address "mirrorSNF" → Queue "snfQueue" (C.owner = "mirrorSNF")
              
Size Accounting:
  Address "orders": size of M
  Address "mirrorSNF": size of C
  ✓ Each address counts only its own messages

WILDCARD (Message Reference - CAUSES ISSUE):
Message M → Address "TEST.1" → Routed to Queue "mqtt-subscriber.TEST.#"
              
Size Accounting:
  Address "TEST.1": size of M (paging store holds the message)
  Queue "mqtt-subscriber.TEST.#": bound to BOTH "TEST.#" AND "TEST.1"
  ✗ After restart: binding check fails to detect wildcard binding to "TEST.1"
```

## Cases Where Addresses Have Size > 0 Without Direct Bindings

### 1. Wildcard Queue Bindings (CONFIRMED BUG)

**Scenario:**
- MQTT client subscribes to wildcard topic `TEST/#`
- Creates durable queue `mqtt-subscriber.TEST.#` bound to address `TEST.#`
- Messages published to specific topic `TEST.1` are routed to the wildcard queue
- The wildcard queue gets bound to address `TEST.1` (shown in console and diagnostic output)
- Address `TEST.1` accumulates message size (e.g., 798 bytes in paging store)
- On broker restart, auto-delete logic checks `TEST.1`:
  - Finds no **direct** queue bindings (the binding is via wildcard matching)
  - **Incorrectly deletes** `TEST.1` even though it has 798 bytes and wildcard queue bindings

**Evidence from Test:**
```
BEFORE RESTART:
Address: TEST.1 (AddressSize: 798 bytes)
  Bindings (1): mqtt-subscriber.TEST.# (wildcard queue)
  
AFTER RESTART:
AMQ224113: Auto removing address TEST.1  <- BUG!
Address: TEST.1 - DELETED (lost 798 bytes)
```

**Impact:**
- Data loss: Messages in the address are lost
- Inconsistent routing: New messages to `TEST.1` may not route correctly
- User confusion: Bindings shown before restart disappear after restart

**Test Case:** `MqttWildcardAutoDeleteAddressTest.java`

### 2. Cluster Bridge SNF (Store-and-Forward) Queues (NOT AN ISSUE - Included for Completeness)

**Scenario:**
- Cluster connection configured between brokers
- SNF queues created with name pattern: `$internal.sf.<cluster-name>.<node-id>`
- Messages forwarded through cluster are stored in SNF queues before being sent to remote broker

**Code Location:**
```java
// ClusterConnectionImpl.java:991
queue = server.createQueue(
    QueueConfiguration.of(queueName)
        .setRoutingType(RoutingType.MULTICAST)
        .setAutoCreateAddress(true)  // Creates address with SNF queue name
        .setInternal(true)
);

// BridgeImpl.java:534
message = message.copy();  // ALWAYS copies the message before forwarding
```

**Why This Does NOT Cause the Issue:**
Cluster bridges **always copy messages** before forwarding (BridgeImpl.java:533-537):
```java
protected Message beforeForward(Message message, final SimpleString forwardingAddress) {
   message = message.copy();  // Creates new message object with new ID
   message.setAddress(forwardingAddress);  // Can set new address
   return beforeForwardingNoCopy(message, forwardingAddress);
}
```

The copied message is a completely separate object, just like diverts. The SNF queue stores the copy, and size is counted only on the SNF address, not on the original source address.

**Conclusion:**
- Cluster SNF queues do NOT cause addresses to have inflated size from messages belonging elsewhere
- No risk of auto-delete issues related to size inflation
- However, SNF addresses should still be marked as internal/non-auto-deletable for other operational reasons

### 3. Mirror/Replication SNF Queues (For Non-Paged Messages Only)

**Scenario:**
- AMQP broker connection with mirror replication configured
- Mirror creates SNF (Store-and-Forward) queue with dedicated `mirrorSNF` address (e.g., `$ACTIVEMQ_ARTEMIS_MIRROR_mirror`)
- When messages are routed to ANY address on the broker, mirror replication intercepts them

**How Mirror Replication Works - TWO PATHS:**

**Path 1: Paged Messages (or messages needing paging) - NO ISSUE**
```java
// AMQPMirrorControllerSource.java:406-419, 422-428
int creditsWrite = snfQueue.getPagingStore().page(message, tx, pagedRouteContext, this::copyMessageForPaging, true);
// Message is COPIED during paging (line 408 comment: "the message will be copied into paging")

if (message.isPaged()) {
   // if the source was paged, we copy the message
   // We can only use additional references on the queue when not in page mode.
   // otherwise it must be a copy
   message = copyMessageForPaging(message);  // Creates new message with new ID
}
```
- Message is **copied** with `message.copy(newID, false)` - just like diverts
- No size inflation - each address counts only its own copy

**Path 2: Non-Paged Messages (not needing paging) - CAUSES ISSUE**
```java
// AMQPMirrorControllerSource.java:431-434
// Only reaches here if message is NOT paged and paging not needed
MessageReference ref = MessageReference.Factory.createReference(message, snfQueue);
snfQueue.refUp(ref);  // References SAME message object
```

This triggers special mirror logic (QueueImpl.java:775-779):
```java
if (isMirrorController() && owner != null && pagingStore != owner) {
   // When using mirror in this situation, it means the address belong to another queue
   // it's acting as if the message is being copied
   pagingStore.addSize(messageReference.getMessage().getOriginalEstimate(), false, false);
}
```

**Key Point for Non-Paged Messages:** 
The same message object has its size counted in TWO paging stores:
- **Original address** "orders" (where it was first routed) - from normal refUp logic
- **Mirror SNF address** (where it's queued for replication) - from mirror special logic

**Why This Creates the Issue (Non-Paged Path Only):**
- For non-paged messages, mirror doesn't copy - it references the same message object
- But it adds size to its own address's paging store as if it were a copy
- The mirror SNF **address** accumulates size from messages that belong to OTHER addresses
- If the mirror SNF address is auto-created and somehow the SNF queue binding is removed (broker restart, configuration change, etc.)
- The address has size > 0 but no bindings → could be auto-deleted
- Non-paged messages pending replication would be lost

**Potential Issue:**
- Mirror SNF address has size > 0 from non-paged messages belonging to other addresses (acting as virtual copies)
- If mirror SNF address is auto-created and loses its queue binding, it could be deleted with pending messages
- Replication would fail and messages could be lost

**Note:** This issue only affects non-paged messages. Paged messages are copied (like diverts) and don't cause size inflation.

**Impact:**
- Mirror replication failure for non-paged messages
- Data inconsistency between mirrored brokers
- Message loss in replication pipeline

### 4. Diverts (NOT AN ISSUE - Included for Completeness)

**Scenario:**
- Diverts copy messages from source address to forward address
- Messages are **copied** with new message ID and new address

**Code Evidence:**
```java
// DivertImpl.java:113-124
copy = message.copy(id);  // New message object, new ID
copy.referenceOriginalMessage(message, this.getUniqueName());
copy.setAddress(forwardAddress);  // Address changed to forward address
postOffice.route(copy, ...);  // Routed as a separate message
```

**Why This Does NOT Cause the Issue:**
- Diverts create a **TRUE COPY** - completely separate message object with new ID
- The copy's address is set to the forward address, NOT the original address
- The copy's owner (PagingStore) is set to the forward address's paging store
- Message size is counted ONLY on the forward address, never on the original address
- This is fundamentally different from mirror/wildcard which use message **references** to the same message

**Conclusion:**
- Diverts do NOT cause addresses to have inflated size from messages belonging elsewhere
- No risk of auto-delete issues with divert source or target addresses

### 5. Federation (NOT AN ISSUE)

**Scenario:**
- Federation creates remote consumers that pull messages from upstream brokers
- Messages are consumed from remote and produced to local queues

**Conclusion:**
- Federation consumers pull messages, they don't create local size inflation
- Federated queues are local queues with local bindings
- No evidence of size counting towards addresses without bindings

## Proposed Solution

### Add Address Size Check to Auto-Delete Logic

**Change:** Modify `SimpleAddressManager.checkAutoRemoveAddress()` to check address size before allowing deletion.

**Implementation:**
```java
@Override
public boolean checkAutoRemoveAddress(AddressInfo addressInfo,
                                      AddressSettings settings,
                                      boolean ignoreDelay) throws Exception {
   // Existing checks
   boolean canDelete = settings.isAutoDeleteAddresses() && 
                       addressInfo != null && 
                       addressInfo.isAutoCreated() && 
                       !bindingsFactory.isAddressBound(addressInfo.getName()) && 
                       (ignoreDelay || addressWasUsed(addressInfo, settings)) && 
                       (ignoreDelay || delayCheck(addressInfo, settings));
   
   if (!canDelete) {
      return false;
   }
   
   // NEW: Check if address has messages before allowing deletion
   PagingStore pagingStore = pagingManager.getStore(addressInfo.getName());
   if (pagingStore != null && pagingStore.getAddressSize() > 0) {
      logger.debug("Address {} has size {} bytes, cannot auto-delete", 
                   addressInfo.getName(), pagingStore.getAddressSize());
      return false;
   }
   
   return true;
}
```

**Why This Solution:**
- Simple and comprehensive fix that addresses the root cause
- Prevents deletion of ANY address with messages, regardless of the reason for the size
- Handles all current cases (wildcard queues, mirror SNF non-paged messages) automatically
- Future-proof: protects against any new cases where addresses accumulate size without direct bindings
- Low risk: conservative approach prevents data loss
- Minimal code change with clear semantics

## Testing Requirements

1. **MQTT Wildcard Queue Test:** `MqttWildcardAutoDeleteAddressTest.java` (already exists)
   - Verify `TEST.1` survives restart with MQTT wildcard subscription
   - Verify address size is preserved
   - Demonstrates the bug with wildcard queue bindings

2. **Mirror SNF Test:** (needs to be created)
   - Configure AMQP mirror replication
   - Send non-paged messages to replicate
   - Restart broker and verify mirror SNF address survives
   - Verify replication continues after restart

3. **Core API Test:** (needs to be created)
   - Test address with size > 0 but no bindings after broker restart
   - Verify address is NOT deleted while size > 0
   - Verify address IS deleted when size == 0

## Related Files

**Core Logic (Fix Location):**
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/postoffice/impl/SimpleAddressManager.java:414`
  - `checkAutoRemoveAddress()` method - needs size check added
  - Needs access to `PagingManager` to get paging store and check address size

**Wildcard Queue References:**
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/postoffice/impl/WildcardAddressManager.java`
  - Manages wildcard address matching

**Cluster SNF (Context):**
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/server/cluster/impl/ClusterConnectionImpl.java:94,991`
  - SNF queue creation (uses message copies, not affected by this bug)
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/server/cluster/impl/BridgeImpl.java:534`
  - Bridge always copies messages

**Mirror SNF:**
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/server/impl/QueueImpl.java:775-796`
  - Mirror controller size accounting
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/config/amqpBrokerConnectivity/AMQPMirrorBrokerConnectionElement.java`
  - Mirror SNF configuration

**Paging Store:**
- `artemis-server/src/main/java/org/apache/activemq/artemis/core/paging/impl/PagingStoreImpl.java:534`
  - `getAddressSize()` method - used for size check

**Test Files:**
- `tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/mqtt/MqttWildcardAutoDeleteAddressTest.java`
  - Existing wildcard queue test
- `tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/mqtt/BUG_ANALYSIS.md`
  - Detailed bug analysis with evidence

## Priority

**CRITICAL** - This is a data loss bug that affects production deployments using:
- Wildcard subscriptions (MQTT, AMQP, Core)
- Cluster connections
- Mirror replication
- Auto-delete addresses

## Next Steps

1. Review this document with the team
2. Decide on implementation approach (recommend Solution 4)
3. Create JIRA ticket with this analysis
4. Implement fix with comprehensive tests
5. Document in release notes as bug fix
