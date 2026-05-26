# MQTT Wildcard Queue Auto-Delete Bug Analysis

## Bug Confirmed: Auto-Delete Logic Does Not Check Wildcard Queue Bindings

The diagnostic output from the test proves this is an **auto-delete bug**, not a console bug.

## Evidence from Test Execution

### BEFORE RESTART (testWildcardQueueBindingPreventsAutoDelete)

```
Addresses and their queues:
  Address: TEST.1 (AutoCreated: true, RoutingTypes: [MULTICAST], AddressSize: 798 bytes)
    Bindings (1):
      - mqtt-subscriber.TEST.# (Type: LOCAL_QUEUE)
    Queues (1):
      - mqtt-subscriber.TEST.# (Durable: true, Messages: 1, Consumers: 0, PersistentSize: 240 bytes)

  Address: TEST.# (AutoCreated: true, RoutingTypes: [MULTICAST], AddressSize: 72 bytes)
    Bindings (1):
      - mqtt-subscriber.TEST.# (Type: LOCAL_QUEUE)
    Queues (1):
      - mqtt-subscriber.TEST.# (Durable: true, Messages: 1, Consumers: 0, PersistentSize: 240 bytes)
```

**Key Findings:**
1. ✓ The wildcard queue `mqtt-subscriber.TEST.#` **IS** bound to address `TEST.1`
2. ✓ The binding is real and active (queue has 1 message received from TEST.1)
3. ✓ The same queue appears in both addresses' binding lists
4. ✓ Address `TEST.1` has **798 bytes** in paging store (proving it holds real data)
5. ✓ Address `TEST.#` has **72 bytes** in paging store
6. ✓ Queue persistent size is **240 bytes** (the actual message size)

### AFTER RESTART

```
AMQ224113: Auto removing address TEST.1

Addresses and their queues:
  Address: TEST.# (AutoCreated: true, RoutingTypes: [MULTICAST], AddressSize: 72 bytes)
    Bindings (1):
      - mqtt-subscriber.TEST.# (Type: LOCAL_QUEUE)
    Queues (1):
      - mqtt-subscriber.TEST.# (Durable: true, Messages: 1, Consumers: 0, PersistentSize: 240 bytes)
```

**Key Findings:**
1. ✗ Address `TEST.1` was **incorrectly auto-deleted** during broker startup
2. ✗ The **798 bytes** in `TEST.1` paging store was lost when the address was deleted
3. ✓ The wildcard queue `mqtt-subscriber.TEST.#` survived (it's durable)
4. ✓ The **240 bytes** of persistent queue data survived the restart
5. ✗ The auto-delete logic did not detect the wildcard queue's binding to `TEST.1`
6. ✗ Despite having 798 bytes in the paging store, `TEST.1` was still deleted

## Root Cause Analysis

The auto-delete logic during broker restart is checking if an address has queues bound to it, but it's **failing to detect wildcard queue bindings**.

### How Wildcard Queues Work

1. MQTT client subscribes to `TEST/#` (wildcard subscription)
2. Artemis creates queue `mqtt-subscriber.TEST.#` bound to address `TEST.#`
3. When a message is published to `TEST.1`:
   - Address `TEST.1` is auto-created
   - The message is routed via multicast to matching wildcard subscriptions
   - The wildcard queue `mqtt-subscriber.TEST.#` receives the message
   - **The queue gets bound to the specific address `TEST.1`**

4. On broker restart:
   - The durable queue `mqtt-subscriber.TEST.#` is restored
   - Address `TEST.#` is restored with the queue binding
   - Address `TEST.1` is restored from bindings journal
   - **BUG**: Auto-delete logic checks `TEST.1` and fails to find the wildcard queue binding
   - Address `TEST.1` is incorrectly removed

## Why This Is a Problem

1. **Data Loss Risk**: Messages in the queue for `TEST.1` topics could be lost
2. **Inconsistent State**: Before restart, the binding exists; after restart, it doesn't
3. **User Confusion**: The console shows the binding before restart, then it disappears
4. **Violates Expectations**: Addresses with bound queues should not be auto-deleted
5. **Persistent Data Ignored**: Even with 240 bytes of persistent data, the address is deleted
6. **Wildcard Routing Broken**: After restart, new messages to `TEST.1` may not route to the wildcard queue

## Expected Behavior

When checking if an address can be auto-deleted, the logic should:
1. Check for direct queue bindings (currently working)
2. **Check for wildcard queue bindings** (currently NOT working)
3. Only delete if there are no bindings of any kind

An address like `TEST.1` should survive restart if:
- Any durable queue is bound to it, OR
- Any wildcard queue (e.g., `TEST.#`) has a binding to it

## Fix Required

The auto-delete logic in the broker startup code needs to be updated to:
1. When checking if address `TEST.1` can be deleted
2. Look for ALL queues bound to it, including wildcard queues
3. If ANY queue (direct or wildcard) is bound, do NOT auto-delete

## Test Results

Both test methods successfully reproduce the bug:

1. **testWildcardQueueBindingPreventsAutoDelete**: 
   - Status: **FAILS** (expected behavior after fix: PASS)
   - Proves the wildcard queue IS bound before restart
   - Proves TEST.1 is incorrectly deleted after restart

2. **testWildcardSubscriptionPreventsAutoDeleteAfterRestart**:
   - Status: **PASSES** (currently expects buggy behavior)
   - Confirms TEST.1 is deleted after restart
   - Includes diagnostic message: "BUG REPRODUCED"

## Files Created

1. `MqttWildcardAutoDeleteAddressTest.java` - Test reproduction
2. `MqttWildcardAutoDeleteAddressTest_README.md` - Test documentation
3. `BUG_ANALYSIS.md` - This file with detailed analysis

## Next Steps for Developers

1. Review the auto-delete logic in the broker startup code
2. Identify where address auto-deletion decisions are made
3. Update the logic to check for wildcard queue bindings
4. Run the test to verify the fix
5. Update test assertions to expect correct behavior
