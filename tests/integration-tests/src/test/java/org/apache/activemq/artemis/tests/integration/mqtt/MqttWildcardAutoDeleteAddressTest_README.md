# MQTT Wildcard Auto-Delete Address Test

## Purpose

This test reproduces an issue where addresses are incorrectly auto-deleted when they have wildcard queue bindings after a broker restart.

## Bug Status: REPRODUCED ✓

The test successfully reproduces the bug. See test output showing:
```
AMQ224113: Auto removing address TEST\.1
BUG REPRODUCED: TEST.1 was auto-deleted after restart even though wildcard queue exists
```

## Issue Description

When using MQTT with the following setup:
- Auto-delete addresses enabled for a wildcard pattern (e.g., `TEST.#`)
- An MQTT durable subscriber with a wildcard subscription (e.g., `TEST/#`)
- Messages sent to a specific topic (e.g., `TEST.1`)

**Expected behavior:**
1. The wildcard queue (created by the subscriber) receives messages sent to `TEST.1`
2. When the broker restarts, addresses that have bound queues should NOT be auto-deleted
3. The specific address `TEST.1` should survive the restart because the wildcard queue `TEST.#` is bound to it

**Actual behavior (BUG):**
1. After a broker restart, the specific address `TEST.1` is **incorrectly deleted**
2. This indicates one of two root causes:
   - **Console Bug**: The queue `TEST.#` is not actually bound to the address `TEST.1` (only to `TEST.#`), but the console incorrectly shows the binding
   - **Auto-delete Bug**: The queue `TEST.#` IS bound to `TEST.1`, but the auto-delete logic doesn't properly check for wildcard queue bindings

## Test Cases

### testWildcardQueueBindingPreventsAutoDelete

Comprehensive test that verifies the bug:
1. Creates an MQTT durable subscriber with wildcard subscription `TEST/#`
2. Sends a message to `TEST.1`
3. Verifies the wildcard queue received the message (proving it's bound to `TEST.1`)
4. Restarts the broker
5. Checks if `TEST.1` address still exists 

**Current Result**: FAILS - `TEST.1` is incorrectly deleted
**Expected After Fix**: PASS - `TEST.1` should survive

### testWildcardSubscriptionPreventsAutoDeleteAfterRestart

Simplified minimal reproduction:
1. Creates durable wildcard subscriber for `TEST/#`
2. Sends one message to `TEST.1`
3. Restarts broker
4. Confirms `TEST.1` was deleted (bug reproduction)

**Current Result**: PASS - confirms the bug exists
**Expected After Fix**: Change assertion from `assertNull` to `assertNotNull`, then should PASS

## How to Run

From the `tests/integration-tests` directory:

```bash
mvn test -Dtest=MqttWildcardAutoDeleteAddressTest -DskipIntegrationTests=false
```

Or run a specific test method:

```bash
mvn test -Dtest=MqttWildcardAutoDeleteAddressTest#testWildcardSubscriptionPreventsAutoDeleteAfterRestart -DskipIntegrationTests=false
```

## Test Results

### Current State (Bug Present)
- `testWildcardQueueBindingPreventsAutoDelete`: **FAILS** ❌ - TEST.1 incorrectly deleted
- `testWildcardSubscriptionPreventsAutoDeleteAfterRestart`: **PASSES** ✓ - Confirms bug exists

### After Fix
Both tests should be updated to expect `TEST.1` to survive restart, then both should PASS.

## Evidence from Test Output

```
[main] 07:03:43,916 INFO  [org.apache.activemq.artemis.core.server] AMQ224113: Auto removing address TEST\.1
```

This log message confirms that `TEST.1` is being auto-deleted during broker startup even though:
1. The wildcard queue `TEST.#` exists
2. The wildcard queue received messages from `TEST.1` (proving binding works)

## Related Configuration

The test configures:
```java
new AddressSettings()
   .setAutoDeleteAddresses(true)
   .setAutoCreateQueues(true)
   .setAutoCreateAddresses(true)
```

Applied to match pattern: `TEST.#`

This matches the user's reported broker configuration.
