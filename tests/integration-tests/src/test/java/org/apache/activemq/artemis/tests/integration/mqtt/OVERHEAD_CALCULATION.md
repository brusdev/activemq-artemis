# Source Code Analysis: 558 Bytes Paging Overhead

## Overview

The 558 bytes overhead (69.9% of 798 total) is **not explicitly calculated** by Artemis. Instead, it's the natural consequence of how messages are encoded for paging storage vs. how queue sizes are tracked.

## The Two Size Metrics

### 1. Address Size (798 bytes) - What's Written to Disk

**Source**: `PagingStoreImpl.getAddressSize()` → Line 534-536

```java
public long getAddressSize() {
   return size.getSize();  // Returns total bytes written to paging files
}
```

This accumulates the actual bytes written to paging files.

**Where bytes are written**: `PageReadWriter.writeMessage()` → Line 74-96

```java
public static int writeMessage(PagedMessage message, SequentialFileFactory fileFactory, SequentialFile file) {
   final int messageEncodedSize = message.getEncodeSize();  // Line 75
   final int bufferSize = messageEncodedSize + SIZE_RECORD; // Line 76: Add record overhead
   // ... write to file ...
   return bufferSize;  // Line 96: This many bytes written
}
```

**SIZE_RECORD overhead**: `PageReadWriter.SIZE_RECORD` → Line 45

```java
public static final int SIZE_RECORD = DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + DataConstants.SIZE_BYTE;
```

= 1 (START_BYTE) + 4 (message size int) + 1 (END_BYTE) = **6 bytes**

**Message encoding size**: `PagedMessageImpl.getEncodeSize()` → Line 268-275

```java
public int getEncodeSize() {
   if (LargeMessageType.isCoreLargeMessage(message)) {
      return DataConstants.SIZE_LONG + DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + 
             LargeMessagePersister.getInstance().getEncodeSize((LargeServerMessage)message) +
             DataConstants.SIZE_INT + queueIDs.length * DataConstants.SIZE_LONG;
   } else {
      // For normal messages:
      return DataConstants.SIZE_LONG +                              // 8 bytes: transactionID
             DataConstants.SIZE_BYTE +                              // 1 byte:  largeMessageType flag
             message.getPersister().getEncodeSize(message) +        // Variable: encoded message
             DataConstants.SIZE_INT +                               // 4 bytes: queueIDs array length
             queueIDs.length * DataConstants.SIZE_LONG;             // 8 * N:   queue IDs
   }
}
```

**Where PagedMessage is created**: `PagingStoreImpl.page()` → Line 1496

```java
pagedMessage = new PagedMessageImpl(message, routeQueues(tx, listCtx), transactionID);
```

The `routeQueues(tx, listCtx)` returns the array of queue IDs that this message is routed to.

### 2. Queue Persistent Size (240 bytes) - Message Payload Only

**Source**: `PagedMessageImpl.getPersistentSize()` → Line 289-291

```java
public long getPersistentSize() throws ActiveMQException {
   return message.getPersistentSize();  // Just the message data, NO routing overhead
}
```

This returns **only** the message payload size, excluding:
- Transaction ID
- Large message type flag
- Queue IDs array
- Page file record markers

## Breakdown of the 558 Bytes Overhead

For the test message sent to `TEST.1` with wildcard queue `TEST.#`:

```
Total bytes written to paging file (Address Size):    798 bytes

Composed of:
├─ PagedMessage overhead:
│  ├─ transactionID (SIZE_LONG)                         8 bytes
│  ├─ largeMessageType (SIZE_BYTE)                      1 byte
│  ├─ queueIDs.length (SIZE_INT)                        4 bytes
│  └─ queueIDs array (1 queue * SIZE_LONG)              8 bytes
│
├─ Page record overhead (SIZE_RECORD):
│  ├─ START_BYTE                                        1 byte
│  ├─ message size (SIZE_INT)                           4 bytes
│  └─ END_BYTE                                          1 byte
│
├─ Message encoding overhead:
│  └─ MQTT/Artemis headers, properties, metadata      ~332 bytes
│
└─ Message payload (persistent size):                 240 bytes
                                                      ──────────
    TOTAL:                                            798 bytes

Queue Persistent Size:                                240 bytes
Overhead (calculated by test):                        558 bytes (69.9%)
```

## Key Source Files

1. **PagingStoreImpl.java** - Lines 1464-1563
   - `page()` method: Creates PagedMessage with routing info
   - Line 1496: `new PagedMessageImpl(message, routeQueues(tx, listCtx), transactionID)`
   - Line 1540: `bytesToWrite = pagedMessage.getEncodeSize() + PageReadWriter.SIZE_RECORD`

2. **PagedMessageImpl.java** - Lines 95-98, 246-291
   - Constructor stores `queueIDs` array (routing overhead)
   - `encode()` method: Writes transactionID, type flag, message, and queueIDs
   - `getEncodeSize()` method: Calculates total encoded size with routing
   - `getPersistentSize()` method: Returns ONLY message size (no routing)

3. **PageReadWriter.java** - Lines 45, 74-97
   - `SIZE_RECORD` constant: 6 bytes for page file record markers
   - `writeMessage()` method: Writes encoded message + record overhead

4. **Page.java** - Lines 184-200
   - `writeDirect()` method: Line 198 accumulates size from PageReadWriter.writeMessage()

## Why This Matters for the Auto-Delete Bug

When address `TEST.1` is deleted:

1. The **798 bytes** in the paging store are **lost** (address deleted)
2. The **240 bytes** in the queue's persistent storage **survive** (queue is durable)
3. The **558 bytes of routing metadata** is **lost**
4. This breaks the ability to deliver those paged messages after restart

The routing overhead isn't wasteful - it's essential for multicast routing to multiple queues. When the address is deleted, this routing infrastructure is destroyed, even though the queue that needs it still exists.

## Summary

The 558 bytes overhead is NOT calculated by a single method. It emerges from:

- **What's added to address size**: Full encoded message with routing = `message.getEncodeSize() + 6` (PageReadWriter.writeMessage)
- **What's added to queue size**: Just message payload = `message.getPersistentSize()` (PagedMessageImpl.getPersistentSize)
- **The difference**: Routing envelope (21 bytes) + page record (6 bytes) + message encoding overhead (~531 bytes)

The test calculates it as: `addressSize - queuePersistentSize = 798 - 240 = 558 bytes`
