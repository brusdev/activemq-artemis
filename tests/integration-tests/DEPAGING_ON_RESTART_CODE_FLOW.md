# Artemis Code Flow: Depaging After Restart

This document explains exactly what Artemis code causes eager depaging during broker restart.

## The Complete Call Chain

```
Server Startup
    ↓
AbstractJournalStorageManager.loadBindingJournal()
    ↓
PagingManager.processReload()  [Line 1403 in AbstractJournalStorageManager.java]
    ↓
PagingStoreImpl.processReload()  [Called for each page store]
    ↓
PageCursorProviderImpl.processReload()  [Line 652 in PagingStoreImpl.java]
    ↓
PageSubscriptionImpl.processReload()  [Called for each subscription - Line 115 in PageCursorProviderImpl.java]
    ↓
PostOfficeJournalLoader.postLoad()  [Line 1406 in AbstractJournalStorageManager.java]
    ↓
Queue.resume()  [Line 283 in PostOfficeJournalLoader.java]
    ↓
QueueImpl.deliverAsync()  [Line 2769 in QueueImpl.java]
    ↓
QueueImpl.DeliverRunner.run()  [Line 1110 in QueueImpl.java - executed async]
    ↓
QueueImpl.checkDepage()  [Line 4238 in DeliverRunner.run()]
    ↓
QueueImpl.scheduleDepage()  [Line 3155 in QueueImpl.java]
    ↓
QueueImpl.depage()  [Line 3276 in QueueImpl.java - executed async]
    ↓
PageIterator.tryNext() / PageIterator.next()  [Line 3304, 3312 in QueueImpl.depage()]
    ↓
QueueImpl.addTail(reference, false)  [Line 3316 in QueueImpl.java]
    ↓
Messages are now in the queue, no longer in page files!
```

## Key Files and Line Numbers

### 1. **AbstractJournalStorageManager.java:1403**
```java
// To recover positions on Iterators
if (pagingManager != null) {
    // it could be null on certain tests that are not dealing with paging
    // This could also be the case in certain embedded conditions
    pagingManager.processReload();
}
```
**Purpose:** Triggers page reload during journal loading

### 2. **PagingManagerImpl.java:564**
```java
public void processReload() throws Exception {
    logger.debug("Processing reload");
    for (PagingStore store : stores.values()) {
        logger.debug("Processing reload on page store {}", store.getAddress());
        store.processReload();
    }
}
```
**Purpose:** Iterates all page stores and processes reload for each

### 3. **PagingStoreImpl.java:651**
```java
public void processReload() throws Exception {
    cursorProvider.processReload();
}
```
**Purpose:** Delegates to cursor provider

### 4. **PageCursorProviderImpl.java:115**
```java
public void processReload() throws Exception {
    Collection<PageSubscription> cursorList = this.activeCursors.values();
    for (PageSubscription cursor : cursorList) {
        cursor.processReload();
    }
    
    if (!cursorList.isEmpty()) {
        // If you ack out of order, the min page could be beyond the first page
        long cursorsMinPage = checkMinPage(cursorList);
        
        if (cursorsMinPage != Long.MAX_VALUE) {
            for (long startPage = pagingStore.getFirstPage(); startPage < cursorsMinPage; startPage++) {
                for (PageSubscription cursor : cursorList) {
                    cursor.reloadPageInfo(startPage);
                }
            }
        }
    }
    
    cleanup();
}
```
**Purpose:** Processes reload for each subscription and calls cleanup()

### 5. **PostOfficeJournalLoader.java:282**
```java
public void postLoad(Journal messageJournal,
                     ResourceManager resourceManager,
                     Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception {
    for (Queue queue : queues.values()) {
        if (!queue.isPersistedPause()) {
            queue.resume();  // ← THIS TRIGGERS DEPAGING!
        }
    }
}
```
**Purpose:** Resumes all non-paused queues after journal load

### 6. **QueueImpl.java:2757**
```java
public synchronized void resume() {
    paused = false;
    
    if (pauseStatusRecord >= 0) {
        try {
            storageManager.deleteQueueStatus(pauseStatusRecord);
        } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.unableToResumeQueue(e);
        }
        pauseStatusRecord = -1;
    }
    
    deliverAsync();  // ← Schedules delivery which triggers depaging
}
```
**Purpose:** Resumes queue and triggers async delivery

### 7. **QueueImpl.java:1102**
```java
public void deliverAsync() {
    deliverAsync(false);
}

private void deliverAsync(boolean noWait) {
    if (scheduledRunners.get() < MAX_SCHEDULED_RUNNERS) {
        scheduledRunners.incrementAndGet();
        try {
            getExecutor().execute(deliverRunner);  // ← Executes DeliverRunner
        } catch (RejectedExecutionException ignored) {
            scheduledRunners.decrementAndGet();
        }
    }
}
```
**Purpose:** Schedules the delivery runner asynchronously

### 8. **QueueImpl.java:4216 (DeliverRunner class)**
```java
private final class DeliverRunner implements Runnable {
    @Override
    public void run() {
        try {
            boolean needCheckDepage = false;
            try (ArtemisCloseable metric = measureCritical(CRITICAL_DELIVER)) {
                deliverLock.lock();
                try {
                    needCheckDepage = deliver();
                } finally {
                    deliverLock.unlock();
                }
            }
            
            if (needCheckDepage) {
                try (ArtemisCloseable metric = measureCritical(CRITICAL_CHECK_DEPAGE)) {
                    checkDepage();  // ← Checks if depaging is needed
                }
            }
        } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorDelivering(e);
        }
    }
}
```
**Purpose:** Executes delivery and checks if depaging is needed

### 9. **QueueImpl.java:3143**
```java
private void checkDepage() {
    if (queueDestroyed) {
        return;
    }
    if (pageIterator != null && pageSubscription.isStorePaging()) {
        if (logger.isDebugEnabled()) {
            logger.debug("CheckDepage on queue name {}, id={}", 
                        queueConfiguration.getName(), queueConfiguration.getId());
        }
        // we will issue a delivery runnable to check for released space from acks and resume depage
        pageDelivered = true;
        
        if (!depagePending && needsDepage() && pageIterator.tryNext() != PageIterator.NextResult.noElements) {
            scheduleDepage(false);  // ← Schedules depaging!
        }
    } else {
        pageDelivered = false;
    }
}
```
**Purpose:** Checks if page store is paging and schedules depaging if needed

### 10. **QueueImpl.java:3271**
```java
private void scheduleDepage(final boolean scheduleExpiry) {
    if (!depagePending) {
        logger.trace("Scheduling depage for queue {}", queueConfiguration.getName());
        
        depagePending = true;
        pageSubscription.getPagingStore().execute(() -> depage(scheduleExpiry));  // ← Executes depage!
    }
}
```
**Purpose:** Schedules the actual depaging operation

### 11. **QueueImpl.java:3280 - THE CORE DEPAGING LOGIC**
```java
private void depage(final boolean scheduleExpiry) {
    depagePending = false;
    
    if (!depageLock.tryLock()) {
        return;
    }
    
    try {
        synchronized (this) {
            if (isPaused() || pageIterator == null) {
                return;
            }
        }
        
        long timeout = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DELIVERY_TIMEOUT);
        
        this.directDeliver = false;
        
        int depaged = 0;
        while (timeout - System.nanoTime() > 0 && needsDepage()) {
            PageIterator.NextResult status = pageIterator.tryNext();
            if (status == PageIterator.NextResult.retry) {
                continue;
            } else if (status == PageIterator.NextResult.noElements) {
                break;
            }
            
            depaged++;
            PagedReference reference = pageIterator.next();  // ← Read message from page
            if (logger.isDebugEnabled()) {
                logger.debug("Depaging reference {} on queue {} depaged::{}", 
                           reference, queueConfiguration.getName(), depaged);
            }
            addTail(reference, false);  // ← ADD MESSAGE TO QUEUE!
            pageIterator.remove();
        }
        
        deliverAsync(true);
        
        if (depaged > 0 && scheduleExpiry) {
            expireReferences();
        }
    } finally {
        depageLock.unlock();
    }
}
```
**Purpose:** **THE ACTUAL DEPAGING** - Reads messages from page files and adds them to the queue

## Why This Causes Eager Depaging

The key difference between runtime and restart:

### During Runtime:
- `checkDepage()` is only called when:
  - A consumer becomes active and starts consuming
  - Messages are delivered
  - Space is freed up in the queue
- **Lazy behavior:** Depaging happens on-demand based on consumer activity

### During Restart:
- `queue.resume()` is called for ALL queues during `PostOfficeJournalLoader.postLoad()`
- This triggers `deliverAsync()` → `DeliverRunner` → `checkDepage()` → `scheduleDepage()` → `depage()`
- **Eager behavior:** Depaging happens IMMEDIATELY for all queues, regardless of consumer presence
- The goal is **durability** - ensure all persisted messages are properly loaded and routed

## Log Messages You'll See

During restart, you'll see this sequence in the logs:

```
AMQ222038: Starting paging on address 'X'        ← Page files loaded from disk
AMQ224108: Stopped paging on address 'X'         ← Depaging completed
AMQ224113: Auto removing address X               ← Source address deleted (if auto-delete enabled)
```

This happens because:
1. Pages are loaded during `PagingStoreImpl.start()`
2. Messages are depaged during `QueueImpl.depage()`
3. Source address becomes empty after routing, triggering auto-delete

## Summary

**The root cause of eager depaging after restart is:**

`PostOfficeJournalLoader.postLoad()` calls `queue.resume()` on ALL queues, which triggers async delivery, which calls `checkDepage()`, which schedules `depage()`, which reads all messages from page files and adds them to queues via `addTail()`.

This is **intentional behavior** to ensure message durability and proper recovery of all persisted messages.
