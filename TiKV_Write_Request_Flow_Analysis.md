# TiKV Write Request Flow: From Client to RocksDB Persistence

## Table of Contents
1. [Overview](#overview)
2. [Architecture Overview](#architecture-overview)
3. [Detailed Write Flow](#detailed-write-flow)
4. [Thread Pool Analysis](#thread-pool-analysis)
5. [FSM State Transitions](#fsm-state-transitions)
6. [RocksDB Persistence Details](#rocksdb-persistence-details)
7. [Performance Considerations](#performance-considerations)
8. [Error Handling](#error-handling)
9. [Code References](#code-references)
10. [Summary](#summary)

## Overview

This document provides a comprehensive analysis of how write requests flow through TiKV's RaftStore system, from the initial client request to final persistence in RocksDB. The analysis covers all thread pools, Finite State Machines (FSMs), and the intricate coordination required to maintain both high performance and strong consistency guarantees.

### Key Design Principles
- **Sequential processing per region**: Maintains Raft consensus ordering
- **Parallel processing across regions**: Enables high throughput
- **Batch processing**: Reduces overhead and improves efficiency
- **Two-phase commit**: Raft log first, then state machine application
- **Dedicated worker threads**: Specialized handling for different operations

## Architecture Overview

### Thread Pool Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    TiKV Server Threads                      │
├─────────────────────────────────────────────────────────────┤
│  Main Server Threads                                        │
│  ├── Client Request Handler                                 │
│  └── HTTP/gRPC Interface                                    │
├─────────────────────────────────────────────────────────────┤
│  Batch System Pollers                                       │
│  ├── Store FSM Poller Threads                              │
│  ├── Peer FSM Poller Threads                               │
│  └── Apply FSM Poller Threads                              │
├─────────────────────────────────────────────────────────────┤
│  Specialized Workers                                        │
│  ├── Async Write Workers                                   │
│  ├── Async Read Workers                                    │
│  ├── Background Workers                                    │
│  └── High Priority Pool                                    │
└─────────────────────────────────────────────────────────────┘
```

### FSM Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Store FSM     │    │    Peer FSM     │    │   Apply FSM     │
│                 │    │                 │    │                 │
│ • Route msgs    │───▶│ • Raft consensus│───▶│ • State machine │
│ • Manage regions│    │ • Propose cmds  │    │ • Apply entries │
│ • Handle ticks  │    │ • Handle ready  │    │ • Write to DB   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Detailed Write Flow

### Stage 1: Client Request Entry

**Thread Pool**: Main Server Thread  
**Files**: `src/server/raftkv/mod.rs`, `src/storage/mod.rs`

```rust
// Client write request entry point
async fn async_write(
    &self,
    ctx: &Context,
    batch: WriteData,
    subscribed: u8,
    on_applied: Option<OnAppliedCb>,
) -> WriteRes {
    // Validate request
    if batch.modifies.is_empty() {
        return Err(KvError::from(KvErrorInner::EmptyRequest));
    }
    
    // Convert to RaftCmdRequest
    let mut cmd = RaftCmdRequest::default();
    cmd.set_header(new_request_header(ctx));
    cmd.set_requests(reqs.into());
    
    // Schedule the command
    let (tx, rx) = WriteResFeed::pair();
    let proposed_cb = self.schedule_raft_command(cmd, tx);
}
```

**Process**:
1. Client sends write request via gRPC/HTTP
2. Request is validated and converted to `WriteData`
3. `RaftCmdRequest` is constructed with proper headers
4. Request is scheduled for processing by the appropriate region

### Stage 2: Store FSM Processing

**Thread Pool**: Store FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/store.rs`

```rust
// Store FSM message handling
fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg<EK>>) {
    let timer = SlowTimer::from_millis(100);
    let count = msgs.len();
    
    for m in msgs.drain(..) {
        distribution[m.discriminant()] += 1;
        match m {
            StoreMsg::Tick(tick) => self.on_tick(tick),
            StoreMsg::RaftMessage(msg) => {
                if !self.ctx.coprocessor_host.on_raft_message(&msg.msg) {
                    continue;
                }
                if let Err(e) = self.on_raft_message(msg) {
                    // Handle errors appropriately
                }
            }
            // ... other message types
        }
    }
}

// Route message to specific region
fn on_raft_message(&mut self, msg: Box<InspectedRaftMessage>) -> Result<()> {
    let region_id = msg.msg.get_region_id();
    let msg = match self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg, None)) {
        Ok(()) => {
            forwarded.set(true);
            return Ok(());
        }
        Err(TrySendError::Disconnected(PeerMsg::RaftMessage(im, None))) => im.msg,
        // ... handle other errors
    };
}
```

**Process**:
1. Store FSM receives the write request
2. Message is validated and processed
3. Request is routed to the specific region's Peer FSM
4. Uses the router to send `PeerMsg::RaftCommand` to target region

### Stage 3: Peer FSM Processing

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/peer.rs`

```rust
// Peer FSM message handling
fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg<EK, ER>>) {
    for m in msgs.drain(..) {
        if self.fsm.stopped && !matches!(&m, PeerMsg::RaftCommand(_)) {
            continue;
        }
        
        match m {
            PeerMsg::RaftMessage(msg, sent_time) => {
                if let Some(sent_time) = sent_time {
                    let wait_time = sent_time.saturating_elapsed().as_secs_f64();
                    self.ctx.raft_metrics.process_wait_time.observe(wait_time);
                }
                
                if !self.ctx.coprocessor_host.on_raft_message(&msg.msg) {
                    continue;
                }
                
                if let Err(e) = self.on_raft_message(msg) {
                    // Handle errors
                }
            }
            PeerMsg::RaftCommand(cmd) => {
                let propose_time = cmd.send_time.saturating_elapsed();
                self.ctx.raft_metrics.propose_wait_time.observe(propose_time.as_secs_f64());
                
                // Propose the command to Raft
                self.propose_raft_command(msg, cb, diskfullopt);
            }
            // ... other message types
        }
    }
}
```

**Process**:
1. Peer FSM receives the `RaftCommand`
2. Request is validated and permissions are checked
3. `propose_raft_command()` is called to submit to Raft consensus
4. Callback is stored for later execution

### Stage 4: Raft Proposal and Log Entry

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/peer.rs`

```rust
// Propose command to Raft
fn propose_raft_command_internal(
    &mut self,
    mut msg: RaftCmdRequest,
    cb: Callback<EK::Snapshot>,
    diskfullopt: DiskFullOpt,
) {
    // Pre-propose validation
    match self.pre_propose_raft_command(&msg) {
        Ok(Some(resp)) => {
            cb.invoke_with_response(resp);
            return;
        }
        Err(e) => {
            cb.invoke_with_response(new_error(e));
            return;
        }
        _ => (),
    }
    
    // Propose to Raft
    let mut resp = RaftCmdResponse::default();
    let term = self.fsm.peer.term();
    bind_term(&mut resp, term);
    
    if self.fsm.peer.propose(self.ctx, cb, msg, resp, diskfullopt) {
        self.fsm.has_ready = true;
    }
}

// Actual Raft proposal
fn propose_normal(&mut self, poll_ctx: &mut PollContext<EK, ER, T>, req: RaftCmdRequest) -> Result<()> {
    let data = req.write_to_bytes()?;
    poll_ctx.raft_metrics.propose_log_size.observe(data.len() as f64);
    
    if data.len() as u64 > poll_ctx.cfg.raft_entry_max_size.0 {
        return Err(Error::RaftEntryTooLarge {
            region_id: self.region_id,
            entry_size: data.len() as u64,
        });
    }
    
    let propose_index = self.next_proposal_index();
    self.raft_group.propose(ctx.to_vec(), data)?;
    
    // Store callback for later execution
    self.pending_cmds.append_normal(PendingCmd {
        index: propose_index,
        term: self.term(),
        cb,
        // ... other fields
    });
    
    Ok(())
}
```

**Process**:
1. Request is serialized and validated
2. Entry size is checked against limits
3. Command is proposed to Raft consensus
4. Callback is stored in `pending_cmds` queue
5. Raft ensures the entry is replicated and committed

### Stage 5: Raft Ready Processing

**Thread Pool**: Peer FSM Poller Thread  
**Files**: `components/raftstore/src/store/peer_storage.rs`

```rust
// Handle Raft ready with committed entries
pub fn handle_raft_ready(
    &mut self,
    ready: &mut Ready,
    destroy_regions: Vec<metapb::Region>,
) -> Result<(HandleReadyResult, WriteTask<EK, ER>)> {
    let region_id = self.get_region_id();
    let prev_raft_state = self.raft_state().clone();
    
    let mut write_task = WriteTask::new(region_id, self.peer_id, ready.number());
    
    // Handle snapshot if present
    let mut res = if ready.snapshot().is_empty() {
        HandleReadyResult::SendIoTask
    } else {
        let last_first_index = self.first_index().unwrap();
        let (snap_region, for_witness) = self.apply_snapshot(ready.snapshot(), &mut write_task, &destroy_regions)?;
        HandleReadyResult::Snapshot(Box::new(HandleSnapshotResult {
            msgs: ready.take_persisted_messages(),
            snap_region,
            destroy_regions,
            last_first_index,
            for_witness,
        }))
    };
    
    // Append committed entries
    if !ready.entries().is_empty() {
        self.append(ready.take_entries(), &mut write_task);
    }
    
    // Update Raft state
    if self.raft_state().get_last_index() > 0 {
        if let Some(hs) = ready.hs() {
            self.raft_state_mut().set_hard_state(hs.clone());
        }
    }
    
    // Save Raft state if changed
    if prev_raft_state != *self.raft_state() || !ready.snapshot().is_empty() {
        write_task.raft_state = Some(self.raft_state().clone());
    }
    
    Ok((res, write_task))
}
```

**Process**:
1. Raft ready contains committed log entries
2. Log entries are appended to the write task
3. Raft state is updated and saved
4. Write task is prepared for async write workers

### Stage 6: Async Write Worker Processing

**Thread Pool**: Async Write Workers  
**Files**: `components/raftstore/src/store/async_io/write.rs`

```rust
// Async write worker main loop
fn run(&mut self) {
    let mut stopped = false;
    while !stopped {
        let handle_begin = match self.receiver.recv() {
            Ok(msg) => {
                let now = Instant::now();
                stopped |= self.handle_msg(msg);
                now
            }
            Err(_) => return,
        };
        
        // Batch multiple write tasks
        while self.batch.get_raft_size() < self.raft_write_size_limit {
            match self.receiver.try_recv() {
                Ok(msg) => {
                    stopped |= self.handle_msg(msg);
                }
                Err(TryRecvError::Empty) => {
                    if self.batch.should_wait() {
                        self.batch.wait_for_a_while();
                        continue;
                    } else {
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    stopped = true;
                    break;
                }
            };
        }
        
        if self.batch.is_empty() {
            self.clear_latency_inspect();
            continue;
        }
        
        // Write to database
        self.write_to_db(true);
        self.clear_latency_inspect();
    }
}

// Write to RocksDB and RaftDB
pub fn write_to_db(&mut self, notify: bool) {
    if self.batch.is_empty() {
        return;
    }
    
    let timer = Instant::now();
    self.batch.before_write_to_db(&self.metrics);
    
    // Write KV data to RocksDB
    let mut write_kv_time = 0f64;
    if let ExtraBatchWrite::V1(kv_wb) = &mut self.batch.extra_batch_write {
        if !kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("store {}: {} failed to write to kv engine: {:?}", 
                       self.store_id, self.tag, e);
            });
            write_kv_time = duration_to_sec(now.saturating_elapsed());
            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
        }
    }
    
    // Write Raft log to RaftDB
    let mut write_raft_time = 0f64;
    if !self.batch.raft_wbs[0].is_empty() {
        let now = Instant::now();
        self.perf_context.start_observe();
        for i in 0..self.batch.raft_wbs.len() {
            self.raft_engine.consume_and_shrink(
                &mut self.batch.raft_wbs[i],
                true,
                RAFT_WB_SHRINK_SIZE,
                RAFT_WB_DEFAULT_SIZE,
            ).unwrap_or_else(|e| {
                panic!("store {}: {} failed to write to raft engine: {:?}", 
                       self.store_id, self.tag, e);
            });
        }
        write_raft_time = duration_to_sec(now.saturating_elapsed());
        STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
    }
    
    self.batch.after_write_all();
}
```

**Process**:
1. Write tasks are collected and batched together
2. KV data is written to the main RocksDB instance
3. Raft log entries are written to the RaftDB instance
4. Both writes are persisted with appropriate sync options
5. Performance metrics are recorded

### Stage 7: Apply FSM Processing

**Thread Pool**: Apply FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/apply.rs`

```rust
// Apply committed entries to state machine
fn process_raft_cmd(
    &mut self,
    apply_ctx: &mut ApplyContext<EK>,
    index: u64,
    term: u64,
    req: RaftCmdRequest,
) -> ApplyResult<EK::Snapshot> {
    if index == 0 {
        panic!("{} processing raft command needs a none zero index", self.tag);
    }
    
    // Set sync log hint if required
    apply_ctx.sync_log_hint |= should_sync_log(&req);
    
    // Pre-apply hooks
    apply_ctx.host.pre_apply(&self.region, &req);
    
    // Execute the actual write operation
    let (mut cmd, exec_result, should_write) = self.apply_raft_cmd(apply_ctx, index, term, req);
    
    if let ApplyResult::WaitMergeSource(_) = exec_result {
        return exec_result;
    }
    
    debug!(
        "applied command";
        "region_id" => self.region_id(),
        "peer_id" => self.id(),
        "index" => index
    );
    
    // Bind term and find callback
    cmd_resp::bind_term(&mut cmd.response, self.term);
    let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd.request));
    
    // Add to applied batch
    apply_ctx.applied_batch.push(cmd_cb, cmd, &self.observe_info, self.region_id());
    
    if should_write {
        // Write apply state and commit
        self.write_apply_state(apply_ctx.kv_wb_mut());
        apply_ctx.commit(self);
    }
    
    exec_result
}

// Apply context commit
pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
    if delegate.last_flush_applied_index < delegate.apply_state.get_applied_index() {
        delegate.maybe_write_apply_state(self);
    }
    self.commit_opt(delegate, true);
}

fn commit_opt(&mut self, delegate: &mut ApplyDelegate<EK>, persistent: bool) {
    delegate.update_metrics(self);
    if persistent {
        if let (_, Some(seqno)) = self.write_to_db() {
            delegate.unfinished_write_seqno.push(seqno);
        }
        self.prepare_for(delegate);
        delegate.last_flush_applied_index = delegate.apply_state.get_applied_index();
        delegate.has_pending_ssts = false;
    }
    self.kv_wb_last_bytes = self.kv_wb().data_size() as u64;
    self.kv_wb_last_keys = self.kv_wb().count() as u64;
}
```

**Process**:
1. Committed Raft entries are applied to the state machine
2. Actual KV operations (Put/Delete) are executed
3. Changes are accumulated in write batches
4. Apply state is updated to reflect the applied index

### Stage 8: Final RocksDB Persistence

**Thread Pool**: Apply FSM Poller Thread  
**Files**: `components/raftstore/src/store/fsm/apply.rs`

```rust
// Final write to RocksDB
pub fn write_to_db(&mut self) -> (bool, Option<SequenceNumber>) {
    let need_sync = self.sync_log_hint && !self.disable_wal;
    let mut seqno = None;
    
    // Handle pending SSTs first to maintain order
    if !self.pending_ssts.is_empty() {
        let tag = self.tag.clone();
        self.importer.ingest(&self.pending_ssts, &self.engine).unwrap_or_else(|e| {
            panic!("{} failed to ingest ssts {:?}: {:?}", tag, self.pending_ssts, e);
        });
        self.pending_ssts = vec![];
    }
    
    // Write KV data to RocksDB
    if !self.kv_wb_mut().is_empty() {
        self.perf_context.start_observe();
        let mut write_opts = engine_traits::WriteOptions::new();
        write_opts.set_sync(need_sync);
        write_opts.set_disable_wal(self.disable_wal);
        
        if self.disable_wal {
            let sn = SequenceNumber::pre_write();
            seqno = Some(sn);
        }
        
        let seq = self.kv_wb_mut().write_opt(&write_opts).unwrap_or_else(|e| {
            panic!("failed to write to engine: {:?}", e);
        });
        
        if let Some(seqno) = seqno.as_mut() {
            seqno.post_write(seq)
        }
        
        // Report performance metrics
        let trackers: Vec<_> = self
            .applied_batch
            .cb_batch
            .iter()
            .flat_map(|(cb, _)| cb.write_trackers())
            .flat_map(|trackers| trackers.as_tracker_token())
            .collect();
        self.perf_context.report_metrics(&trackers);
        
        self.sync_log_hint = false;
        
        // Shrink write batch if too large
        let data_size = self.kv_wb().data_size();
        if data_size > APPLY_WB_SHRINK_SIZE {
            let kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
            let kv_wb = self.host.on_create_apply_write_batch(kv_wb);
            self.kv_wb = kv_wb;
        } else {
            self.kv_wb_mut().clear();
        }
        
        self.kv_wb_last_bytes = 0;
        self.kv_wb_last_keys = 0;
    }
    
    (need_sync, seqno)
}
```

**Process**:
1. Write batch containing all KV operations is written to RocksDB
2. WAL (Write-Ahead Log) is optionally synced for durability
3. Sequence number is returned for consistency tracking
4. Write batch is cleared or shrunk for memory efficiency
5. Performance metrics are reported

## Thread Pool Analysis

### Thread Pool Hierarchy and Responsibilities

| **Thread Pool** | **Purpose** | **Key Operations** | **Files** |
|----------------|-------------|-------------------|-----------|
| **Main Server Threads** | Client interface | Request validation, gRPC handling | `src/server/raftkv/mod.rs` |
| **Store FSM Poller** | Store-level coordination | Message routing, region management | `components/raftstore/src/store/fsm/store.rs` |
| **Peer FSM Poller** | Raft consensus | Command proposal, ready handling | `components/raftstore/src/store/fsm/peer.rs` |
| **Apply FSM Poller** | State machine application | Entry application, final writes | `components/raftstore/src/store/fsm/apply.rs` |
| **Async Write Workers** | Raft log persistence | RaftDB writes, batching | `components/raftstore/src/store/async_io/write.rs` |
| **Async Read Workers** | Read operations | Local reads, snapshots | `components/raftstore/src/store/worker/read.rs` |
| **Background Workers** | Maintenance tasks | Cleanup, compaction | Various worker files |

### Thread Coordination

```rust
// Batch system coordination
pub fn poll(&mut self) {
    let mut batch = Batch::with_capacity(self.max_batch_size);
    let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);
    
    while run && self.fetch_fsm(&mut batch) {
        // Process control FSM first
        if batch.control.is_some() {
            let len = self.handler.handle_control(batch.control.as_mut().unwrap());
            // Handle control FSM results
        }
        
        // Process normal FSMs (regions) in batch
        for (i, p) in batch.normals.iter_mut().enumerate() {
            let res = self.handler.handle_normal(p);
            // Handle normal FSM results
        }
        
        // Batch processing complete
        self.handler.end(&mut batch.normals);
    }
}
```

## FSM State Transitions

### Store FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Idle      │───▶│ Processing  │───▶│   Ready     │
│             │    │ Messages    │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
       ▲                   │                   │
       └───────────────────┴───────────────────┘
```

### Peer FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Follower  │───▶│   Candidate │───▶│    Leader   │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
       ▲                   │                   │
       └───────────────────┴───────────────────┘
```

### Apply FSM States

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Waiting   │───▶│  Applying   │───▶│  Committed  │
│             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

## RocksDB Persistence Details

### Two-Phase Write Process

1. **Phase 1: Raft Log Persistence**
   - Raft log entries are written to RaftDB
   - Ensures durability of consensus decisions
   - Required for Raft protocol compliance

2. **Phase 2: State Machine Application**
   - Committed entries are applied to the state machine
   - Actual KV operations are executed
   - Changes are written to the main RocksDB instance

### Write Batch Optimization

```rust
// Write batch management
pub struct WriteTaskBatch<EK, ER> {
    raft_wbs: Vec<ER::LogBatch>,
    raft_states: HashMap<u64, RaftLocalState>,
    extra_batch_write: ExtraBatchWrite<EK>,
    tasks: Vec<WriteTask<EK, ER>>,
    // ... other fields
}

impl<EK, ER> WriteTaskBatch<EK, ER> {
    fn add_write_task(&mut self, raft_engine: &ER, mut task: WriteTask<EK, ER>) {
        // Split large batches if needed
        if self.raft_wb_split_size > 0
            && self.raft_wbs.last().unwrap().persist_size() >= self.raft_wb_split_size
        {
            self.flush_states_to_raft_wb();
            self.raft_wbs.push(raft_engine.log_batch(RAFT_WB_DEFAULT_SIZE));
        }
        
        // Merge write batches
        let raft_wb = self.raft_wbs.last_mut().unwrap();
        if let Some(wb) = task.raft_wb.take() {
            raft_wb.merge(wb).unwrap();
        }
        
        // Append entries
        raft_wb.append(
            task.region_id,
            task.overwrite_to,
            std::mem::take(&mut task.entries),
        ).unwrap();
    }
}
```

## Performance Considerations

### Batch Processing Benefits

1. **Reduced Context Switching**: Multiple operations per thread
2. **Better Cache Locality**: Related operations stay together
3. **Efficient Resource Utilization**: Minimize thread overhead
4. **Controlled Parallelism**: Balance throughput vs. ordering

### Memory Management

```rust
// Write batch shrinking
if data_size > APPLY_WB_SHRINK_SIZE {
    let kv_wb = self.engine.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE);
    let kv_wb = self.host.on_create_apply_write_batch(kv_wb);
    self.kv_wb = kv_wb;
} else {
    self.kv_wb_mut().clear();
}
```

### Performance Metrics

```rust
// Key performance metrics
STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
STORE_APPLY_LOG_HISTOGRAM.observe(duration_to_sec(elapsed));
FSM_POLL_DURATION.get(N::FSM_TYPE).observe(timer.saturating_elapsed_secs());
```

## Error Handling

### Error Propagation

1. **Client Level**: Request validation errors
2. **Store Level**: Routing and region errors
3. **Peer Level**: Raft consensus errors
4. **Apply Level**: State machine application errors
5. **Write Level**: Persistence errors

### Error Recovery

```rust
// Error handling in write operations
kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
    panic!("store {}: {} failed to write to kv engine: {:?}", 
           self.store_id, self.tag, e);
});

// Graceful error handling in FSM
if let Err(e) = self.on_raft_message(msg) {
    if matches!(&e, Error::RegionNotRegistered { .. }) {
        info!("handle raft message failed"; "err" => ?e);
    } else {
        error!(?e; "handle raft message failed");
    }
}
```

## Code References

### Key Files and Functions

| **Component** | **File** | **Key Functions** |
|---------------|----------|-------------------|
| **Client Interface** | `src/server/raftkv/mod.rs` | `async_write()` |
| **Store FSM** | `components/raftstore/src/store/fsm/store.rs` | `handle_msgs()`, `on_raft_message()` |
| **Peer FSM** | `components/raftstore/src/store/fsm/peer.rs` | `handle_msgs()`, `propose_raft_command()` |
| **Raft Proposal** | `components/raftstore/src/store/peer.rs` | `propose_normal()` |
| **Raft Ready** | `components/raftstore/src/store/peer_storage.rs` | `handle_raft_ready()` |
| **Async Write** | `components/raftstore/src/store/async_io/write.rs` | `run()`, `write_to_db()` |
| **Apply FSM** | `components/raftstore/src/store/fsm/apply.rs` | `process_raft_cmd()`, `write_to_db()` |
| **Batch System** | `components/batch-system/src/batch.rs` | `poll()`, `handle_normal()` |

### Configuration Parameters

```rust
// Key configuration parameters
pub struct Config {
    pub max_batch_size: usize,           // Maximum FSMs per batch
    pub messages_per_tick: usize,        // Messages per region per tick
    pub raft_write_size_limit: usize,    // Raft write batch size limit
    pub raft_write_batch_size_hint: usize, // Raft write batch hint
    pub raft_write_wait_duration: Duration, // Wait duration for batching
}
```

## Summary

### Key Insights

1. **Multi-Stage Processing**: Write requests flow through multiple specialized thread pools and FSMs
2. **Sequential per Region**: Each region's writes are processed sequentially to maintain Raft ordering
3. **Parallel across Regions**: Different regions can be processed concurrently for high throughput
4. **Batch Optimization**: Multiple operations are batched together for efficiency
5. **Two-Phase Commit**: Raft log is written first, then state machine is applied
6. **Dedicated Workers**: Specialized threads handle different aspects of the write process

### Performance Characteristics

- **High Throughput**: Parallel processing across regions
- **Strong Consistency**: Sequential processing within regions
- **Low Latency**: Batch processing reduces overhead
- **Scalability**: Multiple regions can be processed concurrently
- **Durability**: Two-phase write ensures data persistence

### Design Trade-offs

**Advantages**:
- Maintains Raft consensus guarantees
- High performance through parallelization
- Efficient resource utilization
- Scalable architecture

**Challenges**:
- Complex thread coordination
- Potential for head-of-line blocking
- Configuration complexity
- Debugging difficulty

This architecture demonstrates how TiKV successfully balances the competing demands of high performance and strong consistency in a distributed storage system, making it suitable for production workloads that require both high throughput and reliable data consistency.

## Detailed Example: Write Request Flow in TiKV

### Scenario Setup
Let's say we have a TiKV cluster with 3 regions and 2 write requests:

**Write Request 1**: `PUT key="user:123", value="John Doe"` → Region A (region_id=1)
**Write Request 2**: `PUT key="user:456", value="Jane Smith"` → Region B (region_id=2)

### Step-by-Step Execution

## Stage 1: Client Request Entry
**Thread**: Main Server Thread
**Time**: T0

```rust
// Client sends write request
async_write(
    ctx: Context { region_id: 1 },
    batch: WriteData {
        modifies: vec![Modify::Put("user:123", "John Doe")],
        extra: WriteExtra { one_pc: false }
    }
)

// Converted to RaftCmdRequest
RaftCmdRequest {
    header: RequestHeader {
        region_id: 1,
        region_epoch: RegionEpoch { version: 1, conf_ver: 1 },
        term: 5,
        // ...
    },
    requests: vec![Request {
        cmd_type: CmdType::Put,
        put: PutRequest {
            key: b"user:123",
            value: b"John Doe",
            cf: "default"
        }
    }]
}
```

## Stage 2: Store FSM Processing
**Thread**: Store FSM Poller Thread
**Time**: T1

```rust
// Store FSM receives message
StoreMsg::RaftCommand(RaftCommand {
    request: RaftCmdRequest { /* ... */ },
    callback: Callback { /* ... */ },
    send_time: T0
})

// Store FSM routes to Region A
fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg>) {
    for msg in msgs.drain(..) {
        match msg {
            StoreMsg::RaftCommand(cmd) => {
                // Route to Region A (region_id=1)
                self.ctx.router.send(1, PeerMsg::RaftCommand(cmd));
            }
        }
    }
}
```

## Stage 3: Peer FSM Processing (Region A)
**Thread**: Peer FSM Poller Thread
**Time**: T2

```rust
// Peer FSM for Region A receives command
PeerMsg::RaftCommand(RaftCommand {
    request: RaftCmdRequest { /* ... */ },
    callback: Callback { /* ... */ },
    send_time: T0
})

// Collect multiple messages for batching
fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg>) {
    while self.peer_msg_buf.len() < self.messages_per_tick { // e.g., 8
        match peer.receiver.try_recv() {
            Ok(msg) => {
                self.peer_msg_buf.push(msg);
                // peer_msg_buf now contains: [RaftCommand for user:123]
            }
            Err(TryRecvError::Empty) => break,
        }
    }
    
    // Process the batch
    for msg in self.peer_msg_buf.drain(..) {
        match msg {
            PeerMsg::RaftCommand(cmd) => {
                self.propose_raft_command(cmd.request, cmd.callback, diskfullopt);
            }
        }
    }
}
```

## Stage 4: Raft Proposal (Region A)
**Thread**: Peer FSM Poller Thread
**Time**: T3

```rust
// Propose to Raft consensus
fn propose_raft_command_internal(&mut self, msg: RaftCmdRequest, cb: Callback) {
    // Serialize the request
    let data = msg.write_to_bytes()?; // "user:123=John Doe"
    
    // Propose to Raft
    let propose_index = self.next_proposal_index(); // e.g., 1001
    self.raft_group.propose(ctx.to_vec(), data)?;
    
    // Store callback for later
    self.pending_cmds.append_normal(PendingCmd {
        index: 1001,
        term: 5,
        cb: cb,
        request: msg,
        // ...
    });
    
    // Raft log entry created:
    // Entry { index: 1001, term: 5, data: "user:123=John Doe" }
}
```

## Stage 5: Raft Ready Processing (Region A)
**Thread**: Peer FSM Poller Thread
**Time**: T4

```rust
// Raft ready contains committed entries
Ready {
    entries: vec![Entry {
        index: 1001,
        term: 5,
        data: "user:123=John Doe"
    }],
    committed_entries: vec![Entry {
        index: 1001,
        term: 5,
        data: "user:123=John Doe"
    }],
    // ...
}

// Create write task
fn handle_raft_ready(&mut self, ready: &mut Ready) -> WriteTask {
    let mut write_task = WriteTask::new(1, 1, ready.number());
    
    // Append committed entries
    if !ready.entries().is_empty() {
        self.append(ready.take_entries(), &mut write_task);
    }
    
    // write_task now contains:
    // - region_id: 1
    // - entries: [Entry { index: 1001, term: 5, data: "user:123=John Doe" }]
    // - raft_state: RaftLocalState { last_index: 1001, ... }
    
    write_task
}
```

## Stage 6: Async Write Worker Processing
**Thread**: Async Write Worker Thread
**Time**: T5

```rust
// Async write worker receives write task
WriteMsg::WriteTask(WriteTask {
    region_id: 1,
    entries: vec![Entry { index: 1001, term: 5, data: "user:123=John Doe" }],
    raft_state: RaftLocalState { last_index: 1001, ... },
    // ...
})

// Batch multiple write tasks
fn run(&mut self) {
    while self.batch.get_raft_size() < self.raft_write_size_limit { // e.g., 1MB
        match self.receiver.try_recv() {
            Ok(WriteMsg::WriteTask(task)) => {
                self.batch.add_write_task(&self.raft_engine, task);
                // batch now contains write tasks from multiple regions
            }
        }
    }
    
    // Write to databases
    self.write_to_db(true);
}

// Write to RocksDB and RaftDB
fn write_to_db(&mut self) {
    // Write KV data to RocksDB
    if !self.batch.extra_batch_write.is_empty() {
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        self.batch.extra_batch_write.write_opt(&write_opts)?;
    }
    
    // Write Raft log to RaftDB
    for raft_wb in &mut self.batch.raft_wbs {
        self.raft_engine.consume_and_shrink(raft_wb, true, ...)?;
    }
    
    // Persisted to disk:
    // RaftDB: Entry { index: 1001, term: 5, data: "user:123=John Doe" }
    // RocksDB: (will be written later by Apply FSM)
}
```

## Stage 7: Apply FSM Processing (Region A)
**Thread**: Apply FSM Poller Thread
**Time**: T6

```rust
// Apply FSM receives committed entry
ApplyTask::CommittedEntries {
    region_id: 1,
    entries: vec![Entry {
        index: 1001,
        term: 5,
        data: "user:123=John Doe"
    }]
}

// Process the committed entry
fn process_raft_cmd(&mut self, index: 1001, term: 5, req: RaftCmdRequest) {
    // Deserialize the request
    let put_req = req.get_requests()[0].get_put();
    let key = put_req.get_key(); // "user:123"
    let value = put_req.get_value(); // "John Doe"
    
    // Execute the actual write operation
    let (mut cmd, exec_result, should_write) = self.apply_raft_cmd(apply_ctx, 1001, 5, req);
    
    if should_write {
        // Add to write batch
        self.kv_wb.put(key, value)?;
        // kv_wb now contains: "user:123" -> "John Doe"
        
        // Update apply state
        self.write_apply_state(apply_ctx.kv_wb_mut());
        apply_ctx.commit(self);
    }
}
```

## Stage 8: Final RocksDB Persistence (Region A)
**Thread**: Apply FSM Poller Thread
**Time**: T7

```rust
// Write to RocksDB
fn write_to_db(&mut self) -> (bool, Option<SequenceNumber>) {
    if !self.kv_wb_mut().is_empty() {
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        
        // Write to RocksDB
        let seq = self.kv_wb_mut().write_opt(&write_opts)?;
        
        // Persisted to disk:
        // RocksDB: "user:123" -> "John Doe" (sequence number: 12345)
    }
    
    // Invoke callback to notify client
    if let Some(cmd_cb) = self.find_pending(1001, 5, false) {
        cmd_cb.invoke_with_response(RaftCmdResponse {
            header: ResponseHeader { /* ... */ },
            responses: vec![Response {
                cmd_type: CmdType::Put,
                put: PutResponse { /* success */ }
            }]
        });
    }
}
```

## Parallel Processing Example

Now let's see how Region B processes its write in parallel:

### Region B Processing (Parallel to Region A)
**Thread**: Peer FSM Poller Thread (same thread, different batch)
**Time**: T2-T7 (overlapping with Region A)

```rust
// Region B receives its write request
PeerMsg::RaftCommand(RaftCommand {
    request: RaftCmdRequest {
        header: RequestHeader { region_id: 2, /* ... */ },
        requests: vec![Request {
            cmd_type: CmdType::Put,
            put: PutRequest {
                key: b"user:456",
                value: b"Jane Smith",
                cf: "default"
            }
        }]
    },
    callback: Callback { /* ... */ }
})

// Similar flow as Region A:
// 1. Propose to Raft (index: 1002)
// 2. Raft consensus
// 3. Raft ready processing
// 4. Async write worker
// 5. Apply FSM processing
// 6. Final RocksDB persistence
```

## Batch System Polling Example

```rust
// Batch system poller processes multiple regions
fn poll(&mut self) {
    // Round 1: Process regions A and B
    let mut batch = Batch::with_capacity(8); // max_batch_size = 8
    
    // Fetch FSMs for regions A and B
    batch.normals = vec![
        Some(PeerFsm { region_id: 1, /* ... */ }), // Region A
        Some(PeerFsm { region_id: 2, /* ... */ }), // Region B
    ];
    
    // Process each region sequentially within the batch
    for (i, peer_fsm) in batch.normals.iter_mut().enumerate() {
        let peer_fsm = peer_fsm.as_mut().unwrap();
        
        // Region A processing
        if peer_fsm.region_id() == 1 {
            // Process messages for Region A sequentially
            while peer_fsm.receiver.len() > 0 {
                let msg = peer_fsm.receiver.try_recv().unwrap();
                // Process: RaftCommand for "user:123"
            }
        }
        
        // Region B processing  
        if peer_fsm.region_id() == 2 {
            // Process messages for Region B sequentially
            while peer_fsm.receiver.len() > 0 {
                let msg = peer_fsm.receiver.try_recv().unwrap();
                // Process: RaftCommand for "user:456"
            }
        }
    }
}
```

## Timeline Summary

```
T0: Client sends write requests
    ├── Request 1: "user:123" -> Region A
    └── Request 2: "user:456" -> Region B

T1: Store FSM routes messages
    ├── Routes to Region A
    └── Routes to Region B

T2: Peer FSM processes (parallel)
    ├── Region A: Propose "user:123" (index: 1001)
    └── Region B: Propose "user:456" (index: 1002)

T3: Raft consensus (parallel)
    ├── Region A: Entry 1001 committed
    └── Region B: Entry 1002 committed

T4: Raft ready processing (parallel)
    ├── Region A: Create write task for entry 1001
    └── Region B: Create write task for entry 1002

T5: Async write workers (parallel)
    ├── Write Region A's raft log to RaftDB
    └── Write Region B's raft log to RaftDB

T6: Apply FSM processing (parallel)
    ├── Region A: Apply entry 1001, add to KV write batch
    └── Region B: Apply entry 1002, add to KV write batch

T7: Final RocksDB persistence (parallel)
    ├── Region A: Write "user:123" -> "John Doe" to RocksDB
    └── Region B: Write "user:456" -> "Jane Smith" to RocksDB

T8: Client callbacks invoked
    ├── Notify client 1: Write successful
    └── Notify client 2: Write successful
```

## Key Insights from This Example

1. **Sequential per Region**: Within Region A, the "user:123" write is processed sequentially
2. **Parallel across Regions**: Region A and Region B are processed in parallel
3. **Batching**: Multiple messages per region are collected and processed together
4. **Two-Phase Write**: Raft log first (T5), then state machine application (T6-T7)
5. **Thread Coordination**: Different thread pools handle different stages
6. **Consistency**: Raft ensures both regions maintain consistency within themselves

This example shows how TiKV achieves both high performance (parallel processing) and strong consistency (sequential processing within regions) through its sophisticated thread scheduling and FSM architecture!

## TiKV Queue Architecture: Multiple Queues, Not Single Queue

### Overview

**Answer: TiKV uses MULTIPLE queues, not a single queue.** Each region has its own dedicated message queue, and there are also separate queues for different message types and FSMs. This sophisticated queue architecture is fundamental to TiKV's ability to achieve both high performance and strong consistency.

### Queue Architecture Breakdown

#### 1. Per-Region Queues (Peer FSM)

Each region has its own dedicated message queue:

```rust
// From components/batch-system/src/router.rs:49
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<DashMap<u64, BasicMailbox<N>>>,  // region_id -> mailbox
    control_box: BasicMailbox<C>,
    // ...
}

// From components/batch-system/src/mailbox.rs:31-34
pub struct BasicMailbox<Owner: Fsm> {
    sender: mpsc::LooseBoundedSender<Owner::Message>,  // Per-region channel
    state: Arc<FsmState<Owner>>,
}
```

**Key Points:**
- **One queue per region**: Each region (region_id) has its own `BasicMailbox`
- **Dedicated channels**: Each region has its own `LooseBoundedSender` channel
- **Isolated processing**: Messages for different regions don't interfere with each other

#### 2. Message Type Distribution

**Peer FSM Messages** (per region):
```rust
// From components/raftstore/src/store/msg.rs:837-870
pub enum PeerMsg<EK: KvEngine> {
    RaftMessage(Box<InspectedRaftMessage>, Option<Instant>),  // Raft consensus
    RaftCommand(Box<RaftCommand<EK::Snapshot>>),             // Write commands
    Tick(PeerTick),                                          // Periodic tasks
    ApplyRes(Box<ApplyTaskRes<EK::Snapshot>>),               // Apply results
    SignificantMsg(Box<SignificantMsg<EK::Snapshot>>),       // Critical messages
    Start,                                                   // FSM startup
    Noop,                                                    // Notifications
    Persisted { peer_id: u64, ready_number: u64 },          // Persistence confirmations
    CasualMessage(Box<CasualMessage<EK>>),                  // Low-priority messages
    HeartbeatPd,                                             // PD heartbeats
    UpdateReplicationMode,                                   // Replication changes
    Destroy(u64),                                            // Region destruction
}
```

**Store FSM Messages** (global):
```rust
// From components/raftstore/src/store/msg.rs:936-987
pub enum StoreMsg<EK> {
    RaftMessage(Box<InspectedRaftMessage>),                  // Incoming Raft messages
    StoreUnreachable { store_id: u64 },                     // Store connectivity
    CompactedEvent(EK::CompactedEvent),                     // Compaction events
    ClearRegionSizeInRange { start_key: Vec<u8>, end_key: Vec<u8> },
    Tick(StoreTick),                                         // Store-level ticks
    Start { store: metapb::Store },                         // Store startup
    UpdateReplicationMode(ReplicationStatus),               // Replication updates
    LatencyInspect { factor: InspectFactor, send_time: Instant, inspector: LatencyInspector },
    UnsafeRecoveryReport(pdpb::StoreReport),                // Recovery reports
    UnsafeRecoveryCreatePeer { syncer: UnsafeRecoveryExecutePlanSyncer, create: metapb::Region },
    GcSnapshotFinish,                                        // Snapshot cleanup
    AwakenRegions { abnormal_stores: Vec<u64>, region_ids: Vec<u64> },
}
```

#### 3. Queue Processing Architecture

**Batch System Polling:**
```rust
// From components/batch-system/src/batch.rs:414-443
for (i, p) in batch.normals.iter_mut().enumerate() {
    let p = p.as_mut().unwrap();
    let res = self.handler.handle_normal(p);  // Process each region's queue
    // ...
}
```

**Per-Region Message Collection:**
```rust
// From components/raftstore/src/store/fsm/store.rs:1096-1118
while self.peer_msg_buf.len() < self.messages_per_tick {
    match peer.receiver.try_recv() {  // Each region has its own receiver
        Ok(msg) => {
            self.peer_msg_buf.push(msg);
        }
        Err(TryRecvError::Empty) => break,
        Err(TryRecvError::Disconnected) => {
            peer.stop();
            break;
        }
    }
}
```

### Queue Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    TiKV Queue Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  Store FSM Queue (Global)                                  │
│  ├── StoreMsg::RaftMessage                                 │
│  ├── StoreMsg::Tick                                        │
│  ├── StoreMsg::StoreUnreachable                            │
│  └── StoreMsg::CompactedEvent                              │
├─────────────────────────────────────────────────────────────┤
│  Per-Region Queues (Peer FSM)                              │
│  ├── Region 1: PeerMsg Queue                              │
│  │   ├── RaftCommand (write requests)                     │
│  │   ├── RaftMessage (consensus)                          │
│  │   ├── Tick (periodic tasks)                            │
│  │   └── ApplyRes (apply results)                         │
│  ├── Region 2: PeerMsg Queue                              │
│  │   ├── RaftCommand (write requests)                     │
│  │   ├── RaftMessage (consensus)                          │
│  │   ├── Tick (periodic tasks)                            │
│  │   └── ApplyRes (apply results)                         │
│  └── Region N: PeerMsg Queue                              │
│      ├── RaftCommand (write requests)                     │
│      ├── RaftMessage (consensus)                          │
│      ├── Tick (periodic tasks)                            │
│      └── ApplyRes (apply results)                         │
├─────────────────────────────────────────────────────────────┤
│  Apply FSM Queues (Per Region)                             │
│  ├── Region 1: ApplyMsg Queue                             │
│  ├── Region 2: ApplyMsg Queue                             │
│  └── Region N: ApplyMsg Queue                             │
├─────────────────────────────────────────────────────────────┤
│  Async Write Worker Queues (SHARED)                        │
│  ├── WriteMsg Queue 1 (shared by all regions)             │
│  ├── WriteMsg Queue 2 (shared by all regions)             │
│  ├── WriteMsg Queue N (shared by all regions)             │
│  └── WriteTask Queue (individual tasks)                   │
└─────────────────────────────────────────────────────────────┘
```

### Key Benefits of Multiple Queues

#### 1. Isolation
- **Per-region isolation**: Messages for different regions don't block each other
- **Message type separation**: Different message types have different priorities
- **Independent processing**: Each region can be processed independently

#### 2. Performance
- **Parallel processing**: Multiple regions can be processed simultaneously
- **Reduced contention**: No single queue bottleneck
- **Better cache locality**: Related messages stay together

#### 3. Scalability
- **Horizontal scaling**: More regions = more parallel processing
- **Load distribution**: Hot regions don't affect cold regions
- **Resource isolation**: Each region has its own resource limits

### Message Routing Flow

```rust
// 1. Client sends write request
RaftCmdRequest -> Store FSM Queue

// 2. Store FSM routes to specific region
Store FSM -> Region-specific Peer FSM Queue

// 3. Peer FSM processes region messages
Peer FSM Queue -> Raft consensus + Apply FSM Queue

// 4. Apply FSM processes committed entries
Apply FSM Queue -> Async Write Worker Queue

// 5. Async Write Worker persists to RocksDB
Write Worker Queue -> RocksDB
```

### Queue Implementation Details

#### BasicMailbox Structure

```rust
// From components/batch-system/src/mailbox.rs:31-34
pub struct BasicMailbox<Owner: Fsm> {
    sender: mpsc::LooseBoundedSender<Owner::Message>,  // Per-region channel
    state: Arc<FsmState<Owner>>,
}

impl<Owner: Fsm> BasicMailbox<Owner> {
    pub fn new(
        sender: mpsc::LooseBoundedSender<Owner::Message>,
        fsm: Box<Owner>,
        state_cnt: Arc<AtomicUsize>,
    ) -> BasicMailbox<Owner> {
        BasicMailbox {
            sender,
            state: Arc::new(FsmState::new(fsm, state_cnt)),
        }
    }
}
```

#### Router Implementation

```rust
// From components/batch-system/src/router.rs:48-63
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<DashMap<u64, BasicMailbox<N>>>,  // region_id -> mailbox
    pub(super) control_box: BasicMailbox<C>,
    pub(crate) normal_scheduler: Ns,
    pub(crate) control_scheduler: Cs,
    state_cnt: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}
```

#### Message Sending

```rust
// From components/batch-system/src/mailbox.rs:88-97
pub fn try_send<S: FsmScheduler<Fsm = Owner>>(
    &self,
    msg: Owner::Message,
    scheduler: &S,
) -> Result<(), TrySendError<Owner::Message>> {
    scheduler.consume_msg_resource(&msg);
    self.sender.try_send(msg)?;
    self.state.notify(scheduler, Cow::Borrowed(self));
    Ok(())
}
```

### Queue Processing Patterns

#### 1. Batch Processing

```rust
// From components/batch-system/src/batch.rs:382-487
pub fn poll(&mut self) {
    let mut batch = Batch::with_capacity(self.max_batch_size);
    let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);
    
    while run && self.fetch_fsm(&mut batch) {
        // Process control FSM first
        if batch.control.is_some() {
            let len = self.handler.handle_control(batch.control.as_mut().unwrap());
        }
        
        // Process normal FSMs (regions) in batch
        for (i, p) in batch.normals.iter_mut().enumerate() {
            let p = p.as_mut().unwrap();
            let res = self.handler.handle_normal(p);
        }
    }
}
```

#### 2. Per-Region Message Collection

```rust
// From components/raftstore/src/store/fsm/store.rs:1096-1118
while self.peer_msg_buf.len() < self.messages_per_tick {
    match peer.receiver.try_recv() {
        Ok(msg) => {
            self.peer_msg_buf.push(msg);
        }
        Err(TryRecvError::Empty) => {
            handle_result = HandleResult::stop_at(0, false);
            break;
        }
        Err(TryRecvError::Disconnected) => {
            peer.stop();
            handle_result = HandleResult::stop_at(0, false);
            break;
        }
    }
}
```

### Performance Characteristics

#### 1. Queue Capacity Management

```rust
// LooseBoundedSender provides backpressure
pub struct LooseBoundedSender<T> {
    sender: Sender<T>,
    // Capacity management
}

impl<T> LooseBoundedSender<T> {
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        // Implement capacity checking and backpressure
    }
}
```

#### 2. Message Prioritization

```rust
// Different message types have different priorities
pub enum PeerMsg<EK: KvEngine> {
    RaftCommand(Box<RaftCommand<EK::Snapshot>>),     // High priority
    SignificantMsg(Box<SignificantMsg<EK::Snapshot>>), // High priority
    Tick(PeerTick),                                  // Medium priority
    CasualMessage(Box<CasualMessage<EK>>),          // Low priority
    // ...
}
```

#### 3. Resource Metering

```rust
// From components/raftstore/src/store/msg.rs:872
impl<EK: KvEngine> ResourceMetered for PeerMsg<EK> {}

// Messages are tracked for memory usage
MEMTRACE_RAFT_MESSAGES.trace(TraceEvent::Add(heap_size));
```

### Fault Tolerance and Error Handling

#### 1. Queue Disconnection Handling

```rust
// From components/raftstore/src/store/fsm/store.rs:1112-1117
Err(TryRecvError::Disconnected) => {
    peer.stop();
    handle_result = HandleResult::stop_at(0, false);
    break;
}
```

#### 2. Message Loss Prevention

```rust
// Critical messages use force_send
pub fn force_send<S: FsmScheduler<Fsm = Owner>>(
    &self,
    msg: Owner::Message,
    scheduler: &S,
) -> Result<(), SendError<Owner::Message>> {
    scheduler.consume_msg_resource(&msg);
    self.sender.force_send(msg)?;
    self.state.notify(scheduler, Cow::Borrowed(self));
    Ok(())
}
```

### Configuration Parameters

#### Queue Sizing

```rust
// Key configuration parameters
pub struct Config {
    pub max_batch_size: usize,           // Maximum regions per batch
    pub messages_per_tick: usize,        // Maximum messages per region per tick
    pub raft_write_size_limit: usize,    // Maximum size for raft write batches
    pub raft_write_batch_size_hint: usize, // Raft write batch hint
    pub raft_write_wait_duration: Duration, // Wait duration for batching
}
```

#### Channel Capacity

```rust
// LooseBoundedSender capacity
const DEFAULT_CHANNEL_CAPACITY: usize = 1000;
const HIGH_PRIORITY_CAPACITY: usize = 10000;
const LOW_PRIORITY_CAPACITY: usize = 100;
```

### Monitoring and Metrics

#### Queue Metrics

```rust
// Queue length monitoring
pub fn len(&self) -> usize {
    self.sender.len()
}

pub fn is_empty(&self) -> bool {
    self.sender.is_empty()
}

// Performance metrics
FSM_POLL_DURATION.get(N::FSM_TYPE).observe(timer.saturating_elapsed_secs());
FSM_COUNT_PER_POLL.get(N::FSM_TYPE).observe(self.normals.len() as f64);
```

### Summary

**TiKV uses a sophisticated multi-queue architecture:**

1. **Store FSM**: One global queue for store-level messages
2. **Peer FSM**: One queue per region for region-specific messages
3. **Apply FSM**: One queue per region for apply operations
4. **Async Write Workers**: Separate queues for write operations

This design ensures:
- **High performance** through parallel processing
- **Strong consistency** through per-region sequential processing
- **Scalability** through queue isolation
- **Fault tolerance** through independent region processing

The multiple queue architecture is a key reason why TiKV can handle thousands of regions efficiently while maintaining strong consistency guarantees!

## WriteMsg Queue Architecture: Shared, Not Per-Region

### WriteMsg Queue Structure

**Important Correction: WriteMsg queues are SHARED across all regions, not per-region.**

```rust
// From components/raftstore/src/store/async_io/write_router.rs:271
pub struct SharedSenders<EK: KvEngine, ER: RaftEngine>(Vec<Sender<WriteMsg<EK, ER>>>);

// From components/raftstore/src/store/async_io/write_router.rs:309-313
pub struct WriteSenders<EK: KvEngine, ER: RaftEngine> {
    senders: Tracker<SharedSenders<EK, ER>>,
    cached_senders: Vec<Sender<WriteMsg<EK, ER>>>,  // Multiple shared queues
    io_reschedule_concurrent_count: Arc<AtomicUsize>,
}
```

### WriteMsg Queue Details

#### 1. **Shared Write Workers**
```rust
// From components/raftstore/src/store/async_io/write.rs:1096-1098
pub fn senders(&self) -> WriteSenders<EK, ER> {
    WriteSenders::new(self.writers.clone())
}

// From components/raftstore/src/store/async_io/write.rs:1109-1122
let pool_size = cfg.value().store_io_pool_size;
if pool_size > 0 {
    self.increase_to(
        pool_size,  // Multiple write workers share the load
        StoreWritersContext { /* ... */ },
    )?;
}
```

#### 2. **Per-Peer WriteRouter**
```rust
// From components/raftstore/src/store/async_io/write_router.rs:61-79
pub struct WriteRouter<EK, ER> {
    tag: String,
    writer_id: usize,  // Which shared queue to use
    next_retry_time: Instant,
    next_writer_id: Option<usize>,
    last_unpersisted: Option<u64>,
    pending_write_msgs: Vec<WriteMsg<EK, ER>>,  // Local buffering
    last_msg_priority: Option<u64>,
}
```

#### 3. **Message Routing to Shared Queues**
```rust
// From components/raftstore/src/store/async_io/write_router.rs:239-262
fn send<C: WriteRouterContext<EK, ER>>(&mut self, ctx: &mut C, msg: WriteMsg<EK, ER>) {
    let sender = &ctx.write_senders()[self.writer_id];  // Select shared queue
    sender.consume_msg_resource(&msg);
    match sender.try_send(msg, self.last_msg_priority) {
        Ok(priority) => self.last_msg_priority = priority,
        Err(TrySendError::Full(msg)) => {
            // Blocking send to shared queue
            sender.send(msg, self.last_msg_priority).unwrap();
        }
        Err(TrySendError::Disconnected(_)) => {
            safe_panic!("failed to send write msg, err: disconnected");
        }
    }
}
```

### WriteMsg Queue Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                WriteMsg Queue Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  Per-Region WriteRouter (Load Balancing)                    │
│  ├── Region 1: WriteRouter -> Queue 0                      │
│  ├── Region 2: WriteRouter -> Queue 1                      │
│  ├── Region 3: WriteRouter -> Queue 0                      │
│  ├── Region 4: WriteRouter -> Queue 2                      │
│  └── Region N: WriteRouter -> Queue (N % pool_size)        │
├─────────────────────────────────────────────────────────────┤
│  Shared WriteMsg Queues (All Regions)                       │
│  ├── WriteMsg Queue 0: [Region1, Region3, Region5, ...]   │
│  ├── WriteMsg Queue 1: [Region2, Region6, Region7, ...]   │
│  ├── WriteMsg Queue 2: [Region4, Region8, Region9, ...]   │
│  └── WriteMsg Queue N: [RegionX, RegionY, RegionZ, ...]   │
├─────────────────────────────────────────────────────────────┤
│  Write Workers (Per Queue)                                  │
│  ├── Write Worker 0: Processes Queue 0                     │
│  ├── Write Worker 1: Processes Queue 1                     │
│  ├── Write Worker 2: Processes Queue 2                     │
│  └── Write Worker N: Processes Queue N                     │
└─────────────────────────────────────────────────────────────┘
```

### Benefits of Shared WriteMsg Queues

#### 1. **Load Balancing**
- Multiple regions share the same write workers
- Better resource utilization
- Reduced thread overhead

#### 2. **Batching Efficiency**
- Multiple regions' writes can be batched together
- Better RocksDB write performance
- Reduced I/O overhead

#### 3. **Resource Management**
- Configurable pool size (`store_io_pool_size`)
- Better control over write concurrency
- Prevents write worker explosion

## RocksDB Storage Architecture: Shared Engine, Key-Based Separation

### Storage Engine Structure

**All regions share the same RocksDB instance, but data is separated by key encoding and column families.**

### Key Encoding Strategy

#### 1. **Data Key Encoding**
```rust
// From components/keys/src/lib.rs:28-31
pub const DATA_PREFIX: u8 = b'z';
pub const DATA_PREFIX_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MIN_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MAX_KEY: &[u8] = &[DATA_PREFIX + 1];

// From components/keys/src/lib.rs:101-108
pub fn data_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(DATA_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}
```

#### 2. **Region Boundary Encoding**
```rust
// From components/keys/src/lib.rs:109-116
pub fn enc_start_key(region: &Region) -> Vec<u8> {
    if region.get_start_key().is_empty() {
        DATA_MIN_KEY.to_vec()
    } else {
        data_key(region.get_start_key())
    }
}

pub fn enc_end_key(region: &Region) -> Vec<u8> {
    if region.get_end_key().is_empty() {
        DATA_MAX_KEY.to_vec()
    } else {
        data_key(region.get_end_key())
    }
}
```

### Column Family Organization

#### 1. **Three Main Column Families**
```rust
// From components/raftstore/src/store/snap.rs:56
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

// From engine_traits/src/lib.rs
pub const CF_DEFAULT: &str = "default";
pub const CF_LOCK: &str = "lock";
pub const CF_WRITE: &str = "write";
```

#### 2. **Column Family Usage**
```rust
// From components/raftstore/src/store/fsm/apply.rs:1879-1907
if !req.get_put().get_cf().is_empty() {
    let cf = req.get_put().get_cf();
    if cf == CF_LOCK {
        self.metrics.lock_cf_written_bytes += key.len() as u64;
        self.metrics.lock_cf_written_bytes += value.len() as u64;
    }
    ctx.kv_wb.put_cf(cf, key, value).unwrap();
} else {
    ctx.kv_wb.put(key, value).unwrap();  // Default CF
}
```

### RocksDB Storage Layout

```
┌─────────────────────────────────────────────────────────────┐
│                    Single RocksDB Instance                  │
├─────────────────────────────────────────────────────────────┤
│  Column Family: CF_DEFAULT                                  │
│  ├── z|user:123|... -> "John Doe"                          │
│  ├── z|user:456|... -> "Jane Smith"                        │
│  ├── z|order:789|... -> "Order Data"                       │
│  └── z|product:abc|... -> "Product Info"                   │
├─────────────────────────────────────────────────────────────┤
│  Column Family: CF_WRITE                                    │
│  ├── z|user:123|... -> Write Record                        │
│  ├── z|user:456|... -> Write Record                        │
│  ├── z|order:789|... -> Write Record                       │
│  └── z|product:abc|... -> Write Record                     │
├─────────────────────────────────────────────────────────────┤
│  Column Family: CF_LOCK                                     │
│  ├── z|user:123|... -> Lock Info                           │
│  ├── z|user:456|... -> Lock Info                           │
│  ├── z|order:789|... -> Lock Info                          │
│  └── z|product:abc|... -> Lock Info                        │
├─────────────────────────────────────────────────────────────┤
│  Column Family: CF_RAFT (Raft Metadata)                    │
│  ├── 0x01|0x02|region_id|0x01|log_index -> Raft Log       │
│  ├── 0x01|0x02|region_id|0x02 -> Raft State               │
│  ├── 0x01|0x02|region_id|0x03 -> Apply State              │
│  └── 0x01|0x03|region_id|0x01 -> Region State            │
└─────────────────────────────────────────────────────────────┘
```

### Region Data Separation

#### 1. **Key Range Separation**
```rust
// From components/raftstore/src/store/region_snapshot.rs:340-350
fn update_lower_bound(iter_opt: &mut IterOptions, region: &Region) {
    let region_start_key = keys::enc_start_key(region);
    if iter_opt.lower_bound().is_some() {
        iter_opt.set_lower_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_start_key.as_slice() > *iter_opt.lower_bound().as_ref().unwrap() {
            iter_opt.set_vec_lower_bound(region_start_key);
        }
    } else {
        iter_opt.set_vec_lower_bound(region_start_key);
    }
}
```

#### 2. **Region Iterator**
```rust
// From components/raftstore/src/store/region_snapshot.rs:325-331
pub struct RegionIterator<S: Snapshot> {
    iter: <S as Iterable>::Iterator,
    region: Arc<Region>,  // Enforces region boundaries
}

// From components/raftstore/src/store/region_snapshot.rs:369-381
pub fn new(
    snap: &S,
    region: Arc<Region>,
    mut iter_opt: IterOptions,
    cf: &str,
) -> RegionIterator<S> {
    update_lower_bound(&mut iter_opt, &region);
    update_upper_bound(&mut iter_opt, &region);
    let iter = snap.iterator_opt(cf, iter_opt).expect("creating snapshot iterator");
    RegionIterator { iter, region }
}
```

### Storage Benefits

#### 1. **Single Engine Efficiency**
- One RocksDB instance handles all regions
- Shared memory and cache
- Unified compaction and maintenance

#### 2. **Key-Based Isolation**
- Region boundaries enforced by key encoding
- No cross-region data contamination
- Efficient range queries per region

#### 3. **Column Family Separation**
- Different data types in separate CFs
- Independent tuning per CF
- Better compression and caching

#### 4. **Raft Metadata Separation**
- Raft logs stored separately from user data
- Different access patterns optimized
- Easier backup and recovery

### Multi-Tablet Architecture (Optional)

```rust
// From components/raftstore/src/store/worker/split_check.rs:612-624
let tablet = match &self.engine {
    Either::Left(e) => e,  // Single RocksDB
    Either::Right(r) => match r.get(region.get_id()) {  // Multi-tablet
        Some(c) => {
            cached = Some(c);
            match cached.as_mut().unwrap().latest() {
                Some(t) => t,
                None => return,
            }
        }
        None => return,
    },
};
```

**Note**: TiKV supports both single-RocksDB and multi-tablet architectures, but the default is single-RocksDB with key-based separation.

## Apply FSM Queue: The State Machine Application Layer

### Role of Apply FSM Queue

The **Apply FSM queue** is responsible for **applying committed Raft entries to the state machine** and **executing the actual KV operations**. It serves as the bridge between Raft consensus (log replication) and the actual data persistence.

### Apply FSM Queue Architecture

#### 1. **Per-Region Apply FSM Queues**
```rust
// From components/raftstore/src/store/fsm/apply.rs:3999-4006
pub struct ApplyFsm<EK: KvEngine> {
    delegate: ApplyDelegate<EK>,
    receiver: Receiver<Box<Msg<EK>>>,  // Per-region message queue
    mailbox: Option<BasicMailbox<ApplyFsm<EK>>>,
}
```

#### 2. **Apply Message Types**
```rust
// From components/raftstore/src/store/fsm/apply.rs:3812-3849
pub enum Msg<EK: KvEngine> {
    Apply {
        start: Instant,
        apply: Apply<Callback<EK::Snapshot>>,  // Committed entries to apply
    },
    Registration(Registration),                // FSM registration
    LogsUpToDate(CatchUpLogs),                // Merge operation
    Noop,                                     // No operation
    Destroy(Destroy),                         // Region destruction
    Snapshot(GenSnapTask),                    // Snapshot generation
    Change {                                  // Observer changes
        cmd: ChangeObserver,
        region_epoch: RegionEpoch,
        cb: Callback<EK::Snapshot>,
    },
    Recover(u64),                             // Recovery operations
    CheckCompact {                            // Compaction checks
        region_id: u64,
        voter_replicated_index: u64,
        voter_replicated_term: u64,
    },
    UnsafeForceCompact {                      // Force compaction
        region_id: u64,
        term: u64,
        compact_index: u64,
    },
    InMemoryEngineLoadRegion {                // In-memory engine loading
        region_id: u64,
        trigger_load_cb: Box<dyn FnOnce(&Region) + Send + 'static>,
    },
}
```

### Apply FSM Processing Flow

#### 1. **Message Collection and Processing**
```rust
// From components/raftstore/src/store/fsm/apply.rs:4769-4784
while self.msg_buf.len() < self.messages_per_tick {
    match normal.receiver.try_recv() {
        Ok(msg) => self.msg_buf.push(msg),
        Err(TryRecvError::Empty) => {
            handle_result = HandleResult::stop_at(0, false);
            break;
        }
        Err(TryRecvError::Disconnected) => {
            normal.delegate.stopped = true;
            handle_result = HandleResult::stop_at(0, false);
            break;
        }
    }
}

normal.handle_tasks(&mut self.apply_ctx, &mut self.msg_buf);
```

#### 2. **Apply Task Processing**
```rust
// From components/raftstore/src/store/fsm/apply.rs:4501-4533
match *msg {
    Msg::Apply { start, mut apply } => {
        let apply_wait = start.saturating_elapsed();
        apply_ctx.apply_wait.observe(apply_wait.as_secs_f64());
        
        if let Some(batch) = batch_apply.as_mut() {
            if batch.try_batch(&mut apply) {
                continue;  // Batch with previous apply
            } else {
                self.handle_apply(apply_ctx, batch_apply.take().unwrap());
            }
        }
        if !self.delegate.wait_data {
            batch_apply = Some(apply);
        }
    }
    // Handle other message types...
}
```

#### 3. **Committed Entries Processing**
```rust
// From components/raftstore/src/store/fsm/apply.rs:1141-1218
fn handle_raft_committed_entries(
    &mut self,
    apply_ctx: &mut ApplyContext<EK>,
    mut committed_entries_drainer: Drain<'_, Entry>,
) {
    if committed_entries_drainer.len() == 0 {
        return;
    }
    apply_ctx.prepare_for(self);
    apply_ctx.committed_count += committed_entries_drainer.len();
    
    while let Some(entry) = committed_entries_drainer.next() {
        let expect_index = self.apply_state.get_applied_index() + 1;
        if expect_index != entry.get_index() {
            panic!("expect index {}, but got {}", expect_index, entry.get_index());
        }
        
        let res = match entry.get_entry_type() {
            EntryType::EntryNormal => self.handle_raft_entry_normal(apply_ctx, &entry),
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                self.handle_raft_entry_conf_change(apply_ctx, &entry)
            }
        };
        
        match res {
            ApplyResult::None => {}
            ApplyResult::Res(res) => {
                results.push_back(res);
                if self.wait_data {
                    break;
                }
            }
            ApplyResult::Yield | ApplyResult::WaitMergeSource(_) => {
                // Yield processing for high-latency operations
                return;
            }
        }
    }
    apply_ctx.finish_for(self, results);
}
```

### Apply FSM Queue Processing Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                Apply FSM Queue Processing                    │
├─────────────────────────────────────────────────────────────┤
│  1. Message Collection                                       │
│     ├── ApplyMsg::Apply (committed entries)                 │
│     ├── ApplyMsg::Registration (FSM registration)           │
│     ├── ApplyMsg::Destroy (region destruction)              │
│     ├── ApplyMsg::Snapshot (snapshot generation)            │
│     └── ApplyMsg::Change (observer changes)                 │
├─────────────────────────────────────────────────────────────┤
│  2. Message Batching                                        │
│     ├── Batch multiple Apply messages                       │
│     ├── Yield for high-latency operations                  │
│     └── Resume pending messages                             │
├─────────────────────────────────────────────────────────────┤
│  3. Committed Entries Processing                            │
│     ├── Validate entry sequence                            │
│     ├── Process normal entries (KV operations)             │
│     ├── Process conf change entries                        │
│     └── Update apply state                                 │
├─────────────────────────────────────────────────────────────┤
│  4. KV Operations Execution                                 │
│     ├── PUT operations (data writes)                       │
│     ├── DELETE operations (data deletions)                 │
│     ├── DELETE_RANGE operations (range deletions)          │
│     └── INGEST_SST operations (SST ingestion)              │
├─────────────────────────────────────────────────────────────┤
│  5. WriteBatch Preparation                                 │
│     ├── Add operations to WriteBatch                       │
│     ├── Update apply state                                 │
│     ├── Prepare for persistence                            │
│     └── Commit to RocksDB                                  │
└─────────────────────────────────────────────────────────────┘
```

### Key Functions of Apply FSM Queue

#### 1. **State Machine Application**
- **Applies committed Raft entries** to the actual state machine
- **Executes KV operations** (PUT, DELETE, DELETE_RANGE, INGEST_SST)
- **Maintains apply state** (applied_index, commit_index, commit_term)

#### 2. **WriteBatch Management**
```rust
// From components/raftstore/src/store/fsm/apply.rs:542-572
pub fn prepare_for(&mut self, delegate: &mut ApplyDelegate<EK>) {
    self.applied_batch.push_batch(&delegate.observe_info, delegate.region.get_id());
    self.kv_wb.prepare_for_region(&delegate.region);
}

pub fn commit(&mut self, delegate: &mut ApplyDelegate<EK>) {
    if delegate.last_flush_applied_index < delegate.apply_state.get_applied_index() {
        delegate.maybe_write_apply_state(self);
    }
    self.commit_opt(delegate, true);
}

pub fn write_to_db(&mut self) -> (bool, Option<SequenceNumber>) {
    let need_sync = self.sync_log_hint && !self.disable_wal;
    // Write to RocksDB with proper sync options
}
```

#### 3. **Callback Management**
```rust
// From components/raftstore/src/store/fsm/apply.rs:1441-1452
let cmd_cb = self.find_pending(index, term, is_conf_change_cmd(&cmd.request));
apply_ctx
    .applied_batch
    .push(cmd_cb, cmd, &self.observe_info, self.region_id());
if should_write {
    self.write_apply_state(apply_ctx.kv_wb_mut());
    apply_ctx.commit(self);
}
```

#### 4. **Performance Optimization**
- **Batching**: Multiple apply messages can be batched together
- **Yielding**: High-latency operations yield to prevent blocking
- **Priority handling**: Different priority levels for different operations

### Apply FSM Queue Benefits

#### 1. **Asynchronous Processing**
- **Decouples Raft consensus** from state machine application
- **Allows parallel processing** of different regions
- **Prevents blocking** on slow I/O operations

#### 2. **Consistency Guarantees**
- **Sequential processing** within each region
- **Proper error handling** and recovery
- **State machine consistency** maintenance

#### 3. **Performance Optimization**
- **Batching** reduces I/O overhead
- **Yielding** prevents head-of-line blocking
- **Priority scheduling** for different operation types

#### 4. **Resource Management**
- **Memory management** for large operations
- **SST file handling** for snapshots and ingestion
- **Callback management** for client responses

### Apply FSM Queue in Write Flow

```
Write Request → Peer FSM → Raft Consensus → Apply FSM Queue → RocksDB
     ↓              ↓            ↓              ↓              ↓
  RaftCommand → Propose → Committed → Apply → Persist
     ↓              ↓            ↓              ↓              ↓
  Callback ← Response ← ApplyRes ← Execute ← WriteBatch
```

The Apply FSM queue is the **critical component** that ensures:
1. **Committed Raft entries are applied** to the state machine
2. **KV operations are executed** in the correct order
3. **Data is persisted** to RocksDB
4. **Client callbacks are invoked** with results
5. **Consistency is maintained** across all operations

---

*This document was generated by analyzing the TiKV codebase, specifically focusing on the write request flow through RaftStore thread pools and FSMs. For the most up-to-date information, please refer to the official TiKV documentation and source code.*
