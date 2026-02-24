//! Chaos test: random node kill/restart.
//!
//! 7-node cluster. Background writers and readers continuously operate.
//! Every few seconds, a random node is killed and then restarted.
//! After the chaos period, all objects must be readable.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use shoal_integration_tests::{IntegrationCluster, test_data_seeded};
use tokio::sync::RwLock;
use tokio::time;

/// 7-node cluster under continuous write/read pressure with random kills.
///
/// Test runs for ~15 seconds (shortened for CI). Every 3 seconds, a random
/// node is killed and restarted 1 second later.
///
/// After the chaos phase, all objects written must be verifiable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_random_kill_no_data_loss() {
    // --- Setup ---
    // replication=3 ensures each shard has 3 copies across 7 nodes.
    // k=4, m=2 means we need 4 out of 6 shards per chunk.
    // With replication=3, losing 1 node still leaves 2 copies per shard.
    let c = Arc::new(IntegrationCluster::with_replication(7, 2048, 4, 2, 3).await);

    let stop = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let written_keys: Arc<RwLock<Vec<(String, Vec<u8>)>>> = Arc::new(RwLock::new(Vec::new()));

    // --- Background writers (3 tasks) ---
    let mut handles = Vec::new();
    for writer_id in 0..3 {
        let cluster = c.clone();
        let stop = stop.clone();
        let wc = write_count.clone();
        let keys = written_keys.clone();
        handles.push(tokio::spawn(async move {
            let mut idx = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let seed = writer_id * 100_000 + idx;
                let data = test_data_seeded(1000 + (idx as usize % 5000), seed);
                let key = format!("chaos-w{writer_id}-{idx}");
                let node = (writer_id as usize + idx as usize) % 7;

                match cluster
                    .node(node)
                    .put_object("chaos", &key, &data, BTreeMap::new())
                    .await
                {
                    Ok(_) => {
                        cluster.broadcast_manifest(node, "chaos", &key).await;
                        keys.write().await.push((key, data));
                        wc.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        // Write may fail if the target node is down.
                        // That's fine — just skip and retry.
                        time::sleep(Duration::from_millis(50)).await;
                    }
                }
                idx += 1;
                // Small delay to avoid overwhelming the test.
                time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // --- Background readers (3 tasks) ---
    for reader_id in 0..3 {
        let cluster = c.clone();
        let stop = stop.clone();
        let rc = read_count.clone();
        let keys = written_keys.clone();
        handles.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let snapshot = keys.read().await.clone();
                if snapshot.is_empty() {
                    time::sleep(Duration::from_millis(50)).await;
                    continue;
                }

                // Pick a random-ish object to read.
                let idx = (reader_id * 37 + rc.load(Ordering::Relaxed)) % snapshot.len();
                let (key, _expected) = &snapshot[idx];
                let node = (reader_id + idx) % 7;

                // Read might fail if the target node is down or shards are
                // unavailable. We don't assert here — we verify after chaos.
                if let Ok(_) = cluster.node(node).get_object("chaos", key).await {
                    rc.fetch_add(1, Ordering::Relaxed);
                }

                time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // --- Chaos: kill/restart random nodes ---
    let chaos_duration = Duration::from_secs(15);
    let kill_interval = Duration::from_secs(3);
    let restart_delay = Duration::from_secs(1);
    let deadline = time::Instant::now() + chaos_duration;

    let mut kill_idx = 0usize;
    while time::Instant::now() < deadline {
        // Pick a "random" node (deterministic for reproducibility).
        let target = (kill_idx * 3 + 1) % 7;
        c.kill_node(target).await;
        time::sleep(restart_delay).await;
        c.revive_node(target).await;
        kill_idx += 1;
        time::sleep(kill_interval - restart_delay).await;
    }

    // --- Stop background tasks ---
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.await.unwrap();
    }

    let total_written = write_count.load(Ordering::Relaxed);
    let total_read = read_count.load(Ordering::Relaxed);

    // Sanity: should have written and read some objects.
    assert!(
        total_written >= 10,
        "should have written at least 10 objects, got {total_written}"
    );
    assert!(
        total_read >= 1,
        "should have completed at least 1 read, got {total_read}"
    );

    // --- Verification: all written objects must be readable ---
    // All nodes are alive now.
    let snapshot = written_keys.read().await.clone();
    let mut verified = 0;
    let mut failed = Vec::new();

    for (key, expected) in &snapshot {
        let mut ok = false;
        // Try multiple nodes to read.
        for reader in 0..7 {
            if let Ok((got, _)) = c.node(reader).get_object("chaos", key).await {
                assert_eq!(
                    &got, expected,
                    "data corruption for {key} on reader {reader}"
                );
                ok = true;
                verified += 1;
                break;
            }
        }
        if !ok {
            failed.push(key.clone());
        }
    }

    assert!(
        failed.is_empty(),
        "data loss: {} of {} objects unreadable after chaos: {:?}",
        failed.len(),
        snapshot.len(),
        &failed[..failed.len().min(10)]
    );

    assert_eq!(
        verified,
        snapshot.len(),
        "all {verified} written objects verified"
    );
}
