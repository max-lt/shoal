# Security Model

This document describes how Shoal handles trust between peers, the current
authentication model, and the planned evolution toward a CA-based system.

## Current State: Shared Secret

Today, cluster membership relies on a shared secret configured in `shoal.toml`:

```toml
[cluster]
secret = "my-cluster-secret"
```

The secret is used in two places:

1. **ALPN derivation**: `shoal/0/<blake3(secret)[:16]>` — nodes with different
   secrets get different ALPNs, so TLS handshake fails before any data exchange.
2. **Gossip topic**: `TopicId = blake3(secret)` — only nodes with the same
   secret can join the gossip topic.

### What works

- QUIC (TLS 1.3) authenticates each node's Ed25519 public key during handshake
- ALPN mismatch rejects connections at the TLS layer (fast, no app-level cost)
- Each node has a unique cryptographic identity (iroh keypair)

### What doesn't work

| Problem | Impact |
|---------|--------|
| Secret is shared | If one node is compromised, the secret is known to the attacker |
| No revocation | A leaked secret gives permanent access — no way to exclude a single node |
| No per-message signing | A node inside the cluster can forge identities in gossip |
| No sender verification | Gossip messages are accepted if they deserialize correctly |
| Flat trust | All nodes are equally trusted once they have the secret |

## Planned: CA-Based Node Certificates

The design follows the same pattern as PostgreSQL replication clusters and
Consul/Nomad mTLS: a cluster Certificate Authority signs individual node
certificates, and nodes verify each other's certificates before accepting
them into the cluster.

### Overview

```
                    ┌─────────────┐
                    │  Cluster CA │  (Ed25519 keypair, private key offline)
                    │  Public Key │  = cluster identity
                    └──────┬──────┘
                           │ signs
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Node A  │ │ Node B  │ │ Node C  │
         │ cert_A  │ │ cert_B  │ │ cert_C  │
         └─────────┘ └─────────┘ └─────────┘
```

### Cluster CA

- An Ed25519 keypair generated once at cluster creation time
- The **public key** is the cluster's identity — distributed to all nodes
- The **private key** is used only to sign node certificates and revocation
  lists, then stored offline (hardware token, vault, air-gapped machine)

### Node Certificate

A compact signed structure (~128 bytes):

```rust
struct NodeCertificate {
    /// The node's iroh public key (already used as NodeId).
    node_pubkey: [u8; 32],
    /// Expiry timestamp (Unix seconds). Defense-in-depth.
    expires_at: u64,
    /// Optional: roles, zones, capabilities.
    metadata: BTreeMap<String, String>,
    /// Ed25519 signature by the CA private key over the above fields.
    ca_signature: [u8; 64],
}
```

Verification: `ed25519_verify(ca_pubkey, serialize(node_pubkey, expires_at, metadata), ca_signature)`.

### Enrollment Flow

```
1. Admin generates cluster CA:
   $ shoal ca init → writes ca.pub + ca.key

2. Node generates its iroh keypair (automatic on first start):
   $ shoal start → generates ~/.shoal/node.key, prints NodeId

3. Admin enrolls the node:
   $ shoal ca sign <node-id> --expires 365d → writes node-cert.bin

4. Node is configured with its certificate:
   [cluster]
   ca_pubkey = "<hex>"
   node_cert = "/path/to/node-cert.bin"
```

### Authentication Flow

When node A connects to node B:

```
A ──────── QUIC TLS 1.3 handshake ──────── B
           (Ed25519 identity verified)

A ── shoal/0 ALPN ── B
     (cluster protocol version match)

A ── NodeCertificate ── B
     B verifies:
       1. ca_signature is valid for ca_pubkey
       2. node_pubkey matches A's QUIC identity
       3. expires_at > now
       4. node_pubkey not in revocation list
     If any check fails → drop connection
```

This happens as the first message after ALPN negotiation, before any cluster
data is exchanged.

### Revocation

When a node is compromised or decommissioned:

```
1. Admin signs a revocation entry:
   $ shoal ca revoke <node-id> → signs (node-id, revoked_at) with CA key

2. Revocation is gossiped to all cluster nodes:
   ClusterEvent::NodeRevoked { node_id, revoked_at, ca_signature }

3. Each node adds it to a local revocation list (persisted in Fjall)

4. Future connections from the revoked node are rejected
   (even though its certificate is technically still valid)
```

The revocation list is itself signed by the CA, so nodes can verify its
authenticity. Revocations are permanent — to re-admit a node, issue a new
certificate.

### Message Authentication

Beyond connection-level auth, individual messages should be signed:

```rust
struct AuthenticatedMessage {
    /// The actual message content.
    payload: ShoalMessage,
    /// Sender's NodeId.
    sender: NodeId,
    /// Ed25519 signature over (payload, sender, timestamp).
    signature: [u8; 64],
    /// Prevents replay.
    timestamp: u64,
}
```

This prevents a compromised-then-revoked node from having injected forged
messages that remain in gossip buffers. Each node already has an Ed25519
key (from iroh), so signing is zero-cost in terms of key management.

### Key Rotation

- **Node keys**: node generates a new iroh keypair, admin issues a new
  certificate, old certificate is revoked
- **CA key**: requires a "CA rotation event" gossiped with the old CA's
  signature endorsing the new CA public key. All nodes update their
  `ca_pubkey`. This is a rare, planned operation.

### Migration from Shared Secret

The shared secret can coexist with CA-based auth during migration:

1. **Phase 1**: Add CA support alongside shared secret. Nodes with certificates
   use them; nodes without fall back to secret-only. Log warnings for
   secret-only connections.
2. **Phase 2**: Require certificates. Remove shared secret support.
   `cluster.secret` becomes optional (used only for ALPN derivation as a
   namespace, not for authentication).

### Comparison

| | Shared Secret | CA Certificates |
|-|---------------|-----------------|
| Revoke one node | Impossible (must rotate secret on all nodes) | Sign a revocation entry |
| Secret leaks | Full cluster compromise | Only the compromised node |
| Admin overhead | Copy one string | Run `shoal ca sign` per node |
| Offline CA | N/A | Private key can be air-gapped |
| Expiry | None | Built-in, configurable per node |
| Audit trail | None | CA signs everything, verifiable |
| Scalability | Same secret everywhere | Each node has unique credentials |

## Data Integrity

Orthogonal to peer trust, Shoal verifies data integrity at every layer:

| Layer | Mechanism |
|-------|-----------|
| Storage | `ShardId = blake3(data)`. FileStore re-hashes on read |
| Network | Receiver verifies blake3 hash on every shard push/pull |
| Manifest | `ObjectId = blake3(serialized manifest)`. Verified on gossip receive |
| Anti-entropy | Majority-vote scrub: local hash vs. N peers, majority wins |

A malicious node inside the cluster cannot serve corrupted data without
detection — every piece of data is verified by its content hash.

## Ports and Firewall

| Port | Protocol | Purpose |
|------|----------|---------|
| 4820 | UDP (QUIC) | Inter-node communication (shards, membership, gossip) |
| 4821 | TCP (HTTP) | S3 API |

In production, port 4820 should only be accessible from other cluster nodes.
Port 4821 should be exposed to clients (possibly behind a load balancer or
reverse proxy with TLS termination).
