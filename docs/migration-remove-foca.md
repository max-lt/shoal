# Plan de migration : suppression de foca/SWIM

## Résumé

Virer foca/SWIM et utiliser iroh QUIC comme unique couche réseau.
Remplacer la détection de pannes SWIM par un système ping/pong QUIC natif.

**Ce qu'on garde :** `ClusterState`, `GossipHandle`/`GossipService`, `AddressBook`, placement ring.
**Ce qu'on vire :** `foca` (dépendance), `ClusterIdentity`, `MembershipHandle`, `MembershipCommand`, `SwimData`, le SWIM outgoing routing loop, `PostcardCodec`.

---

## Inventaire des fichiers impactés

### Fichiers à supprimer
| Fichier | Raison |
|---------|--------|
| `crates/shoal-cluster/src/identity.rs` | `ClusterIdentity` = wrapper foca::Identity, plus besoin |

### Fichiers à réécrire en grande partie
| Fichier | Changement |
|---------|------------|
| `crates/shoal-cluster/src/membership.rs` | Remplacer la boucle foca par un `PeerManager` basé sur QUIC ping/pong |
| `crates/shoal-cluster/src/tests.rs` | Réécrire les tests SWIM → tests du nouveau `PeerManager` |

### Fichiers à modifier
| Fichier | Changement |
|---------|------------|
| `crates/shoal-cluster/Cargo.toml` | Supprimer `foca`, `postcard`, `serde` (plus besoin pour le codec foca) |
| `crates/shoal-cluster/src/lib.rs` | Supprimer `identity` module, `ClusterIdentity` re-export. Exporter `PeerManager`/`PeerHandle` |
| `crates/shoal-cluster/src/error.rs` | Supprimer `ClusterError::Foca`, ajouter `ClusterError::PingTimeout` ou similaire |
| `crates/shoal-cluster/src/gossip.rs` | Aucun changement fonctionnel (il utilise iroh-gossip, pas foca) |
| `crates/shoal-cluster/src/state.rs` | Aucun changement (il est déjà indépendant de foca) |
| `crates/shoal-net/src/message.rs` | Supprimer `SwimData(Vec<u8>)`. Garder `Ping`/`Pong` (déjà présents). Ajouter `JoinRequest`/`JoinResponse` |
| `crates/shoal-net/src/tests.rs` | Supprimer tests de serialization de `SwimData`. Ajouter tests pour `JoinRequest`/`JoinResponse` |
| `crates/shoald/src/handler.rs` | Supprimer `MembershipHandle` du `ShoalProtocol`. Supprimer le match arm `SwimData`. Ajouter handler pour `Ping`→`Pong` et `JoinRequest`→`JoinResponse` |
| `crates/shoald/src/main.rs` | Supprimer : SWIM outgoing routing loop (L463-505), `ClusterIdentity` construction (L396-397), `membership::start_with_address_book()` (L398-404), SWIM seed join (L406-461). Remplacer par : init du `PeerManager`, seed connect via QUIC |
| `Cargo.toml` (workspace) | Supprimer `foca` de `[workspace.dependencies]` |

---

## Architecture du remplacement

### Nouveau composant : `PeerManager`

Le `PeerManager` remplace `MembershipHandle` + foca. Il vit dans `crates/shoal-cluster/src/membership.rs` (réécriture).

```
PeerManager
├── known_peers: HashMap<NodeId, PeerState>
├── cluster: Arc<ClusterState>
├── address_book: AddressBook
├── transport: Arc<dyn Transport>  ← NOUVEAU : besoin d'accès direct au transport
├── meta: Option<Arc<MetaStore>>
└── config: PeerManagerConfig
```

**`PeerState`** :
```rust
struct PeerState {
    addr: EndpointAddr,
    last_pong: Instant,
    consecutive_failures: u32,
    state: MemberState,   // Alive | Suspect | Dead
    generation: u64,
    capacity: u64,
    topology: NodeTopology,
}
```

**`PeerManagerConfig`** :
```rust
struct PeerManagerConfig {
    ping_interval: Duration,        // 1s en prod, 100ms en test
    suspect_timeout: Duration,      // 5s en prod, 500ms en test
    dead_timeout: Duration,         // 15s en prod, 2s en test
    max_failures_before_suspect: u32,  // 3
}
```

### Détection de pannes (remplacement de SWIM)

Boucle principale :
1. Toutes les `ping_interval` : envoyer `Ping { timestamp }` à chaque pair connu via `transport.send_to()`
2. Sur réception de `Pong { timestamp }` : mettre à jour `last_pong`, remettre `consecutive_failures = 0`
3. Si pas de pong après `ping_interval` : incrémenter `consecutive_failures`
4. Si `consecutive_failures >= max_failures_before_suspect` : marquer `Suspect`, émettre log
5. Si `last_pong` > `dead_timeout` : appeler `cluster.mark_dead()`, émettre `MembershipDead`
6. Sur `Pong` d'un nœud `Suspect` : remettre `Alive`, appeler `cluster.add_member()`

**Différence vs SWIM :** Pas d'indirect ping (ping via un tiers). On perd la résistance aux faux positifs de SWIM. Mais dans notre cas :
- iroh gère déjà le retry/relay au niveau QUIC
- Les connexions QUIC ont un heartbeat natif (keep-alive)
- On peut ajouter l'indirect ping plus tard si besoin

### Rejoindre le cluster (remplacement de `foca.announce()`)

Nouveau message : `JoinRequest` / `JoinResponse`.

```rust
// Dans ShoalMessage :
JoinRequest {
    node_id: NodeId,
    generation: u64,
    capacity: u64,
    topology: NodeTopology,
},
JoinResponse {
    members: Vec<Member>,    // tous les membres connus
},
```

Flow :
1. Nouveau nœud envoie `JoinRequest` au seed via `transport.send_to()`
2. Le seed répond avec `JoinResponse { members }` (liste complète)
3. Le nouveau nœud ajoute tous les membres à son `ClusterState` + `AddressBook`
4. Le seed ajoute le nouveau nœud et broadcast l'événement via gossip (`GossipPayload::Event(NodeJoined)`)
5. Les autres nœuds reçoivent le gossip et ajoutent le nœud

### Départ gracieux (remplacement de `foca.leave_cluster()`)

Flow :
1. Le nœud broadcast `GossipPayload::Event(NodeLeft(node_id))` via gossip
2. Les pairs reçoivent et appellent `cluster.remove_member()`
3. Le `PeerManager` s'arrête

Pas besoin de message dédié — gossip suffit.

### Gestion des adresses (remplacement de la dissémination via SWIM)

Actuellement, les listen_addrs sont propagés dans `ClusterIdentity` via foca.
Remplacement : les `JoinRequest`/`JoinResponse` portent les `EndpointAddr`. En régime permanent, les adresses sont apprises automatiquement via iroh (relay discovery par `EndpointId`).

---

## Ordre des opérations

### Phase 1 : Préparer les messages réseau

**Fichier : `crates/shoal-net/src/message.rs`**
1. Supprimer `SwimData(Vec<u8>)` de `ShoalMessage`
2. Ajouter `JoinRequest { node_id, generation, capacity, topology }` et `JoinResponse { members }` à `ShoalMessage`
3. Vérifier que `Ping` et `Pong` sont toujours présents (ils le sont déjà)

**Fichier : `crates/shoal-net/src/tests.rs`**
4. Supprimer les tests `test_swim_data_roundtrip` et `test_swim_data_empty_roundtrip`
5. Ajouter tests roundtrip pour `JoinRequest` / `JoinResponse`

### Phase 2 : Réécrire le membership dans shoal-cluster

**Fichier : `crates/shoal-cluster/Cargo.toml`**
1. Supprimer `foca` des dépendances

**Fichier : supprimer `crates/shoal-cluster/src/identity.rs`**
2. Supprimer le fichier entier

**Fichier : `crates/shoal-cluster/src/lib.rs`**
3. Supprimer `mod identity` et `pub use identity::ClusterIdentity`
4. Supprimer `pub use membership::AddressBook` (le déplacer dans membership ou le garder — à évaluer)
5. Ajouter les nouveaux exports : `pub use membership::{PeerManager, PeerHandle, PeerManagerConfig}`

**Fichier : `crates/shoal-cluster/src/error.rs`**
6. Remplacer `ClusterError::Foca(String)` par `ClusterError::PingFailed(String)` (ou similaire)

**Fichier : `crates/shoal-cluster/src/membership.rs` (réécriture)**
7. Supprimer tout le code foca (`membership_loop`, `drain_runtime`, `handle_notification`, `MembershipCommand`, `MembershipHandle`)
8. Implémenter `PeerManagerConfig` avec `test_config()` et `default_config()`
9. Implémenter `PeerState` (état par pair)
10. Implémenter `PeerManager` :
    - `new(config, node_id, cluster, transport, meta, address_book) -> Self`
    - `async fn run(&self)` — boucle principale (ping, détection, cleanup)
    - `async fn add_peer(&self, node_id, addr, member_info)` — ajouter un pair connu
    - `async fn handle_pong(&self, node_id, timestamp)` — callback sur pong reçu
    - `async fn handle_join_request(&self, req) -> JoinResponse` — callback quand un nœud veut rejoindre
    - `async fn join_via_seed(&self, seed_addr) -> Result<()>` — envoyer JoinRequest et traiter la réponse
    - `fn leave(&self)` — arrêter la boucle
11. Implémenter `PeerHandle` (API publique, comme `MembershipHandle` mais sans SWIM) :
    - `join_via_seed(seed_addr)`
    - `leave()`
    - `add_peer(node_id, addr)`
    - `state() -> &Arc<ClusterState>`

**Fichier : `crates/shoal-cluster/src/tests.rs` (réécriture)**
12. Supprimer tous les tests foca (`test_cluster_identity_foca_traits`, `test_foca_handles_suspect_self_without_panic`, etc.)
13. Conserver et adapter les tests `ClusterState` (ils ne dépendent pas de foca)
14. Écrire de nouveaux tests :
    - `test_ping_pong_alive` — pair reste alive avec pongs réguliers
    - `test_no_pong_marks_suspect_then_dead` — absence de pong → suspect → dead
    - `test_join_via_seed` — nouveau nœud obtient la liste des membres
    - `test_leave_gracefully` — nœud part, pairs le détectent
    - `test_recovery_after_suspect` — pong reçu après suspect → retour alive
    - `test_peer_manager_with_multiple_nodes` — 3+ nœuds, stabilité

### Phase 3 : Mettre à jour le handler du daemon

**Fichier : `crates/shoald/src/handler.rs`**
1. Supprimer le champ `membership: Arc<MembershipHandle>` de `ShoalProtocol`
2. Ajouter `peer_manager: Arc<PeerHandle>` (ou passer les callbacks via closure)
3. Dans le handler uni-stream : supprimer le match arm `ShoalMessage::SwimData`
4. Dans le handler bi-stream : ajouter un match arm pour `JoinRequest` → appeler `peer_manager.handle_join_request()` → répondre `JoinResponse`
5. Ajouter un match arm pour `Ping` → répondre `Pong` (note : Ping/Pong pourraient aussi être sur uni-stream si on ne veut pas bloquer)
6. Mettre à jour `ShoalProtocol::new()` pour prendre le `PeerHandle` au lieu de `MembershipHandle`

### Phase 4 : Mettre à jour le daemon main

**Fichier : `crates/shoald/src/main.rs`**
1. Supprimer les imports : `ClusterIdentity`, `membership::*`
2. Supprimer la construction de `ClusterIdentity` (L396-397)
3. Supprimer `membership::start_with_address_book()` (L398-404)
4. Supprimer la boucle de chargement des pairs persistés avec `membership_handle.join()` (L406-439)
5. Supprimer la boucle de connexion aux peers CLI avec `membership_handle.join()` (L441-461)
6. Supprimer la boucle SWIM outgoing routing (L463-505)
7. Supprimer `membership_handle.leave()` dans le shutdown (L1028)
8. Remplacer par :
   - Créer le `PeerManager` avec transport, cluster, address_book
   - Pour les pairs persistés : `peer_manager.add_peer()` + `peer_manager.join_via_seed()`
   - Pour les pairs CLI : `peer_manager.join_via_seed(seed_addr)`
   - Spawner `peer_manager.run()` en background
   - Passer le `PeerHandle` à `ShoalProtocol` au lieu de `MembershipHandle`
   - Au shutdown : `peer_manager.leave()`

### Phase 5 : Nettoyer le workspace

**Fichier : `Cargo.toml` (workspace root)**
1. Supprimer `foca = { ... }` de `[workspace.dependencies]`

**Fichier : `crates/shoal-cluster/Cargo.toml`**
2. Vérifier que `postcard` et `serde` sont encore nécessaires (gossip les utilise → oui, garder)
3. Ajouter `shoal-net` aux dépendances si pas déjà présent (il l'est déjà)

**Fichier : `docs/plan.md`**
4. Mettre à jour la description de `shoal-cluster` : "Membership (QUIC ping/pong) + gossip (iroh-gossip)" au lieu de "Membership (foca SWIM)"

### Phase 6 : Vérification

1. `cargo build` — doit compiler
2. `cargo test` — tous les tests passent
3. `cargo clippy -- -D warnings` — zéro warning
4. `cargo fmt --check` — formaté
5. Vérifier qu'aucune référence à `foca`, `SWIM`, `SwimData`, `ClusterIdentity`, `MembershipHandle` ne subsiste (sauf dans ce document)

---

## Dépendances entre phases

```
Phase 1 (messages) ← Phase 2 (membership) ← Phase 3 (handler)
                                            ← Phase 4 (main)
                                                      ← Phase 5 (cleanup)
                                                      ← Phase 6 (vérif)
```

Les phases 3 et 4 peuvent être faites en parallèle après la phase 2.

---

## Points d'attention

### Cold start
Le problème initial (cold start de plusieurs secondes sur le premier write) sera résolu car :
- Le `JoinRequest` via QUIC force l'établissement de la connexion au seed dès le boot
- Le seed répond avec la liste complète → le nouveau nœud ouvre les connexions QUIC aux autres pairs immédiatement (connection pooling dans `ShoalTransport`)
- Plus de phase "discovery" asynchrone comme avec SWIM

### Régression possible : faux positifs de détection de panne
SWIM utilise l'indirect ping (ping via un tiers) pour réduire les faux positifs. Notre remplacement ne l'a pas.
Mitigation :
- `max_failures_before_suspect: 3` = 3 pings manqués avant suspect
- `dead_timeout: 15s` = 15s sans pong avant dead
- iroh QUIC a déjà un retry + relay automatique
- On peut ajouter l'indirect ping plus tard (le `PeerManager` peut demander à un autre pair de ping le suspect)

### Régression possible : dissémination d'adresses
Actuellement, les `listen_addrs` sont propagés via les messages SWIM (dans `ClusterIdentity`).
Remplacement :
- `JoinResponse` contient les `Member` avec leur `EndpointAddr`
- iroh découvre les adresses directes automatiquement via le relay
- Si besoin, on peut ajouter un champ `addrs` dans le `Member` struct

### Ce qui ne change PAS
- `ClusterState` — inchangé (pas de dépendance foca)
- `GossipService` / `GossipHandle` — inchangé (utilise iroh-gossip)
- `EventBus` — inchangé
- `Ring` / placement — inchangé
- `Transport` trait — inchangé
- `ShoalNode` / engine — inchangé (utilise `ClusterState`, pas `MembershipHandle`)
- `RepairDetector` / `RepairScheduler` / `RepairExecutor` — inchangé (utilise `ClusterState`)
- `shoal-s3` — inchangé
- Test harness (`tests/src/lib.rs`) — inchangé (manipule `ClusterState` directement)
- Tests d'intégration (`tests/integration/`) — inchangé (n'utilisent pas foca)
- Tests chaos (`tests/chaos/`) — inchangé (n'utilisent pas foca)

### Transport dans shoal-cluster
Actuellement `shoal-cluster` dépend de `shoal-net` mais n'utilise pas directement le `Transport` trait — foca produit des bytes et le daemon les route. Avec le `PeerManager`, le cluster a besoin d'envoyer des pings directement. Deux options :
1. **Le `PeerManager` prend un `Arc<dyn Transport>`** — couplage direct mais simple
2. **Le daemon route les pings comme il routait les messages SWIM** — moins de couplage mais même pattern qu'on veut éliminer

→ Option 1 recommandée. Le `PeerManager` utilise `transport.send_to()` pour les pings et `JoinRequest`. C'est plus simple et élimine la boucle de routing intermédiaire.

---

## Estimation de l'impact

| Métrique | Valeur |
|----------|--------|
| Fichiers supprimés | 1 (`identity.rs`) |
| Fichiers réécrits | 2 (`membership.rs`, `tests.rs` dans shoal-cluster) |
| Fichiers modifiés | ~8 |
| Lignes supprimées (estimation) | ~800 (foca membership loop, identity, SWIM routing, SwimData, tests) |
| Lignes ajoutées (estimation) | ~500 (PeerManager, ping/pong, JoinRequest/JoinResponse, tests) |
| Dépendances supprimées | 1 (`foca` + ses transitive deps) |
| Tests à réécrire | ~10 tests SWIM → ~8 tests PeerManager |
| Tests inchangés | ~tous les tests d'intégration et chaos |
