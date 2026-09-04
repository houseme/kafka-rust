# Kafka Protocol Coverage

This matrix tracks the `kafka-protocol` `0.18.0` request APIs visible to the crate and how far
`rustfs-kafka` exposes them. It is intentionally biased toward normal client/admin APIs; broker,
controller, and coordinator-internal APIs should not be exposed as stable public helpers unless there is
a clear user-facing workflow.

Source checked:

- `kafka-protocol-0.18.0/src/messages.rs`
- `kafka-protocol-0.18.0/src/messages/*_request.rs`
- `crates/rustfs-kafka/src/protocol/*`
- `crates/rustfs-kafka/src/client/mod.rs`

## Coverage Summary

- Total `kafka-protocol` API keys: 87.
- Public or high-level runtime coverage: 62 APIs; async convenience coverage now includes
  ApiVersions, CreateTopics, DeleteTopics, Telemetry, ConsumerGroupHeartbeat, ShareGroupHeartbeat,
  ShareFetch, and ShareAcknowledge on top of raw generated requests.
- Internal runtime coverage without direct public API: 10 APIs.
- Raw generated protocol coverage: all remaining generated request/response APIs can be sent through
  `KafkaClient::send_raw_protocol_request` or `AsyncKafkaClient::send_raw_protocol_request` with
  explicit `api_key` and `api_version`. The stronger
  `kafka_protocol::protocol::Request` association is not exposed as a public facade under the
  current client-only dependency feature set because those generated impls also require
  `kafka-protocol`'s `broker` feature.
- Client-facing backlog: 0 APIs. `CreateTopics` and `DeleteTopics` now use generated
  `kafka-protocol` request/response codecs instead of handwritten body parsers.
- Advanced runtime backlog: no missing public protocol adapters; share-consumer and telemetry session helpers are
  available, including converting acquired `ShareFetch` ranges into `ShareAcknowledge` options.
  Automatic background runtime loops remain intentionally caller-owned.
- Broker/controller/internal backlog: 0 public adapters; high-level workflows remain intentionally absent for
  quorum, coordinator, broker, controller, raft snapshot, and share-state internals.
- Generated request framing is centralized in a single-buffer encoder based on
  `Encodable::compute_size`; sync transport, SASL, admin helpers, and async wire code all use the
  same path.

## API Matrix

| Key | Protocol | Current status | Next action |
| --- | --- | --- | --- |
| 0 | Produce | Public/runtime implemented | Keep current producer API; continue protocol-version upgrades as needed. |
| 1 | Fetch | Public/runtime implemented | Keep current consumer API; continue protocol-version upgrades as needed. |
| 2 | ListOffsets | Public implemented | Keep public offset query helpers. |
| 3 | Metadata | Public/runtime implemented | Keep metadata loading helpers. |
| 8 | OffsetCommit | Public/runtime implemented | Keep group offset commit helpers. |
| 9 | OffsetFetch | Public/runtime implemented | Keep group offset fetch helpers. |
| 10 | FindCoordinator | Internal/runtime implemented | Keep internal; expose only via higher-level group/transaction APIs. |
| 11 | JoinGroup | Internal consumer runtime implemented | Keep internal until consumer group protocol is modernized. |
| 12 | Heartbeat | Internal consumer runtime implemented | Keep internal. |
| 13 | LeaveGroup | Internal consumer runtime implemented | Keep internal. |
| 14 | SyncGroup | Internal consumer runtime implemented | Keep internal until group protocol modernization. |
| 15 | DescribeGroups | Public admin implemented | Done. |
| 16 | ListGroups | Public admin implemented | Done. |
| 17 | SaslHandshake | Internal auth runtime implemented | Keep internal auth flow. |
| 18 | ApiVersions | Public admin/runtime implemented | Done. |
| 19 | CreateTopics | Public admin implemented with generated codec | Done. |
| 20 | DeleteTopics | Public admin implemented with generated codec | Done. |
| 21 | DeleteRecords | Public admin implemented | Done. |
| 22 | InitProducerId | Internal transactional producer runtime implemented | Keep internal; extend only through transaction producer API. |
| 23 | OffsetForLeaderEpoch | Public diagnostic implemented | Done. |
| 24 | AddPartitionsToTxn | Internal transactional producer runtime implemented | Keep internal. |
| 25 | AddOffsetsToTxn | Public advanced transaction implemented | Done. |
| 26 | EndTxn | Internal transactional producer runtime implemented | Keep internal. |
| 27 | WriteTxnMarkers | Raw generated protocol access implemented | Coordinator-internal; no high-level client API. |
| 28 | TxnOffsetCommit | Public advanced transaction implemented | Done. |
| 29 | DescribeAcls | Public admin implemented | Done. |
| 30 | CreateAcls | Public admin implemented | Done. |
| 31 | DeleteAcls | Public admin implemented | Done. |
| 32 | DescribeConfigs | Public admin implemented | Done. |
| 33 | AlterConfigs | Public legacy admin implemented, deprecated | Prefer `IncrementalAlterConfigs`; kept only for compatibility. |
| 34 | AlterReplicaLogDirs | Public broker storage admin implemented | Done. |
| 35 | DescribeLogDirs | Public admin implemented | Done. |
| 36 | SaslAuthenticate | Internal auth runtime implemented | Keep internal auth flow. |
| 37 | CreatePartitions | Public admin implemented | Done. |
| 38 | CreateDelegationToken | Public security admin implemented | Done. |
| 39 | RenewDelegationToken | Public security admin implemented | Done. |
| 40 | ExpireDelegationToken | Public security admin implemented | Done. |
| 41 | DescribeDelegationToken | Public admin implemented | Done. |
| 42 | DeleteGroups | Public admin implemented | Done. |
| 43 | ElectLeaders | Public admin implemented | Done. |
| 44 | IncrementalAlterConfigs | Public admin implemented | Done. |
| 45 | AlterPartitionReassignments | Public admin implemented | Done. |
| 46 | ListPartitionReassignments | Public admin implemented | Done. |
| 47 | OffsetDelete | Public admin implemented | Done. |
| 48 | DescribeClientQuotas | Public admin implemented | Done. |
| 49 | AlterClientQuotas | Public admin implemented | Done. |
| 50 | DescribeUserScramCredentials | Public admin implemented | Done. |
| 51 | AlterUserScramCredentials | Public security admin implemented | Done; caller supplies precomputed SCRAM salt and salted password bytes. |
| 52 | Vote | Raw generated protocol access implemented | Quorum-internal; no high-level client API. |
| 53 | BeginQuorumEpoch | Raw generated protocol access implemented | Quorum-internal; no high-level client API. |
| 54 | EndQuorumEpoch | Raw generated protocol access implemented | Quorum-internal; no high-level client API. |
| 55 | DescribeQuorum | Public admin implemented | Done. |
| 56 | AlterPartition | Raw generated protocol access implemented | Controller/internal; no high-level client API. |
| 57 | UpdateFeatures | Public KRaft feature admin implemented | Done; prefer `validate_only` before applying changes. |
| 58 | Envelope | Raw generated protocol access implemented | Broker/controller forwarding; no high-level client API. |
| 59 | FetchSnapshot | Raw generated protocol access implemented | Raft snapshot protocol; no high-level client API. |
| 60 | DescribeCluster | Public admin implemented | Done. |
| 61 | DescribeProducers | Public diagnostic implemented | Done. |
| 62 | BrokerRegistration | Raw generated protocol access implemented | Broker-internal; no high-level client API. |
| 63 | BrokerHeartbeat | Raw generated protocol access implemented | Broker-internal; no high-level client API. |
| 64 | UnregisterBroker | Public KRaft broker lifecycle admin implemented | Done; destructive cluster operation. |
| 65 | DescribeTransactions | Public diagnostic implemented | Done. |
| 66 | ListTransactions | Public diagnostic implemented | Done. |
| 67 | AllocateProducerIds | Raw generated protocol access implemented | Broker/internal producer-id API; no high-level client API. |
| 68 | ConsumerGroupHeartbeat | Public low-level consumer heartbeat protocol API implemented | Full modern consumer-group runtime still pending. |
| 69 | ConsumerGroupDescribe | Public diagnostic implemented | Done. |
| 70 | ControllerRegistration | Raw generated protocol access implemented | Controller-internal; no high-level client API. |
| 71 | GetTelemetrySubscriptions | Public low-level telemetry protocol API implemented | Use `TelemetrySession` for subscription state; full automatic telemetry scheduler/export pipeline still pending. |
| 72 | PushTelemetry | Public low-level telemetry protocol API implemented | Use `TelemetrySession` to build compatible push options; OpenTelemetry encoding remains caller-owned. |
| 73 | AssignReplicasToDirs | Public broker storage admin implemented | Done; use only with explicit JBOD/directory-assignment workflow. |
| 74 | ListConfigResources | Public admin implemented | Done. |
| 75 | DescribeTopicPartitions | Public diagnostic implemented | Done. |
| 76 | ShareGroupHeartbeat | Public low-level share-consumer protocol API implemented | Use `ShareConsumerSession` for member/assignment state; full fetch loop still pending. |
| 77 | ShareGroupDescribe | Public diagnostic implemented | Done. |
| 78 | ShareFetch | Public low-level share-consumer protocol API implemented | Use `ShareConsumerSession` to compose assignment-based fetch options. |
| 79 | ShareAcknowledge | Public low-level share-consumer protocol API implemented | Use `ShareConsumerSession` to compose acknowledgement options. |
| 80 | AddRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 81 | RemoveRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 82 | UpdateRaftVoter | Public KRaft quorum admin implemented | Done; explicit KRaft voter workflow only. |
| 83 | InitializeShareGroupState | Raw generated protocol access implemented | Share coordinator internal; no high-level client API. |
| 84 | ReadShareGroupState | Raw generated protocol access implemented | Share coordinator internal; no high-level client API. |
| 85 | WriteShareGroupState | Raw generated protocol access implemented | Share coordinator internal; no high-level client API. |
| 86 | DeleteShareGroupState | Raw generated protocol access implemented | Share coordinator internal; no high-level client API. |
| 87 | ReadShareGroupStateSummary | Raw generated protocol access implemented | Share coordinator internal; no high-level client API. |
| 90 | DescribeShareGroupOffsets | Public diagnostic implemented | Done. |
| 91 | AlterShareGroupOffsets | Public share-group admin implemented | Done. |
| 92 | DeleteShareGroupOffsets | Public share-group admin implemented | Done. |

## Recommended Implementation Batches

All visible client-facing protocol adapters are now implemented, and topic create/delete administration
now uses generated request/response codecs. Remaining generated protocol messages are reachable through
the typed raw request API. Remaining high-level work is runtime-level:

1. Share consumer runtime: `ShareConsumerSession` composes stateful request options and ack options from acquired fetch ranges; a full background fetch loop remains outside the current stable API.
2. Telemetry runtime: `TelemetrySession` tracks broker subscriptions; the automatic scheduler/export pipeline remains outside the current stable API.
3. Keep broker, controller, coordinator, and raft-log internals out of stable high-level APIs unless a dedicated
   controller client is introduced.
4. Keep `kafka-protocol` on the `client` feature only unless broker-side APIs are deliberately
   introduced; enabling generated `Request` impls widens compile surface for limited client-side
   benefit.
