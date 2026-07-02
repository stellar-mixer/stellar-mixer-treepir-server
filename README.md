# Stellar Mixer TreePIR Server

**Stellar Mixer TreePIR Server** is a working HTTP server for privately retrieving Merkle paths used by the Stellar Mixer infrastructure.

It connects three layers:

1. The **Stellar Mixer contract**, which keeps the on-chain mixer state minimal.
2. The **public Stellar event stream**, which contains enough information to reconstruct the mixer Merkle tree.
3. **TreePIR**, which lets clients retrieve Merkle paths without revealing which leaf they care about.

In plain terms: this server watches the mixer contract, rebuilds the public Merkle tree off-chain, and answers private PIR requests for Merkle paths.

## Why this server exists

A mixer needs a Merkle tree.

When a user deposits, withdraws change, or receives a private transfer, a new leaf is added to the tree. Later, the user needs a Merkle path for their leaf in order to prove membership against a public root.

The simple way would be:

    Client:
      "Give me the Merkle path for leaf index 12345."

    Server:
      "Here is the path."

That works, but it leaks the leaf index.

For a privacy system, that is a problem. The server would learn which note, commitment, or transfer output the user is interested in.

TreePIR changes this flow.

Instead of asking for a path directly, the client creates private PIR queries. The server processes those queries over public level databases and returns encrypted PIR responses. The client then decodes the responses locally and reconstructs the Merkle path.

The server helps the client, but should not learn the selected leaf index.

## Contract-minimal architecture

The Stellar Mixer contract is intentionally minimal.

Its job is to enforce the rules that must be enforced on-chain:

    - verify zero-knowledge proofs
    - move tokens according to valid proofs
    - prevent double spends through nullifiers
    - update the public Merkle root/state
    - emit enough events for off-chain infrastructure

The contract should not become a database server.

It should not expose large query APIs, store bulky recovery data, calculate Merkle paths for users, or serve historical sync data. Doing that on-chain would make the contract heavier, more expensive, and more complex than necessary.

Instead, the mixer is split into two parts:

    On-chain contract:
      minimal consensus-critical state and verification

    Off-chain infrastructure:
      indexing, tree reconstruction, note sync, path retrieval, recovery helpers

This repository is one of those off-chain infrastructure pieces.

## What this server does

This server:

    - connects to Stellar RPC
    - watches the Stellar Mixer contract events
    - extracts output leaves from deposit, withdraw, and transfer events
    - rebuilds the mixer Merkle tree locally
    - stores indexed leaves in RocksDB
    - exposes public HTTP endpoints
    - lets registered clients request Merkle paths through TreePIR
    - returns PIR responses without receiving the private leaf index

The server is not trusted to prove correctness by itself. The client still verifies the resulting Merkle path against the public root.

If a server is stale, broken, or malicious, the client can reject the result or use another server.

## Logical flow

    1. Mixer contract emits events
       Deposits, withdraw changes, and transfer outputs add new leaves.

    2. TreePIR server indexes those events
       The server reads contract events from Stellar RPC.

    3. Server rebuilds the Merkle tree
       It appends the same leaves in the same order and tracks the public root.

    4. Server builds TreePIR level databases
       Each tree level becomes a database of possible sibling nodes.

    5. Client registers PIR CRS
       The server learns only the client CRS hash / registration state.

    6. Client keeps its leaf index private
       The leaf index is never sent to the server.

    7. Client creates private PIR queries
       One query is created for each Merkle tree level.

    8. Server answers the private queries
       The server performs cryptographic PIR response work over its level databases.

    9. Client decodes responses locally
       The client recovers one sibling per level.

    10. Client reconstructs the Merkle path
        The recovered siblings form a standard Merkle proof path.

    11. Client verifies against the public root
        The root is public, so the final path can be checked normally.

## Normal Merkle path server vs TreePIR server

A normal server would see this:

    "Give me the path for leaf #12345."

A TreePIR server sees this:

    "Here is a private query for level 0."
    "Here is a private query for level 1."
    "Here is a private query for level 2."
    ...

The difference is important.

The normal request directly reveals the note index.

The TreePIR request lets the server do useful work without learning which sibling was selected at each level.

## What the server sees

The server sees:

    - public contract events
    - public Merkle tree data
    - public level database sizes
    - client CRS registration
    - PIR queries
    - number of requested levels

The server should not learn:

    - the target leaf index
    - the selected sibling indexes
    - the final Merkle path chosen by the client
    - which private note the client is trying to use

## Why not put this directly into the contract?

A contract is good at enforcing rules.

It is bad at serving large dynamic datasets.

If the contract tried to serve Merkle paths directly, several bad things would happen:

    - every path request would be visible on-chain
    - path retrieval would become expensive
    - the contract would become larger and harder to audit
    - user access patterns would become public
    - infrastructure logic would be coupled to consensus logic

That is exactly what this architecture avoids.

The contract stays small and focused. TreePIR servers handle off-chain data access.

## Why not use a simple centralized API?

A simple API could expose:

    GET /path?leaf_index=12345

That is easy to build.

But it creates a privacy leak and a central point of observation. Whoever runs the API can see which leaf each client asks for.

TreePIR keeps the useful part of the API — path retrieval — while removing the direct index leak.

The server still does work, but the sensitive selection stays client-side.

## Why not make every client download the whole tree?

That would give strong privacy because the client would not ask for a specific path.

But it does not scale well.

For a small tree, downloading everything is fine. For a large mixer tree, repeatedly downloading all leaves and all levels becomes heavy for normal users.

TreePIR is the middle ground:

    - lighter than downloading the full tree
    - more private than asking for a direct path
    - easy to run as public infrastructure
    - verifiable by the client

## Public infrastructure model

This server is part of the Stellar Mixer infrastructure.

It does not need to be run only by the original mixer developers. Any operator can run a compatible TreePIR server and help support the network.

That is important.

A privacy system should not depend on one official backend. Multiple independent servers make the system more robust:

    - clients can switch servers
    - stale servers can be avoided
    - overloaded servers can be replaced
    - infrastructure can be geographically distributed
    - the mixer is less dependent on a single operator

The data source is public: Stellar contract events.

The verification target is public: the Merkle root.

The result is client-verifiable: a Merkle path either matches the root or it does not.

That means the server can be useful without being fully trusted.

## Current HTTP API

The server exposes:

    GET  /health
    GET  /ready
    GET  /v1/pir-params
    GET  /v1/layout
    POST /v1/clients/register
    POST /v1/path

The exact client flow is:

    1. Fetch PIR params
       GET /v1/pir-params

    2. Register client CRS
       POST /v1/clients/register

    3. Fetch current layout
       GET /v1/layout?crs_hash=...

    4. Build private path query locally
       The client keeps the leaf index private.

    5. Send PIR path query
       POST /v1/path

    6. Decode PIR responses locally
       The server does not decode the selected path for the client.

## Configuration

The default `.env` is intended for Stellar testnet infrastructure.

Important values:

    TREEPIR_BIND_ADDR=0.0.0.0:3000
    TREEPIR_STELLAR_RPC_URL=https://soroban-rpc.testnet.stellar.gateway.fm
    TREEPIR_MIXER_CONTRACT_ID=...
    TREEPIR_START_LEDGER=...
    TREEPIR_DB_PATH=./treepir-server-state-v2.rocksdb

Port `3000` is a normal default service port.

For production deployments, operators may keep the server on port `3000` internally and expose it through nginx, Caddy, a load balancer, or a firewall rule.

## Running locally

Build:

    cargo build

Run:

    cargo run

Check health:

    curl http://127.0.0.1:3000/health

Check readiness:

    curl http://127.0.0.1:3000/ready

Run tests:

    cargo test

## Relationship to TreePIR Core

`treepir-core` contains the reusable TreePIR protocol logic:

    - Merkle tree construction
    - Merkle path verification
    - level database layout
    - client-side PIR query flow
    - server-side PIR response flow
    - InsPIRe-backed PIR integration

This repository turns that core library into a working Stellar Mixer infrastructure service.

In short:

    treepir-core:
      reusable private Merkle path retrieval library

    stellar-mixer-treepir-server:
      Stellar-aware event indexer + persistent tree state + HTTP TreePIR API

## Future x402 incentives

Running a public TreePIR server costs resources:

    - RPC bandwidth
    - indexing time
    - disk storage
    - CPU for PIR response generation
    - server hosting

At first, servers can be run voluntarily by the project or community operators.

In future implementations, this infrastructure can be paired with **x402**.

The idea is simple:

    1. A client requests a paid PIR operation.
    2. The server replies with HTTP 402 Payment Required.
    3. The client pays a small amount using x402.
    4. The server performs the PIR work and returns the response.

That creates a direct incentive for independent operators to run TreePIR servers.

The fee should be small, but enough to compensate server operators for bandwidth, CPU, and uptime.

This is not required for the current server to work. It is a future incentive layer for a healthier decentralized infrastructure network.

## Trust model

Clients should treat TreePIR servers as helpful but not authoritative.

A server can:

    - be stale
    - be offline
    - refuse requests
    - return malformed responses
    - index the wrong contract if misconfigured

But a server should not be able to make a client accept an invalid Merkle path, because the client verifies the recovered path against the public root.

The server provides data access.

The client verifies correctness.

## Why it matters

A mixer needs public state so everyone can agree on the same root.

A user needs a Merkle path so they can prove membership.

But asking for that path directly reveals which leaf the user cares about.

Stellar Mixer TreePIR Server exists to solve that gap.

It keeps the contract minimal, keeps the infrastructure open, and lets clients privately retrieve the paths they need.
