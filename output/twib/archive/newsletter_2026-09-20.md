# 📰 This Week in Bitcoin (2026-09-14 to 2026-09-20)

## 📌 The TL;DR
- Ongoing fundamental research into future protocol evolution, with discussions centered on integrating Post-Quantum Cryptography (PQC) output types and exploring Block-wide Signature Aggregation via SNARKs to address long-term security and scalability challenges.
- A significant refinement in how advanced script expressions are handled within wallets, marked by the decision to drop the concept of a validatable Descriptor ID for Miniscript, alongside active technical discussions on improving multi-party signing workflows, specifically for PSBT/MuSig2 coordination using decentralized communication relays like Nostr.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#34861: wallet: Add importdescriptors interface](https://github.com/bitcoin/bitcoin/pull/34861)
**Author:** [@polespinasa](https://github.com/polespinasa) | **[Wallet & User Tools]** *(Activity: 97 review events)*
> This PR introduces a new interface for importing output descriptors into the wallet, making it easier and more reliable to manage complex sets of addresses and watch-only wallets. This enhances the wallet's capabilities for power users and interoperability with other tools.

**Technical Details:** This change adds a new `importdescriptors` RPC or internal interface that allows users to supply a list of output descriptors for wallet management. The implementation likely involves parsing the descriptor strings, validating them, deriving relevant scriptPubKeys, and integrating them into the wallet's key and script management system, potentially leveraging existing descriptor logic but exposing it as a batch import function.

#### [#33593: guix: Use UCRT runtime for Windows release binaries](https://github.com/bitcoin/bitcoin/pull/33593)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 67 review events)*
> This PR updates the Guix build system to use the Universal C Runtime (UCRT) for Windows release binaries. This ensures compatibility with modern Windows systems and standardizes the runtime environment for better software distribution and stability.

**Technical Details:** The Guix build system, which facilitates reproducible builds for Bitcoin Core, is now configured to compile Windows binaries using the UCRT. Previously, a different or older C runtime library might have been in use. Switching to UCRT aligns with Microsoft's current recommended runtime for Windows applications, ensuring broader compatibility, resolving potential dynamic linking issues, and standardizing library function behavior across various Windows versions.

#### [#35436: wallet: Add addHDkey interface](https://github.com/bitcoin/bitcoin/pull/35436)
**Author:** [@pseudoramdom](https://github.com/pseudoramdom) | **[Wallet & User Tools]** *(Activity: 54 review events)*
> This PR introduces a new `addHDkey` RPC interface to the wallet, allowing users to import individual Hierarchical Deterministic (HD) keys. This provides more granular control over wallet management and key provisioning.

**Technical Details:** This commit implements a new `addHDkey` RPC function, expanding the wallet's key management capabilities. This RPC allows the direct import of individual extended private keys along with their derivation paths. Unlike importing full descriptors or plain private keys, this interface targets the specific use case of managing individual HD key components within the wallet's key pool, likely for specific signing or watch-only purposes, enhancing the wallet's HD features.

#### [#34566: feature: Use different datadirs for different signets](https://github.com/bitcoin/bitcoin/pull/34566)
**Author:** [@ekzyis](https://github.com/ekzyis) | **[Maintenance & Tech Debt]** *(Activity: 50 review events)*
> This PR introduces the ability to use separate data directories for different signet networks. This allows developers and testers to easily manage multiple signet environments without data conflicts, streamlining development workflows.

**Technical Details:** The implementation modifies the node's startup logic and configuration parsing to recognize distinct signet identifiers. When a specific signet is activated (e.g., via a command-line flag), the node will automatically select or create a unique data directory path, preventing accidental data mixing or corruption between different signet instances, by extending existing data directory management functionality.

#### [#34743: p2p: don't disconnect manual peers for block stalling](https://github.com/bitcoin/bitcoin/pull/34743)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Network & Privacy]** *(Activity: 25 review events)*
> This PR modifies Bitcoin Core's peer-to-peer behavior by preventing the automatic disconnection of manually configured peers when they experience block stalling. This ensures that users who explicitly add peers can maintain those connections even if the peer temporarily falls behind in block relay.

**Technical Details:** This PR adjusts the P2P connection management logic. It introduces an exception to the block-stalling disconnection rule specifically for "manual peers" (peers connected via `addnode` or `-connect`). Previously, these peers could be disconnected if they failed to provide blocks within a certain timeframe, similar to automatically discovered peers. This change exempts manual peers from this specific disconnection trigger, prioritizing user-configured connections over strict block-relay performance checks.

#### [#35975: wallet: Fix `CWalletTx` malleated transaction metadata sync](https://github.com/bitcoin/bitcoin/pull/35975)
**Author:** [@achow101](https://github.com/achow101) | **[Wallet & User Tools]** *(Activity: 23 review events)*
> This PR fixes an issue where the wallet might incorrectly handle metadata for transactions that have been malleated, ensuring the wallet accurately reflects the true state of such transactions. This prevents discrepancies and potential user confusion regarding transaction status.

**Technical Details:** This PR likely addresses a bug in the `CWalletTx` class's handling of transactions whose transaction ID (txid) might change due to malleability (e.g., pre-SegWit transactions). The fix probably ensures that when a malleated version of an existing wallet transaction is encountered, the associated metadata (like labels, purpose, etc.) is correctly transferred or updated to the new, confirmed `CWalletTx` entry, preventing data loss or incorrect display within the wallet.

#### [#34681: wallet: move rescan logic into ChainScanner and wallet/scan](https://github.com/bitcoin/bitcoin/pull/34681)
**Author:** [@Eunovo](https://github.com/Eunovo) | **[Maintenance & Tech Debt]** *(Activity: 23 review events)*
> This PR refactors the wallet's blockchain rescan logic into dedicated modules, `ChainScanner` and `wallet/scan`. This improves code organization and modularity, making the rescan process easier to maintain and extend.

**Technical Details:** The existing wallet rescan functionality, previously dispersed, has been consolidated into new `ChainScanner` and `wallet/scan` components. This involves abstracting the details of block processing and transaction matching into clear interfaces, enhancing the architectural design of the wallet's scanning capabilities and improving modularity.

#### [#36246: [32.x] Bump to 32.0rc1](https://github.com/bitcoin/bitcoin/pull/36246)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 18 review events)*

#### [#36230: wallet: Improve `HasWalletDescriptor` performance and other canonical descriptor string followups](https://github.com/bitcoin/bitcoin/pull/36230)
**Author:** [@achow101](https://github.com/achow101) | **[Performance & Optimization]** *(Activity: 16 review events)*
> This PR significantly improves the performance of the `HasWalletDescriptor` function and other descriptor string operations within the wallet component. This optimization leads to faster wallet loading, scanning, and overall responsiveness, especially for wallets with many descriptors.

**Technical Details:** The optimization targets the internal representation and comparison logic for wallet descriptors, particularly improving the efficiency of canonicalizing descriptor strings. This likely involves optimizing string hashing, caching canonical forms, or using more efficient data structures for descriptor lookups and comparisons. The changes aim to reduce the computational overhead associated with managing and querying descriptor strings within the wallet's descriptor pool, leading to quicker operations for descriptor-based wallets.

#### [#36256: guix: Update osslsigncode to 2.14](https://github.com/bitcoin/bitcoin/pull/36256)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 16 review events)*

#### [#36207: kernel: add wtxid accessor](https://github.com/bitcoin/bitcoin/pull/36207)
**Author:** [@KY-U](https://github.com/KY-U) | **[Maintenance & Tech Debt]** *(Activity: 16 review events)*

#### [#36194: kernel: expose block header Merkle root](https://github.com/bitcoin/bitcoin/pull/36194)
**Author:** [@lucasdbr05](https://github.com/lucasdbr05) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*

#### [#36297: rpc: Correct invalid OpenRPC defaults](https://github.com/bitcoin/bitcoin/pull/36297)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Wallet & User Tools]** *(Activity: 13 review events)*

#### [#36083: test: cover getrawtransaction on a stale block via txindex](https://github.com/bitcoin/bitcoin/pull/36083)
**Author:** [@arejula27](https://github.com/arejula27) | **[Maintenance & Tech Debt]** *(Activity: 13 review events)*

#### [#36081: rpc: add bestblockhash to getmininginfo](https://github.com/bitcoin/bitcoin/pull/36081)
**Author:** [@jakubtrnka](https://github.com/jakubtrnka) | **[Wallet & User Tools]** *(Activity: 11 review events)*
> This PR enhances the `getmininginfo` RPC by including the hash of the best known block. This provides miners and pool operators with immediate, critical information about the current chain tip directly from the mining information RPC.

**Technical Details:** The `getmininginfo` RPC handler is modified to query the current chain state for the block hash of the active chain tip. This `BlockHash` is then added as a new key, `bestblockhash`, to the JSON object returned by the RPC call. The implementation involves accessing the `m_chain_tip` from `CChainState` and serializing its hash into the RPC response structure.

#### [#35819: test: add coverage for untested descriptor parse error paths](https://github.com/bitcoin/bitcoin/pull/35819)
**Author:** [@azuchi](https://github.com/azuchi) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*

#### [#36285: refactor: Use static const over inline const to work around ld64 bug](https://github.com/bitcoin/bitcoin/pull/36285)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*

#### [#36260: torcontrol: Use reconnect backoff after dropped connections](https://github.com/bitcoin/bitcoin/pull/36260)
**Author:** [@fjahr](https://github.com/fjahr) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*

#### [#35961: depends: Update `boost` package to 1.92.0](https://github.com/bitcoin/bitcoin/pull/35961)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*

#### [#36093: test: ipc should reject invalid json](https://github.com/bitcoin/bitcoin/pull/36093)
**Author:** [@Sjors](https://github.com/Sjors) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*

#### [#36249: net, rpc: Asmap version improvements/follow-ups](https://github.com/bitcoin/bitcoin/pull/36249)
**Author:** [@fjahr](https://github.com/fjahr) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*

#### [#35472: test: add coverage for feebumper uncomputable cluster error path](https://github.com/bitcoin/bitcoin/pull/35472)
**Author:** [@151henry151](https://github.com/151henry151) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*

#### [#36267: [32.x] Backports for rc2](https://github.com/bitcoin/bitcoin/pull/36267)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*

#### [#36160: refactor: Minor improvements to HTTP unit tests](https://github.com/bitcoin/bitcoin/pull/36160)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*

#### [#36206: build: drop use of `OBJC_OLD_DISPATCH_PROTOTYPES`](https://github.com/bitcoin/bitcoin/pull/36206)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*

#### [#36286: crypto: Fix MuHash3072 division by itself](https://github.com/bitcoin/bitcoin/pull/36286)
**Author:** [@fjahr](https://github.com/fjahr) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*

#### [#36251: rest: add `generated` and `height` to spenttxouts JSON](https://github.com/bitcoin/bitcoin/pull/36251)
**Author:** [@0xB10C](https://github.com/0xB10C) | **[Wallet & User Tools]** *(Activity: 5 review events)*

#### [#36186: test: return False for a too-short ECDSA signature](https://github.com/bitcoin/bitcoin/pull/36186)
**Author:** [@fametrano](https://github.com/fametrano) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*

#### [#36288: build: Revert remove `cmake/script/CoverageFuzz.cmake`"](https://github.com/bitcoin/bitcoin/pull/36288)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*

#### [#36282: Update leveldb subtree to latest master](https://github.com/bitcoin/bitcoin/pull/36282)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*

#### [#36253: fuzz: Use `LIMITED_WHILE` in `connect_block`](https://github.com/bitcoin/bitcoin/pull/36253)
**Author:** [@marcofleon](https://github.com/marcofleon) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*

#### [#35849: init: don't suggest -reindex-chainstate for recovery on a pruned node](https://github.com/bitcoin/bitcoin/pull/35849)
**Author:** [@kwsantiago](https://github.com/kwsantiago) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*

#### [#36198: http: Add missing LIFETIMEBOUND annotations](https://github.com/bitcoin/bitcoin/pull/36198)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Wallet & User Tools]** *(Activity: 3 review events)*

#### [#36247: iwyu: Always keep `bitcoin-build-info.h` header](https://github.com/bitcoin/bitcoin/pull/36247)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#36233: guix: Update time-machine to `60f6956aeffa7f30285745bd0ea615e9acfc74f8`](https://github.com/bitcoin/bitcoin/pull/36233)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 20 review events this week)*

#### [#36246: [32.x] Bump to 32.0rc1](https://github.com/bitcoin/bitcoin/pull/36246)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 18 review events this week)*

#### [#36182: fees: return `block_policy` fee rate estimate when `mempool_policy` is not ready](https://github.com/bitcoin/bitcoin/pull/36182)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Wallet & User Tools]** *(Activity: 17 review events this week)*
> This PR improves Bitcoin Core's fee estimation service by providing a `block_policy` estimate when the preferred `mempool_policy` estimate is unavailable. This ensures users consistently receive a reliable fee recommendation, even during startup or network congestion.

#### [#36256: guix: Update osslsigncode to 2.14](https://github.com/bitcoin/bitcoin/pull/36256)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 16 review events this week)*

#### [#36250: rpc: clarify sendrawtransaction decode error](https://github.com/bitcoin/bitcoin/pull/36250)
**Author:** [@MrHodlX](https://github.com/MrHodlX) | **[Wallet & User Tools]** *(Activity: 15 review events this week)*

## 🗓️ Dev Meeting
Summary of the core dev IRC meeting on 2026-09-17 with 25 participants.

- Working Group Updates (QA, QML GUI, Benchmarking)
- QML GUI progress, including a new contributor and the opening of tracking issue #36289
- Release testing status and remaining review items for milestone 84
- Discussion around PR #36284 for potential inclusion in v32

**Action Items:**
- johnny9dev to use issue #36289 to share the plan for merging gui-qml and help organize/prioritize remaining issues
- Review items in milestone 84
- Consider PR #36284 for inclusion in v32

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: Bounds on chain length with BIP-54 timewarp fixes](https://delvingbitcoin.org/t/bounds-on-chain-length-with-bip-54-timewarp-fixes/2899/19)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 18

### [Re: Depots: Theft-Proof, Self-Custodial Bitcoin For Billions Of Users](https://delvingbitcoin.org/t/depots-theft-proof-self-custodial-bitcoin-for-billions-of-users/2892/11)
**Source:** Delving | **Started By:** {'username': 'JohnLaw', 'uuid': 'auto_johnlaw'} | **Messages:** 10

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/36)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 10
> Bitcoin developers are actively discussing how to integrate quantum-resistant transaction types to future-proof the network against potential quantum computer threats. This crucial work aims to secure Bitcoin transactions for decades to come by adopting advanced cryptography.

**Technical Details:** The discussion revolves around the architectural approach for introducing Post-Quantum Cryptography (PQC) transaction output types within Bitcoin. Key debate points include whether new PQC constructions should be considered entirely separate output types or deployed as 'segwit subversions' sharing an existing witness version, such as within a potential P2TRv2. While sharing a witness version, such additions would functionally act as distinct output types, highlighting the core decision on how to modularly extend Bitcoin's transaction capabilities.

### [Re: Standardizing an exposure classification for existing outputs (pre-BIP)](https://delvingbitcoin.org/t/standardizing-an-exposure-classification-for-existing-outputs-pre-bip/2866/11)
**Source:** Delving | **Started By:** {'username': 'Duncan0k', 'uuid': 'auto_duncan0k_1'} | **Messages:** 7
> Developers are discussing foundational improvements to Bitcoin's security and efficiency, specifically new transaction types and phasing out old signature methods. The current conversation clarifies how risk assessments are being factored into these critical upgrades.

**Technical Details:** Discussion continues on foundational assumptions for BIP 360 (off-chain key output) and BIP 361 (legacy signature sunset). A key clarification emerged regarding an intentionally excluded 'risk scoring' mechanism, which was previously identified as a missing presupposition for both BIPs. The debate centers on whether such a risk assessment framework is implicitly required for these security and efficiency enhancements, or if its exclusion can be justified, requiring a careful re-evaluation of the BIPs' architectural dependencies.

### [Re: Block-wide Signature Aggregation via SNARKs](https://delvingbitcoin.org/t/block-wide-signature-aggregation-via-snarks/2875/10)
**Source:** Delving | **Started By:** {'username': 'conduition', 'uuid': 'auto_conduition'} | **Messages:** 4
> Developers are exploring ways to handle larger next-generation cryptographic signatures to keep Bitcoin fast and affordable to run, ensuring its long-term resilience.

**Technical Details:** Discussions on integrating Post-Quantum (PQ) signatures address their large size impacting block propagation and archival costs. The focus has shifted to designing these implementations to be DoS-resistant, specifically by using simpler, narrowly focused 'prover' mechanisms for transaction sets. A key architectural debate centers on whether to integrate this prover functionality directly into Bitcoin Core—rather than relying on external miner software—to leverage Bitcoin Core's extensive review for enhanced security and vulnerability mitigation. This approach is seen as crucial for preventing DoS vectors.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@KY-U](https://github.com/KY-U), [@arejula27](https://github.com/arejula27), [@jakubtrnka](https://github.com/jakubtrnka), [@lucasdbr05](https://github.com/lucasdbr05)

### ✍️ Top Authors
The most active PR authors this week: [@fjahr](https://github.com/fjahr), [@fanquake](https://github.com/fanquake), [@hebasto](https://github.com/hebasto), [@achow101](https://github.com/achow101), [@sedited](https://github.com/sedited)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@maflcko](https://github.com/maflcko), [@fanquake](https://github.com/fanquake), [@willcl-ark](https://github.com/willcl-ark), [@hebasto](https://github.com/hebasto)
