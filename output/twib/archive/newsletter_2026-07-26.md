# 📰 This Week in Bitcoin (2026-07-20 to 2026-07-26)

## 📌 The TL;DR
- Significant advancements in the P2P network, including a shift to global transaction rate-limiting, increased inbound capacity for block-relay only connections, and proactive steps towards V2 transport, all aimed at improving network efficiency and resilience.
- Robust, forward-looking discussions on Bitcoin's long-term challenges, encompassing the diminishing block subsidy, potential quantum recovery threats to funds, and the ongoing exploration of protocol upgrades through initiatives like Bitcoin Inquisition.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#28463: p2p: Increase inbound capacity for block-relay only connections](https://github.com/bitcoin/bitcoin/pull/28463)
**Author:** [@mzumsande](https://github.com/mzumsande) | **[Network & Privacy]** *(Activity: 53 review events)*
> This PR increases the maximum number of inbound connections specifically for peers dedicated to block relay. This improves the efficiency and reliability of block propagation by allowing nodes to connect to more specialized block-relay peers.

**Technical Details:** The connection manager's logic is modified to adjust the limits for different types of inbound connections. Specifically, the quota for 'block-relay only' connections is increased beyond the general inbound connection limit. This allows a node to maintain more connections with peers primarily used for fast block dissemination (e.g., those that have `feefilter` enabled), optimizing the network topology for rapid block propagation without impacting the overall resource allocation for full-service peers.

#### [#35090: fuzz: add p2p_private_broadcast harness](https://github.com/bitcoin/bitcoin/pull/35090)
**Author:** [@frankomosh](https://github.com/frankomosh) | **[Network & Privacy]** *(Activity: 44 review events)*
> This PR introduces a new fuzzer harness for the P2P private broadcast mechanism, improving the robustness and security of how private transactions are handled. This helps identify and prevent potential vulnerabilities in the network layer.

**Technical Details:** A new fuzzer target, `p2p_private_broadcast`, is added to the fuzzing framework. This harness specifically exercises the code paths related to broadcasting private transactions over the peer-to-peer network. It feeds randomized or malformed inputs into functions responsible for message construction, serialization, and deserialization of private broadcast messages, aiming to uncover crashes, asserts, or other undefined behaviors, thereby hardening the network code.

#### [#32800: rpc: Distinguish between vsize and sigop adjusted mempool vsize](https://github.com/bitcoin/bitcoin/pull/32800)
**Author:** [@musaHaruna](https://github.com/musaHaruna) | **[Wallet & User Tools]** *(Activity: 44 review events)*
> This update to the RPC interface provides more granular detail on transaction sizes within the mempool. It helps users understand the precise impact of signature operations on a transaction's effective size for fee estimation.

**Technical Details:** This PR enhances RPC responses by exposing both the raw virtual size (vsize) and a signature-operation-adjusted virtual size for transactions in the mempool. Previously, RPCs might only show a single effective size. This change involves modifying the mempool entry data structures and associated RPC serialization logic to include this additional metric, offering more insight into transaction weight calculations specifically influenced by sigops in fee selection.

#### [#34628: p2p: Replace per-peer transaction rate-limiting with global rate limits](https://github.com/bitcoin/bitcoin/pull/34628)
**Author:** [@ajtowns](https://github.com/ajtowns) | **[Network & Privacy]** *(Activity: 42 review events)*
> This Pull Request replaces the per-peer transaction rate-limiting mechanism with a single, global rate limit for incoming transactions. This enhances the network's resilience against transaction flooding attacks and ensures fairer resource allocation across all connected peers.

**Technical Details:** Instead of each connected peer having its own independent token bucket for transaction relay, a single global token bucket is introduced. All incoming transactions from any peer must now draw tokens from this shared global pool before being processed. This prevents a single or small group of malicious peers from exhausting individual peer limits while allowing the node operator to set an overall transaction ingest rate limit, simplifying DoS protection logic and providing a more consistent policy across the P2P network.

#### [#34683: rpc: support a formal description of our JSON-RPC interface](https://github.com/bitcoin/bitcoin/pull/34683)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Wallet & User Tools]** *(Activity: 40 review events)*
> This PR introduces a formal way to describe Bitcoin Core's JSON-RPC interface, making it easier for developers to build applications that interact with the node. It improves discoverability and integration for external tools and services.

**Technical Details:** This PR implements support for generating or providing a machine-readable, formal description of all available JSON-RPC methods, their parameters, and return types. This likely involves integrating a schema generation mechanism (e.g., OpenAPI/Swagger or a custom specification) directly into the RPC server. It allows clients to dynamically understand the RPC capabilities without manual parsing of documentation, enhancing client development workflows and reducing integration effort.

#### [#32764: guix: Build for macOS using LLVM toolchain only](https://github.com/bitcoin/bitcoin/pull/32764)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 36 review events)*
> This PR streamlines the macOS build process within the Guix environment by exclusively using the LLVM toolchain. This simplifies build configurations and ensures more consistent and reliable builds for macOS users.

**Technical Details:** Previously, Guix builds for macOS might have implicitly or explicitly attempted to use other compilers or toolchain components. This change enforces the use of the LLVM toolchain (Clang) for all macOS builds within Guix, aligning with standard macOS development practices. It involves adjusting Guix packages and build scripts to specify the LLVM toolchain, removing potential conflicts or unexpected behavior that could arise from mixed toolchains.

#### [#35261: guix: disable LTO in GCC](https://github.com/bitcoin/bitcoin/pull/35261)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 35 review events)*
> This PR improves the build process for Bitcoin Core by disabling Link-Time Optimization (LTO) when using GCC with Guix, resolving compilation issues. It ensures reliable and consistent builds for specific environments.

**Technical Details:** Guix builds for Bitcoin Core using GCC were encountering errors when LTO was enabled. This change introduces a configuration tweak within the Guix build system to explicitly disable LTO for GCC. This circumvents toolchain-specific issues that LTO introduces, ensuring successful compilation without compromising critical optimizations that would typically be handled by higher-level build flags or alternative compilers.

#### [#35215: coins: use SipHash-1-3-UJ for CCoinsMap keys](https://github.com/bitcoin/bitcoin/pull/35215)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Performance & Optimization]** *(Activity: 31 review events)*
> This PR enhances the performance and security of the UTXO set cache by switching to SipHash-1-3-UJ for `CCoinsMap` keys. This improves map efficiency and provides better protection against algorithmic complexity attacks.

**Technical Details:** The `CCoinsMap`, which stores unspent transaction outputs (UTXOs) in memory, previously used a different hashing scheme for its keys. This change replaces it with SipHash-1-3-UJ, a keyed cryptographic hash function. This improves hash distribution, reducing collision rates and enhancing the average-case performance of map lookups. Crucially, it also makes the hash map more resilient to targeted denial-of-service attacks that exploit predictable hash functions by carefully crafting inputs to cause many collisions.

#### [#33014: rpc: Fix internal bug in descriptorprocesspsbt when encountering invalid signatures](https://github.com/bitcoin/bitcoin/pull/33014)
**Author:** [@b-l-u-e](https://github.com/b-l-u-e) | **[Wallet & User Tools]** *(Activity: 30 review events)*
> This update fixes an internal bug in the `descriptorprocesspsbt` RPC that previously caused issues when processing Partially Signed Bitcoin Transactions (PSBTs) with invalid signatures. This ensures the RPC functions robustly even with malformed input, preventing unexpected errors.

**Technical Details:** The PR addresses a specific internal bug within the `descriptorprocesspsbt` RPC command. Previously, encountering an invalid signature during PSBT processing could lead to an internal error or incorrect state handling. The fix involves refining the error handling logic and signature validation routines within the `descriptorprocesspsbt` function, ensuring that invalid signatures are gracefully rejected or handled without triggering an internal assertion or crash, thus improving RPC stability and reliability.

#### [#34672: mining: add reason/debug to `submitSolution` and unify with `submitBlock`](https://github.com/bitcoin/bitcoin/pull/34672)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Wallet & User Tools]** *(Activity: 22 review events)*
> This PR improves the debuggability and consistency of the mining interface by adding reason and debug messages to `submitSolution` and unifying its behavior with `submitBlock`. This provides clearer feedback for miners and tools interacting with Bitcoin Core.

**Technical Details:** The `submitSolution` internal RPC, which is used by mining software, is enhanced to return more informative `reason` and `debug` strings, similar to the existing `submitBlock` RPC. This involves modifying the internal block submission logic to populate these fields consistently, allowing calling applications to understand why a solution was accepted or rejected. This standardization reduces ambiguity and aids in diagnosing mining issues.

#### [#35537: guix: split builds into Linux, Linux GUI and macOS/Windows](https://github.com/bitcoin/bitcoin/pull/35537)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 22 review events)*
> This PR refactors the Guix build system to separate builds into distinct Linux, Linux GUI, and macOS/Windows targets. This organizational improvement simplifies the build process and enhances maintainability for different operating systems.

**Technical Details:** The existing monolithic Guix build script is broken down into more granular components, specifically `guix/build-linux.scm`, `guix/build-linux-gui.scm`, and `guix/build-macos-windows.scm`. This modularization allows for platform-specific build configurations and dependencies to be managed independently, reducing complexity and potential conflicts. It also makes it easier to add new platforms or update toolchains in the future.

#### [#35746: ci: Test build directory path with spaces](https://github.com/bitcoin/bitcoin/pull/35746)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 15 review events)*
> This PR adds a new test to the continuous integration system to ensure Bitcoin Core can be built correctly even when the build directory path contains spaces. This improves the robustness of the build system across various environments and operating systems.

**Technical Details:** This PR introduces a new job or step into the CI pipeline (e.g., GitLab CI, GitHub Actions) that specifically attempts to build Bitcoin Core within a directory path containing spaces. This test aims to identify and prevent issues related to shell escaping or path handling in the build scripts (e.g., `autotools`, `configure`, `make`). It ensures that the build process is resilient to common filesystem path complexities, preventing build failures for users.

#### [#35766: p2p: Assume v2transport for addresses from seeds](https://github.com/bitcoin/bitcoin/pull/35766)
**Author:** [@mzumsande](https://github.com/mzumsande) | **[Network & Privacy]** *(Activity: 13 review events)*
> This change improves how new nodes connect to the Bitcoin network by assuming a more modern transport protocol for addresses obtained from network seeds. This streamlines initial peer discovery and connection stability by prioritizing v2transport.

**Technical Details:** This PR modifies the P2P layer to default to assuming `v2transport` compatibility for peers discovered via DNS seeds or hardcoded seeds. Instead of requiring a full protocol handshake to determine transport version, nodes will proactively attempt `v2transport` connections. This change impacts the initial connection attempts and peer negotiation logic, potentially accelerating the establishment of advanced transport connections with well-behaved peers on startup.

#### [#35320: key: validate BIP32 seed length in CExtKey::SetSeed](https://github.com/bitcoin/bitcoin/pull/35320)
**Author:** [@muhahahmad68](https://github.com/muhahahmad68) | **[Security & Consensus]** *(Activity: 11 review events)*
> This PR enhances the security and robustness of hierarchical deterministic (HD) key generation by adding strict validation for BIP32 seed lengths. It prevents invalid or malicious seeds from creating corrupted extended keys.

**Technical Details:** The `CExtKey::SetSeed` function, responsible for initializing an extended key from a seed, now includes a check to ensure the provided seed length falls within the valid range specified by BIP32 (128 to 512 bits). If the length is invalid, the function will return `false`, preventing the creation of an invalid extended key. This hardens the system against potential misuse or errors in seed generation.

#### [#35783: chainparams: remove my testnet3 seed](https://github.com/bitcoin/bitcoin/pull/35783)
**Author:** [@Sjors](https://github.com/Sjors) | **[Network & Privacy]** *(Activity: 10 review events)*

#### [#35727: blockencodings: fix extra transaction count](https://github.com/bitcoin/bitcoin/pull/35727)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Network & Privacy]** *(Activity: 10 review events)*

#### [#35792: refactor: Make all `const static` class members `constexpr`](https://github.com/bitcoin/bitcoin/pull/35792)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*

#### [#35076: doc: clarify pruning impact on wallet sync](https://github.com/bitcoin/bitcoin/pull/35076)
**Author:** [@MemeticMoney](https://github.com/MemeticMoney) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR updates the documentation to clearly explain the implications of using a pruned Bitcoin Core node on wallet synchronization. It helps users understand how pruning affects the wallet's ability to scan for transactions.

**Technical Details:** The PR modifies existing documentation (e.g., in `doc/`) to provide explicit details on the interaction between a pruned blockchain data directory and wallet operation. It clarifies that a wallet on a pruned node may be unable to scan for transactions predating the pruning boundary, impacting its initial sync and recovery capabilities. This update enhances clarity for users managing node resources.

#### [#35794: doc: Discourage adding AI agents as commit (co)-authors](https://github.com/bitcoin/bitcoin/pull/35794)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*

#### [#34808: cmake, translation: Use native Qt TS file as source for translations on Transifex](https://github.com/bitcoin/bitcoin/pull/34808)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR streamlines the translation process by using native Qt TS files directly as the source for translations on Transifex. It simplifies the localization workflow, making it easier for translators to contribute accurate and up-to-date translations for the Bitcoin Core UI.

**Technical Details:** The change involves modifying the CMake build system to directly generate and consume native Qt TS (Translation Source) files for Transifex integration, instead of relying on an intermediate Gettext PO format. This simplifies the toolchain by eliminating a conversion step, ensuring that the translation platform directly works with the preferred Qt format. This reduces potential for format-related issues and aligns the build process more closely with standard Qt localization practices.

#### [#35767: fuzz: Avoid dangling prevoutfetch threads after AFL fork](https://github.com/bitcoin/bitcoin/pull/35767)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*

#### [#35490: test: cover unused mempool space in coins cache limit](https://github.com/bitcoin/bitcoin/pull/35490)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR introduces a new test case to verify how the mempool's unused allocated space interacts with the coins cache limit. It ensures correct memory management by accurately accounting for resources, enhancing node stability.

**Technical Details:** The PR adds a functional or unit test that specifically exercises scenarios where the transaction memory pool has reserved memory that is currently unutilized. The test asserts that the logic calculating the maximum size for the UTXO (coins) cache correctly considers this unused mempool space, preventing potential memory overcommitment and ensuring that memory limits are respected across different components.

#### [#35775: scripted-diff: Use C.UTF-8 locale in Guix scripts](https://github.com/bitcoin/bitcoin/pull/35775)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*

#### [#35721: lint: drop most remaining default Ruff rule ignores](https://github.com/bitcoin/bitcoin/pull/35721)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*

#### [#35736: bitcoin-util: replace netmagic command with getchainparams command](https://github.com/bitcoin/bitcoin/pull/35736)
**Author:** [@ekzyis](https://github.com/ekzyis) | **[Wallet & User Tools]** *(Activity: 6 review events)*
> This PR replaces the `netmagic` command in `bitcoin-util` with a more descriptive `getchainparams` command, providing clearer information about the network parameters Bitcoin Core is operating on. This enhances user clarity and improves the utility's interface for developers.

**Technical Details:** This PR renames and potentially refactors the `bitcoin-util netmagic` command to `bitcoin-util getchainparams`. The new command will provide comprehensive details about the currently active chain's network parameters, which might include more than just the network magic byte (e.g., genesis block hash, P2P port, etc.). This involves modifying the `bitcoin-util` command-line parsing and output logic, ensuring a more informative output to the user.

#### [#35694: clusterlin: minor SFL optimizations](https://github.com/bitcoin/bitcoin/pull/35694)
**Author:** [@sipa](https://github.com/sipa) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*

#### [#35664: test: add CLTV and CHECK(MULTI)SIGVERIFY failure-path vectors to script_tests.json](https://github.com/bitcoin/bitcoin/pull/35664)
**Author:** [@azuchi](https://github.com/azuchi) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*

#### [#35681: test: cover disconnect on private broadcast peer with relay=false](https://github.com/bitcoin/bitcoin/pull/35681)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*

#### [#35781: Update secp256k1 subtree to latest master](https://github.com/bitcoin/bitcoin/pull/35781)
**Author:** [@fanquake](https://github.com/fanquake) | **[Performance & Optimization]** *(Activity: 5 review events)*
> This PR updates Bitcoin Core's internal `secp256k1` cryptographic library to its latest master branch version. This ensures the project benefits from recent performance optimizations, bug fixes, and robustness improvements in critical cryptographic operations.

**Technical Details:** The PR performs a `git subtree update` operation for the `src/secp256k1/` directory, pulling the latest commits from the upstream `secp256k1` repository. This integration often includes advancements in assembly implementations for elliptic curve operations, fixes for edge cases, and general code hygiene improvements, which directly enhance the efficiency and security posture of Bitcoin Core's signature verification and key handling.

#### [#35782: doc: fix outdated i2p URLs in comments](https://github.com/bitcoin/bitcoin/pull/35782)
**Author:** [@nebula-21](https://github.com/nebula-21) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*

#### [#35770: net: Simplify `AddressPosition` comparitor](https://github.com/bitcoin/bitcoin/pull/35770)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR simplifies the code responsible for comparing network address positions, making the networking code cleaner and easier to maintain. This improves code readability for future development efforts without changing behavior.

**Technical Details:** This PR refactors the comparison logic for `AddressPosition` objects within the networking subsystem. It likely identifies redundant or overly complex conditional statements in the existing `operator<` or comparison function and simplifies them, potentially by leveraging standard library algorithms or by rewriting the logic more concisely. This is primarily a code quality improvement that does not change external behavior but enhances internal maintainability and clarity.

#### [#35709: depends: Update Qt to 6.8.4](https://github.com/bitcoin/bitcoin/pull/35709)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*

#### [#35769: depends, zeromq: Apply upstream patch](https://github.com/bitcoin/bitcoin/pull/35769)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This update integrates an upstream patch for the ZeroMQ library, a third-party dependency, ensuring Bitcoin Core benefits from the latest fixes and improvements from the ZeroMQ project. It helps maintain the stability and security of external components used for notifications.

**Technical Details:** This PR updates the `depends` system, specifically for the ZeroMQ (ZMQ) library, by incorporating a specific patch from the upstream ZMQ project. This involves modifying the `depends/packages/zeromq.mk` build recipe and potentially the patched source files. The patch likely addresses a bug, improves compatibility, or fixes a security vulnerability within ZMQ, impacting components of Bitcoin Core that use ZMQ for notification services.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#35261: guix: disable LTO in GCC](https://github.com/bitcoin/bitcoin/pull/35261)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 19 review events this week)*
> This PR improves the build process for Bitcoin Core by disabling Link-Time Optimization (LTO) when using GCC with Guix, resolving compilation issues. It ensures reliable and consistent builds for specific environments.

#### [#35766: p2p: Assume v2transport for addresses from seeds](https://github.com/bitcoin/bitcoin/pull/35766)
**Author:** [@mzumsande](https://github.com/mzumsande) | **[Network & Privacy]** *(Activity: 13 review events this week)*
> This change improves how new nodes connect to the Bitcoin network by assuming a more modern transport protocol for addresses obtained from network seeds. This streamlines initial peer discovery and connection stability by prioritizing v2transport.

#### [#35746: ci: Test build directory path with spaces](https://github.com/bitcoin/bitcoin/pull/35746)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 13 review events this week)*
> This PR adds a new test to the continuous integration system to ensure Bitcoin Core can be built correctly even when the build directory path contains spaces. This improves the robustness of the build system across various environments and operating systems.

#### [#35754: ci: pin and verify external inputs](https://github.com/bitcoin/bitcoin/pull/35754)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 12 review events this week)*

#### [#35675: mining: add block template manager](https://github.com/bitcoin/bitcoin/pull/35675)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Maintenance & Tech Debt]** *(Activity: 11 review events this week)*

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: A Bitcoin-native LLM: dataset, architecture and open questions](https://delvingbitcoin.org/t/a-bitcoin-native-llm-dataset-architecture-and-open-questions/2550/22)
**Source:** Delving | **Started By:** {'username': 'thomas suau', 'uuid': 'auto_thomas_suau'} | **Messages:** 5
> Discussions are underway to systematically organize Bitcoin's extensive public knowledge base, encompassing its foundational documents and historical communications. This initiative aims to make Bitcoin's principles and evolution more accessible and better preserved for everyone.

**Technical Details:** Developers are exploring architectural approaches for structuring Bitcoin's rich open-source knowledge base, which spans BIPs, mailing list archives, and on-chain data. MrHash has outlined a hierarchical design starting with a 'Constitutional Tier' for foundational elements like the whitepaper and genesis block, followed by a 'Drafting Record' for historical communications such as Satoshi's emails. The debate centers on establishing a comprehensive and logical framework to effectively catalog and present critical historical and developmental information.

### [Re: [Research] A clockless vardiff strands a slowing miner](https://delvingbitcoin.org/t/research-a-clockless-vardiff-strands-a-slowing-miner/2718/8)
**Source:** Delving | **Started By:** {'username': 'Eric Price', 'uuid': 'auto_eric_price'} | **Messages:** 4
> Developers are exploring advanced methods to make Bitcoin mining more efficient and fair, especially for operations with fluctuating hardware performance. The goal is to dynamically adjust the work difficulty for each miner, ensuring they always contribute optimally and maximize their earnings without wasted effort.

**Technical Details:** The discussion focuses on architecting an adaptive, clockless vardiff controller to prevent "stranding" for miners with variable hashrates. Anthony Towns proposes proxy-level vardiff adjustments using a Y/X multiplier based on instructed hashrate changes and connection activity. Eric Price connects this to a prior controller proposal's "five rules," identifying specific adaptive mechanisms like halving vardiff after 30 seconds of inactivity ("ease-on-silence"). The ongoing work involves designing a robust system that dynamically responds to real-time miner performance and explicit instructions for improved mining efficiency.

### [Re: Addressing the Diminishing Block Subsidy](https://delvingbitcoin.org/t/addressing-the-diminishing-block-subsidy/2640/27)
**Source:** Delving | **Started By:** {'username': 'Sho', 'uuid': 'auto_sho'} | **Messages:** 3
> To secure Bitcoin's future and ensure reliable transactions, developers are actively discussing how to sustainably reward miners as the block reward naturally diminishes, guaranteeing the network's long-term strength.

**Technical Details:** The ongoing discussion addresses the critical challenge of maintaining adequate miner incentives and network security as the block subsidy reduces. While various proposals for supplementing miner revenue are being considered, there is strong pushback against inflationary mechanisms due to their fundamental economic drawbacks. The community recognizes the urgent need for a robust, long-term transition strategy that shifts miner economics towards a sustainable fee-driven model without compromising Bitcoin's architecture or monetary policy. Further technical analysis is focused on viable, non-inflationary solutions.

### [Bitcoin Inquisition 29.4](https://delvingbitcoin.org/t/bitcoin-inquisition-29-4/2739/1)
**Source:** Delving | **Started By:** {'username': 'Anthony Towns', 'uuid': 'can_anthony_towns'} | **Messages:** 1
> A new version of Bitcoin Inquisition, a testing ground for future Bitcoin features, is now available. This allows developers to explore and experiment with potential advancements before they are integrated into Bitcoin Core.

**Technical Details:** Bitcoin Inquisition 29.4, an experimental fork of Bitcoin Core, has been released, offering developers access to the latest proposed protocol changes and testing frameworks. This version likely integrates recent upstream Bitcoin Core updates along with specialized patches for features under active development, such as novel opcodes or mempool policy adjustments. Developers testing future Bitcoin protocol enhancements should update to 29.4 to leverage the newest experimental capabilities and provide feedback. The announcement focuses on availability rather than specific technical debates within this release.

### [Re: [bitcoindev] Quantum Recovery Of Hashed Address Secured Coins
 With No Confiscatory Risk](https://gnusha.org/pi/bitcoindev/gJnvMBYdwA6pJzPtnsuBLrymr9Vs1xQ_xejRrEvET1Tz-FJZ6B_b5z0gaT25Fz2RG1N--cVZikyUclGDoLouOTRNVocOTn-fuBuwiyMCs54=@protonmail.com)
**Source:** Mailing List | **Started By:** {'username': 'Shinobi', 'uuid': 'can_shinobi'} | **Messages:** 1
> Developers are iterating on an existing proposal, clarifying its core ideas and underlying assumptions to make it easier to understand and evaluate. This refinement ensures that potential improvements for Bitcoin Core are communicated effectively.

**Technical Details:** A developer has published a rewritten version of an original message, aiming to address specific feedback received from another contributor and to more precisely articulate the implicit assumptions of the overall technical concept. This revision emphasizes clarity and foundational premises, which is crucial for robust architectural discussion and evaluation within the Bitcoin Core development mailing list, ensuring all aspects of the idea are well-understood before deeper technical analysis or potential integration.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@MemeticMoney](https://github.com/MemeticMoney), [@muhahahmad68](https://github.com/muhahahmad68), [@nebula-21](https://github.com/nebula-21)

### ✍️ Top Authors
The most active PR authors this week: [@hebasto](https://github.com/hebasto), [@fanquake](https://github.com/fanquake), [@willcl-ark](https://github.com/willcl-ark), [@w0xlt](https://github.com/w0xlt), [@rustaceanrob](https://github.com/rustaceanrob)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@fanquake](https://github.com/fanquake), [@l0rinc](https://github.com/l0rinc), [@hebasto](https://github.com/hebasto), [@achow101](https://github.com/achow101)
