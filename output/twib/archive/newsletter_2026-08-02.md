# 📰 This Week in Bitcoin (2026-07-27 to 2026-08-02)

## 📌 The TL;DR
- Significant ongoing discussions around future protocol enhancements, including proposals for segregated data carriage, advanced Taproot-based scripting capabilities (CISA), and long-term cryptographic security with Post-Quantum Cryptography (PQC).
- Continued focus on core codebase modernization and robustness, demonstrated by improvements in inter-process communication, efficient string handling, kernel stability for edge cases, and enhancements to the build system and testing infrastructure.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#35084: ipc: Add nonunix platform support](https://github.com/bitcoin/bitcoin/pull/35084)
**Author:** [@ryanofsky](https://github.com/ryanofsky) | **[Strategic Initiatives]** *(Activity: 30 review events)*
> This PR extends Bitcoin Core's Inter-Process Communication (IPC) system to support non-Unix platforms. This is a foundational step towards enabling more robust integration and communication capabilities on operating systems like Windows.

**Technical Details:** This PR introduces an abstraction layer for IPC mechanisms, allowing platform-specific implementations for non-Unix environments. It likely involves conditional compilation to provide distinct IPC primitives (e.g., named pipes on Windows instead of Unix domain sockets). This architectural change ensures a consistent IPC interface can be exposed across diverse operating systems, facilitating feature development that leverages inter-process communication beyond Linux/macOS.

#### [#35551: test: add interface_gui.py to test bitcoin-qt startup](https://github.com/bitcoin/bitcoin/pull/35551)
**Author:** [@ryanofsky](https://github.com/ryanofsky) | **[Maintenance & Tech Debt]** *(Activity: 18 review events)*
> Adds a new functional test, `interface_gui.py`, designed to verify that the `bitcoin-qt` graphical user interface initializes and starts up correctly. This provides automated test coverage for the GUI's entry point, preventing broken releases.

**Technical Details:** This PR introduces a Python-based functional test that spawns the `bitcoin-qt` executable as a subprocess within the test harness. It utilizes basic interface interaction or startup checking to confirm that the Qt event loop starts and the main window is initialized. It establishes a baseline automated test to detect startup crashes, library load failures, or platform-independent Qt initialization bugs.

#### [#35821: guix: followups to #35537](https://github.com/bitcoin/bitcoin/pull/35821)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*
> Implements follow-up cleanups and minor fixes to the Guix reproducible build system configurations. This ensures the deterministic build environment remains maintainable and error-free.

**Technical Details:** This PR refines the Scheme scripts and package definitions within the `contrib/guix` directory. It resolves minor discrepancies, optimizes build inputs, and ensures strict parity with changes from preceding PRs. These changes stabilize the Guix container build environments used for producing deterministic release binaries.

#### [#35606: script: qa: Improve `Key::Fingerprint` type safety](https://github.com/bitcoin/bitcoin/pull/35606)
**Author:** [@davidgumberg](https://github.com/davidgumberg) | **[Maintenance & Tech Debt]** *(Activity: 10 review events)*
> This PR improves the type safety for `Key::Fingerprint` within the scripting quality assurance tests. It ensures more robust and less error-prone handling of key fingerprints in test environments.

**Technical Details:** The change involves refactoring `Key::Fingerprint` usage in `script_tests.cpp` to employ more explicit type handling and conversions. This reduces reliance on implicit conversions that could lead to subtle bugs or misinterpretations of fingerprint values. By enforcing stricter type discipline, the PR makes the test code clearer, more maintainable, and less susceptible to type-related errors when dealing with cryptographic key identifiers.

#### [#35795: build: set CMAKE_VISIBILITY_INLINES_HIDDEN in REDUCE_EXPORTS](https://github.com/bitcoin/bitcoin/pull/35795)
**Author:** [@fanquake](https://github.com/fanquake) | **[Performance & Optimization]** *(Activity: 9 review events)*
> Configures the CMake build system to hide inline function symbols when the export reduction option is enabled, resulting in smaller binary sizes. This helps optimize the footprint of compiled Bitcoin Core binaries and libraries.

**Technical Details:** This PR sets the `CMAKE_VISIBILITY_INLINES_HIDDEN` property when the `REDUCE_EXPORTS` compile option is active. By setting this flag, inline class member functions are compiled with hidden visibility by default, preventing them from being exported in the shared library dynamic symbol table. This reduces the binary size, improves dynamic linking times, and minimizes the public API surface of built libraries like `libbitcoinkernel`.

#### [#35787: init, rpc: ignore empty addnode values](https://github.com/bitcoin/bitcoin/pull/35787)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Network & Privacy]** *(Activity: 9 review events)*
> Modifies the node initialization and RPC interfaces to ignore empty or blank values passed to the `addnode` configuration parameter. This prevents the node from attempting to resolve or connect to invalid empty addresses during startup or runtime.

**Technical Details:** This PR updates the parsing logic in both the command-line configuration parser and the `addnode` RPC handler to sanitize input strings. It introduces a validator that filters out empty or whitespace-only strings before they reach the connection manager. This avoids redundant DNS resolution attempts and prevents assertions or errors inside the P2P networking layer.

#### [#35553: test: Add missing test case for getdata requests from blocks-only peers](https://github.com/bitcoin/bitcoin/pull/35553)
**Author:** [@roqqit](https://github.com/roqqit) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> Introduces a new functional test case targeting the behavior of block requests (getdata) coming from peers operating in block-only mode. This ensures the node handles blocks-only P2P constraints reliably and behaves as specified.

**Technical Details:** The functional test framework is expanded with a test case that configures a mock peer under blocks-only negotiation. The test verifies that the node properly services block requests via `getdata` while ignoring or rejecting transactions. It asserts correct node behavior under blocks-only constraints by checking the outbound message queue of the node under test.

#### [#35692: addrman: remove unreachable tried-collision branch](https://github.com/bitcoin/bitcoin/pull/35692)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR refactors the address manager by removing a code branch related to "tried-collisions" that was found to be unreachable. It cleans up the codebase, improving maintainability and reducing unnecessary complexity.

**Technical Details:** The change identifies and eliminates a specific conditional code path within the `addrman` component's logic for handling 'tried-collisions'. Through static or dynamic analysis, it was determined that this particular branch could never be reached under valid program execution flows. Removing this dead code simplifies the control flow, potentially reduces the compiled binary size, and enhances code clarity for future development and auditing efforts within the address manager.

#### [#35753: kernel: handle null mempool on chainstate deletion](https://github.com/bitcoin/bitcoin/pull/35753)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Strategic Initiatives]** *(Activity: 4 review events)*
> This PR enhances the Bitcoin Core kernel by safely handling cases where the mempool might be null during chainstate deletion. It improves the node's robustness, preventing potential crashes in specific shutdown or error scenarios.

**Technical Details:** This change introduces a null-pointer check for `m_mempool` within the `~ChainstateManager()` destructor. If `m_mempool` is null, the code now defensively skips any operations that would attempt to access its members, such as calling `removeForReorg`. This prevents potential null pointer dereferences and ensures graceful shutdown or cleanup even when the mempool object has not been initialized or has been prematurely destroyed, improving the stability of the kernel component.

#### [#35828: util: Make LineReader consistently use string_view](https://github.com/bitcoin/bitcoin/pull/35828)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Performance & Optimization]** *(Activity: 4 review events)*
> Optimizes the `LineReader` utility by consistently utilizing `std::string_view` for string parsing and line splitting operations. This reduces unnecessary memory allocations and copying when processing text-based inputs.

**Technical Details:** The `LineReader` class is refactored to parse and return `std::string_view` objects rather than instantiating new `std::string` instances for each read line. By referencing substrings directly within the existing buffer, it minimizes heap allocations during text-processing operations like config parsing. The internal parsing state is updated to maintain bounds pointers, improving CPU cache locality and lowering allocator pressure.

#### [#35810: guix: Drop unused `(guix licenses)` import from `manifest_build.scm`](https://github.com/bitcoin/bitcoin/pull/35810)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> Simplifies the Guix build manifest by removing an unused module import for license definitions. This keeps the build configuration scripts clean and free of dead code.

**Technical Details:** The `manifest_build.scm` file is modified to remove the unused `(guix licenses)` import namespace. Since none of the package or manifest definitions in this file reference bindings from the licenses module, removing it eliminates dead code. This cleanup slightly reduces compilation warnings and execution context clutter during Guix build runs.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#34566: feature: Use different datadirs for different signets](https://github.com/bitcoin/bitcoin/pull/34566)
**Author:** [@ekzyis](https://github.com/ekzyis) | **[Maintenance & Tech Debt]** *(Activity: 11 review events this week)*
> This PR introduces the ability to use separate data directories for different signet networks. This allows developers and testers to easily manage multiple signet environments without data conflicts, streamlining development workflows.

#### [#35821: guix: followups to #35537](https://github.com/bitcoin/bitcoin/pull/35821)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 11 review events this week)*
> Implements follow-up cleanups and minor fixes to the Guix reproducible build system configurations. This ensures the deterministic build environment remains maintainable and error-free.

#### [#35531: txindex: hash keys and pack positions to reduce disk usage](https://github.com/bitcoin/bitcoin/pull/35531)
**Author:** [@andrewtoth](https://github.com/andrewtoth) | **[Performance & Optimization]** *(Activity: 10 review events this week)*
> Optimizes the transaction index (`txindex`) database by hashing keys and packing storage positions. This significantly reduces the disk space required to maintain a full transaction index on node operators' machines.

#### [#35820: refactor: keep duration calculations typed](https://github.com/bitcoin/bitcoin/pull/35820)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 9 review events this week)*
> Refactors duration and time calculations across the codebase to use strongly typed standard library types instead of raw integers. This eliminates a common class of bugs related to unit conversions and improves code readability.

#### [#35838: qa: Enable `interface_gui.py` on macOS](https://github.com/bitcoin/bitcoin/pull/35838)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 7 review events this week)*
> Enables the `interface_gui.py` functional test on macOS platforms, ensuring the Bitcoin Core Qt interface starts up correctly on Apple operating systems. This improves overall test coverage and prevents GUI-related regressions on macOS.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [BIP Draft] Segregated Data: a prunable, script-isolated block region for data carriage](https://delvingbitcoin.org/t/bip-draft-segregated-data-a-prunable-script-isolated-block-region-for-data-carriage/2641/48)
**Source:** Delving | **Started By:** {'username': 'MrHash', 'uuid': 'auto_mrhash'} | **Messages:** 7
> Bitcoin Core developers are advancing a proposal called Segregated Data (SegData) to enable secure and flexible storage of various types of data on the blockchain. This initiative aims to open doors for new applications without impacting Bitcoin's primary function of value transfer, ensuring network efficiency while providing robust data capabilities.

**Technical Details:** The discussion centers on Segregated Data (SegData), a proposed soft-fork to introduce an on-chain region for arbitrary data. A core architectural debate revolves around the tension between making data storage optional (relying on 'gentlemen's agreements') versus the perceived need for on-chain data to inherently *force* node storage. A 'newer proposal' is being discussed, which specifically aims to fix previous reorg risks by making SegData optional at every depth while keeping consensus validation identical, directly addressing concerns from AJ Towns and moving towards a more robust implementation.

### [Re: BIP110 Monitor & Simulator](https://delvingbitcoin.org/t/bip110-monitor-simulator/2746/6)
**Source:** Delving | **Started By:** {'username': 'orangesurf', 'uuid': 'auto_orangesurf'} | **Messages:** 5
> A new monitoring tool is actively tracking miner support for a proposed Bitcoin improvement (BIP110) to help ensure smooth network evolution. This helps us visualize potential changes and maintain network stability as new features are considered for adoption.

**Technical Details:** A BIP110 situation monitor has been developed to track live miner signaling data and compare chain tips between a standard Core node and a BIP110 node, acting as an early fork detection mechanism. The current discussion speculates on whether vocal mining proponents of BIP110 will capitulate or persist in signaling and eventually find a conforming block. This ongoing observation is critical for assessing the real-world network impact and miner consensus around BIP110's activation, informing decisions on potential protocol upgrades.

### [Re: Onion Message Jamming in the Lightning Network](https://delvingbitcoin.org/t/onion-message-jamming-in-the-lightning-network/2414/23)
**Source:** Delving | **Started By:** {'username': 'Erick λ', 'uuid': 'auto_erick'} | **Messages:** 5
> We're enhancing the reliability and security of private messages on the network, making sure they get through without being spammed. This improves how users interact with advanced Bitcoin features that rely on secure, private communication.

**Technical Details:** The discussion evaluates the implementation of 'good reputation' for Onion Message (OM) relaying, as recommended by BOLT 4 to counter inherent unreliability and abuse. Key architectural debates include whether OM forwarding should be strictly limited to channel peers, or if a broader relaying model is needed. Developers are also examining if a new, dedicated reputation metric for OMs is required, or if existing channel-jamming protection metrics can be adapted to secure message forwarding effectively.

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/4)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 3
> Bitcoin developers are consolidating discussions on how to prepare the network for potential future quantum threats, ensuring its long-term security and the safety of user funds against advanced attacks.

**Technical Details:** The community is consolidating discussions on integrating Post-Quantum Cryptography (PQC) transaction output types into Bitcoin, which requires designing new script constructions and validation rules to secure transactions against future quantum attacks. The architectural challenge involves balancing compatibility, efficiency, and security considerations within the existing protocol. Recent input emphasizes the unpredictable and potentially sudden nature of 'Q-day,' highlighting the need for proactive, well-defined PQC solutions rather than a reactive, milestone-driven approach to safeguard user funds.

### [Re: [bitcoindev] Re: BIP draft: CISA for Taproot Key Path Spends](https://gnusha.org/pi/bitcoindev/WwTIo2TK4GMzl9tDrkf1MWag8LiZghpjWpBEgSzZ_JG2kl12SNjR_sUhhcAwNmaxjwnfe3mCyAEuTIsLgKoQIXs5aSadnMUgq5f1-xzM2Wk=@protonmail.com)
**Source:** Mailing List | **Started By:** {'username': 'Fabian Jahr', 'uuid': 'can_fabian_jahr'} | **Messages:** 3
> Developers are exploring a method to combine multiple cryptographic signatures to reduce the data size of Bitcoin transactions. If successful, this integration could significantly lower transaction fees and boost network capacity without compromising security.

**Technical Details:** The discussion focuses on applying the half-aggregation technique described in BIP458 to DahLIAS signatures. Fabian suggests that the half-aggregation scheme generalizes to DahLIAS by leveraging multi-scalar multiplication (MSM) verification. The core architectural debate involves assessing the computational efficiency and verification overhead of this generalization compared to standard Schnorr half-aggregation. Moving forward, developers need to formally verify the algebraic properties of DahLIAS in this context and draft a concrete implementation.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@roqqit](https://github.com/roqqit)

### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@ryanofsky](https://github.com/ryanofsky), [@brunoerg](https://github.com/brunoerg), [@davidgumberg](https://github.com/davidgumberg), [@hebasto](https://github.com/hebasto)

### 🕵️ Top Reviewers
Providing critical review and testing: [@maflcko](https://github.com/maflcko), [@hebasto](https://github.com/hebasto), [@l0rinc](https://github.com/l0rinc), [@pinheadmz](https://github.com/pinheadmz), [@fanquake](https://github.com/fanquake)
