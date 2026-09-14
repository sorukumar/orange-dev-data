# 📰 This Week in Bitcoin (2026-08-31 to 2026-09-06)

## 📌 The TL;DR
- Significant work went into hardening core network robustness against misbehaving peers and DoS attempts (e.g., clock sync, V2 message validation, HTTP throttling), alongside performance optimizations for validation (parallel prevout fetching) and extensive testing infrastructure improvements.
- Active conceptual discussions are advancing future protocol enhancements, particularly around improved privacy (Silent Payments), robust multi-party signing coordination (PSBT/MuSig2 over Nostr), and long-term cryptographic resilience (post-quantum signatures, SNARK-based aggregation).

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#35351: net: Disallow invalid HeadersSyncState due to lagging clock](https://github.com/bitcoin/bitcoin/pull/35351)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Security & Consensus]** *(Activity: 24 review events)*
> This PR prevents a Bitcoin Core node from entering an invalid header synchronization state if its system clock is significantly lagging. This improves the reliability and security of header chain synchronization by enforcing valid time constraints.

**Technical Details:** The `HeadersSyncState` enum, which tracks header synchronization progress, now includes a check during state transitions. If the node's `GetAdjustedTime()` is determined to be excessively old compared to the `AssumeValid` block's timestamp, the state transition will be disallowed. This mechanism prevents the node from making incorrect header chain selections due to an inaccurate system clock, enhancing overall network synchronization robustness.

#### [#36123: http: throttle per-connection reads while a request is in flight](https://github.com/bitcoin/bitcoin/pull/36123)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 21 review events)*
> This PR implements throttling for per-connection reads in the HTTP server while a request is being processed. This helps prevent resource exhaustion and potential Denial of Service (DoS) attacks by limiting how much data a single connection can send.

**Technical Details:** When an HTTP request is actively being parsed or handled, a malicious client could potentially flood the server with additional data on the same connection, consuming resources without proper processing. This change introduces a mechanism to limit the rate or volume of data read from a specific HTTP connection once a request is identified as 'in flight,' thereby protecting the server from memory or CPU exhaustion by slowloris-type attacks or simple data floods on open connections.

#### [#36048: util: keep wallet names literal in notification commands](https://github.com/bitcoin/bitcoin/pull/36048)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Wallet & User Tools]** *(Activity: 21 review events)*
> This Pull Request ensures that wallet names are passed literally into external notification commands, preventing misinterpretation of special characters. This improves the reliability of external scripts and tools that integrate with Bitcoin Core's notification system.

**Technical Details:** Bitcoin Core provides notification mechanisms (e.g., `walletnotify`) that execute external commands, passing relevant information, including wallet names, as arguments. Previously, these wallet names were potentially subjected to shell-like interpretation or improper escaping when constructing the command-line arguments, which could lead to unexpected behavior or failures if the names contained special characters. This PR modifies the utility functions responsible for building and executing these notification commands to ensure wallet names are passed as verbatim, uninterpreted string arguments, typically by using appropriate quoting or direct argument passing that bypasses shell parsing. This guarantees the exact wallet name is used by external scripts.

#### [#36118: test: tolerate race condition in interface_http.py](https://github.com/bitcoin/bitcoin/pull/36118)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*
> This PR addresses a race condition that intermittently caused failures in HTTP interface tests. By making the test more robust against timing variations, it eliminates flakiness and improves the reliability of the automated test suite.

**Technical Details:** The fix involves adjusting the `interface_http.py` test script to gracefully handle a timing-sensitive scenario during HTTP interactions. This likely entails adding retry logic, explicit delays, or more robust synchronization mechanisms within the test framework to ensure the test only fails for actual bugs and not for transient timing issues.

#### [#36169: http: Use SO_EXCLUSIVEADDRUSE on Windows](https://github.com/bitcoin/bitcoin/pull/36169)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Network & Privacy]** *(Activity: 13 review events)*
> This PR configures the HTTP server on Windows to use `SO_EXCLUSIVEADDRUSE`, preventing port conflicts when restarting the server. This ensures a smoother server restart experience and prevents 'Address already in use' errors.

**Technical Details:** On Windows, the `SO_EXCLUSIVEADDRUSE` socket option allows a socket to bind to a port exclusively, even if other applications might have been holding a pending connection or if the port is in a `TIME_WAIT` state. By setting this option for the HTTP server's listening socket, the server can reliably rebind to its port immediately after being shut down, avoiding common bind errors on restarts.

#### [#36130: test: add tests in transaction_tests.cpp covering live mutants](https://github.com/bitcoin/bitcoin/pull/36130)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 13 review events)*
> This PR adds new tests to `transaction_tests.cpp` to cover 'live mutants' identified during mutation testing. This enhances the test suite's effectiveness, ensuring critical transaction logic is robustly validated against subtle changes.

**Technical Details:** Mutation testing involves introducing small, syntactic changes (mutants) into the code and checking if existing tests fail. 'Live mutants' are those changes that don't cause any existing tests to fail, indicating a lack of test coverage for that specific code path or logic. This PR specifically targets and adds new test cases to `transaction_tests.cpp` to kill these live mutants, thereby increasing the confidence in the correctness and stability of transaction-related logic.

#### [#35738: coins: parallel input prevout fetching followups](https://github.com/bitcoin/bitcoin/pull/35738)
**Author:** [@andrewtoth](https://github.com/andrewtoth) | **[Performance & Optimization]** *(Activity: 10 review events)*
> This PR includes follow-up improvements to the parallel input previous output fetching mechanism. This enhances transaction validation performance by optimizing how historical transaction outputs are retrieved.

**Technical Details:** The initial 'parallel input prevout fetching' likely introduced concurrency to speed up the process of looking up UTXOs (Unspent Transaction Outputs) needed for validating transaction inputs. This PR contains further refinements, optimizations, or bug fixes related to that parallelization effort. These follow-ups likely address specific performance bottlenecks, race conditions, or improve resource management within the concurrent prevout fetching logic, ultimately leading to faster transaction validation, especially during IBD or for large blocks.

#### [#36137: validation: use unused SetTargetBlockHash](https://github.com/bitcoin/bitcoin/pull/36137)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR integrates an existing but previously unused `SetTargetBlockHash` function into the validation logic. This ensures a consistent and controlled way of setting the target block hash within the `Chainstate` object.

**Technical Details:** It appears there was a `SetTargetBlockHash` function available, but it wasn't being called anywhere. This PR identifies a place where the `target_blockhash` needed to be set, and instead of duplicating logic or directly modifying the member, it utilizes the existing dedicated setter. This improves code consistency, reduces potential for errors, and leverages existing utility functions within the validation subsystem.

#### [#35808: fuzz: reset connman state in p2p targets](https://github.com/bitcoin/bitcoin/pull/35808)
**Author:** [@HowHsu](https://github.com/HowHsu) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This improvement enhances the reliability of fuzzing tests for Bitcoin Core's peer-to-peer networking code by ensuring that each test run starts with a clean state. This leads to more effective bug detection and a more stable network implementation.

**Technical Details:** The fuzzing targets for the P2P networking components have been updated to explicitly reset the `CConnman` state at the beginning of every fuzzing iteration. This prevents state leakage or dependencies between successive fuzzing inputs, which could otherwise mask bugs or produce non-reproducible test failures. By guaranteeing a pristine `CConnman` instance for each test, the fuzzer can more accurately isolate and identify issues within the P2P message processing logic.

#### [#36103: validation: remove unused code](https://github.com/bitcoin/bitcoin/pull/36103)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR removes obsolete or unused code from the validation module, making the codebase cleaner and easier to maintain. This reduces unnecessary complexity within Bitcoin Core's core logic.

**Technical Details:** The PR identifies and excises dead code paths or variables within the `validation` component. This process typically involves reviewing static analysis outputs or manually verifying that specific functions or variables are no longer called or referenced, thereby reducing compilation size and improving code clarity.

#### [#35958: net: align v2 message type validation with v1 range](https://github.com/bitcoin/bitcoin/pull/35958)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[Network & Privacy]** *(Activity: 7 review events)*
> This PR ensures that the validation logic for v2 P2P message types is consistent with the established character range used for v1 messages. This maintains robustness in our network message processing, preventing potential issues with malformed message types in the v2 protocol.

**Technical Details:** The change modifies the network layer's message parsing and validation logic for version 2 (v2) P2P messages. It introduces or tightens a check to ensure that the message type field in v2 messages adheres to the same allowed character set or range of values previously defined for v1 messages, rejecting messages with invalid type characters.

#### [#36131: rpc: Improve two field's OpenRPC types](https://github.com/bitcoin/bitcoin/pull/36131)
**Author:** [@sedited](https://github.com/sedited) | **[Wallet & User Tools]** *(Activity: 7 review events)*
> This PR improves the OpenRPC type definitions for two specific fields in our RPC interface. This enhances the accuracy and clarity of the auto-generated RPC documentation, making it easier for developers to correctly use these API fields.

**Technical Details:** The modification involves updating the OpenRPC schema to refine the data types specified for two fields within certain RPC methods. This could include changing a generic type like `string` to a more specific type like `integer` or `boolean`, or adding constraints such as `enum` values or `pattern` regexes to better reflect the expected data format.

#### [#36100: ci: use LLVM 23 in *san, fuzz, *cross jobs](https://github.com/bitcoin/bitcoin/pull/36100)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR updates the continuous integration (CI) system to utilize LLVM 23 for various jobs, including sanitizers, fuzzing, and cross-compilation. This ensures our automated testing pipeline uses the latest tools, enhancing the reliability and thoroughness of code quality checks.

**Technical Details:** The change modifies CI workflow configurations to specify LLVM version 23 for specific build targets like `address_sanitizer`, `thread_sanitizer`, fuzzing, and cross-compilation environments. This updates the underlying compiler toolchain used by the CI runners, allowing for potential detection of new warnings or errors enabled by the newer compiler version.

#### [#36148: test: Avoid unsafe memory race in index_reorg_crash shutdown](https://github.com/bitcoin/bitcoin/pull/36148)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR resolves a critical test stability issue by preventing an unsafe memory race condition during the shutdown phase of the `index_reorg_crash` test. This improves the reliability of our testing infrastructure, ensuring test failures accurately reflect code bugs rather than test harness issues.

**Technical Details:** The fix addresses a specific memory race that could occur in the `index_reorg_crash` test during node shutdown, potentially leading to undefined behavior or test crashes. This likely involves implementing proper synchronization primitives or reordering resource deallocation within the test setup to prevent concurrent access to freed memory.

#### [#36166: validation: refactor: encapsulate Chainstate::m_target_blockhash](https://github.com/bitcoin/bitcoin/pull/36166)
**Author:** [@stickies-v](https://github.com/stickies-v) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR refactors the `Chainstate` class by encapsulating the `m_target_blockhash` member. This improves the internal architecture of the validation logic, making it cleaner and more maintainable.

**Technical Details:** Encapsulation involves making member variables private and providing public methods (getters/setters) to access or modify them. By encapsulating `Chainstate::m_target_blockhash`, this PR ensures that the `target_blockhash` can only be accessed or modified through defined interfaces within the `Chainstate` class, preventing direct external manipulation. This promotes data integrity and simplifies future changes to the validation subsystem by centralizing its access logic.

#### [#36102: util: Replace !ContainsNoNUL() with ContainsNUL()](https://github.com/bitcoin/bitcoin/pull/36102)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR refactors a utility function check to improve code readability and maintainability. It simplifies a logical expression within the Bitcoin Core codebase.

**Technical Details:** This PR replaces the boolean negation of `ContainsNoNUL()` with a direct call to a new `ContainsNUL()` function in the `util` library. This improves clarity by expressing the intent positively, reducing cognitive load for developers. The change is purely internal and does not alter any external behavior or API.

#### [#35477: test: exercise Schnorr signature cache in txvalidationcache_tests.cpp](https://github.com/bitcoin/bitcoin/pull/35477)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR adds specific tests to verify the correct functionality and efficiency of the Schnorr signature cache within the transaction validation cache. This ensures the reliable performance of Schnorr signature validation.

**Technical Details:** The PR introduces a new set of test cases to the `txvalidationcache_tests.cpp` file, specifically designed to interact with and validate the behavior of the Schnorr signature cache. These tests simulate various scenarios, including cache hits, misses, evictions, and expiry, to confirm that Schnorr signatures are correctly stored, retrieved, and managed by the cache. This ensures the cache accurately reduces redundant signature verification computations, thereby improving transaction validation performance and confirming the correctness of its underlying logic.

#### [#36054: test: add script_tests cases covering interpreter mutants](https://github.com/bitcoin/bitcoin/pull/36054)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR enhances the test suite for Bitcoin's scripting interpreter by adding new test cases to cover 'mutants'. This improves the robustness and reliability of Bitcoin's transaction script validation logic.

**Technical Details:** This PR introduces additional `script_tests` cases designed to cover various 'mutants' in the script interpreter. This often involves fuzzing or mutation testing techniques to generate slightly altered versions of valid scripts and ensure the interpreter correctly rejects them or handles them as expected. The goal is to improve confidence in the interpreter's behavior against edge cases.

#### [#36164: util: diagnose dangling views of temporary strings](https://github.com/bitcoin/bitcoin/pull/36164)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR introduces a utility to detect and diagnose potential issues where string views might outlive the temporary strings they refer to. This helps prevent subtle bugs and improves the robustness of string handling within the codebase.

**Technical Details:** String views (like `std::string_view`) are references to existing string data. If the underlying temporary string is destroyed before the string view, the view becomes 'dangling' and accessing it leads to undefined behavior. This utility likely implements checks, possibly in debug builds or via specific analysis tools, to identify such instances, helping developers catch these memory safety issues during development.

#### [#36161: build: Remove `cmake/script/CoverageFuzz.cmake`](https://github.com/bitcoin/bitcoin/pull/36161)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR cleans up the build system by removing an outdated CMake script related to coverage and fuzzing. This streamlines the build configuration and reduces unnecessary files.

**Technical Details:** The `cmake/script/CoverageFuzz.cmake` file is no longer needed, likely due to changes in how coverage and fuzzing are handled in the build process or deprecation of a specific toolchain. Its removal simplifies the build configuration, preventing potential conflicts or confusion for developers working with the build system. This is a pure cleanup of build artifacts.

#### [#36145: qa: Use IP_PORTRANGE_HIGH on OpenBSD for dynamic port allocation](https://github.com/bitcoin/bitcoin/pull/36145)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR updates our quality assurance (QA) tests to correctly use `IP_PORTRANGE_HIGH` for dynamic port allocation specifically on OpenBSD systems. This ensures our tests run reliably on OpenBSD by accommodating its unique network stack behavior, preventing port binding related test failures.

**Technical Details:** The modification adjusts the QA test suite's network setup when executed on OpenBSD. It changes the socket option used for dynamic port allocation from a generic or Linux-specific value to `IP_PORTRANGE_HIGH`, which is the correct mechanism on OpenBSD for reserving ports from the higher ephemeral range.

#### [#36112: ci: Exclude subtrees from iwyu](https://github.com/bitcoin/bitcoin/pull/36112)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR optimizes our continuous integration by excluding specific subtrees from the 'include-what-you-use' (IWYU) analysis. This change helps reduce CI build times and resource consumption without compromising the code quality checks on the core codebase.

**Technical Details:** The modification updates the CI configuration to adjust the scope of the IWYU script. It adds directives to exclude specified directories or external subtrees, such as vendored dependencies or generated files, from the include analysis, as these are typically not subject to IWYU rules and only add unnecessary overhead.

#### [#36065: test: refactor: Remove confusing ignore_errors=True](https://github.com/bitcoin/bitcoin/pull/36065)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR refactors a test by removing a confusing `ignore_errors=True` flag, which could mask legitimate issues within the test environment. This improves the clarity and effectiveness of our tests, ensuring that errors are always properly reported for investigation.

**Technical Details:** The change involves modifying a specific test script to remove an instance where `ignore_errors=True` was used, likely in a utility for executing shell commands or managing processes. This ensures that any non-zero exit code or error output from the executed command will now correctly propagate and cause the test to fail, catching previously suppressed errors.

#### [#36134: doc: Correct comment about which subsystem detects lagging clocks](https://github.com/bitcoin/bitcoin/pull/36134)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR corrects an outdated internal comment in the codebase regarding which subsystem is responsible for detecting lagging clocks. This improves the accuracy of our internal documentation, aiding future developers in understanding the codebase more effectively.

**Technical Details:** The change involves updating an inline code comment within a source file. It corrects the description of which specific component or subsystem within Bitcoin Core is currently responsible for identifying or handling lagging system clocks, bringing the comment in line with the latest architectural implementation details.

#### [#36163: test: Add coverage for unsatisfiable locktime combination in PSBT `ComputeTimeLock()`](https://github.com/bitcoin/bitcoin/pull/36163)
**Author:** [@nebula-21](https://github.com/nebula-21) | **[Wallet & User Tools]** *(Activity: 2 review events)*
> This PR adds a new test case to cover an edge scenario where a PSBT's `ComputeTimeLock()` function encounters an unsatisfiable locktime combination. This ensures the PSBT logic correctly handles complex and invalid locktime conditions.

**Technical Details:** PSBTs (Partially Signed Bitcoin Transactions) can include various locktime components. The `ComputeTimeLock()` function is responsible for determining the final effective locktime for a transaction. This PR introduces a specific test scenario where conflicting or otherwise unsatisfiable locktime conditions are provided within a PSBT, ensuring that `ComputeTimeLock()` correctly identifies and handles such invalid inputs, preventing unexpected behavior or potential issues when constructing or evaluating PSBTs.

#### [#36144: rpc: detail x-bitcoin-unit in openrpc help](https://github.com/bitcoin/bitcoin/pull/36144)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Wallet & User Tools]** *(Activity: 2 review events)*
> This PR enhances the OpenRPC documentation by providing more detailed information about the `x-bitcoin-unit` HTTP header. This improves the clarity and completeness of our RPC API documentation, making it easier for developers to correctly interact with Bitcoin Core.

**Technical Details:** The change involves modifying the OpenRPC schema definition for the Bitcoin Core RPC interface. Specifically, it adds or expands the descriptive text associated with the `x-bitcoin-unit` HTTP header, explaining its purpose and valid values within the auto-generated API reference.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#36123: http: throttle per-connection reads while a request is in flight](https://github.com/bitcoin/bitcoin/pull/36123)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 16 review events this week)*
> This PR implements throttling for per-connection reads in the HTTP server while a request is being processed. This helps prevent resource exhaustion and potential Denial of Service (DoS) attacks by limiting how much data a single connection can send.

#### [#36169: http: Use SO_EXCLUSIVEADDRUSE on Windows](https://github.com/bitcoin/bitcoin/pull/36169)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Network & Privacy]** *(Activity: 13 review events this week)*
> This PR configures the HTTP server on Windows to use `SO_EXCLUSIVEADDRUSE`, preventing port conflicts when restarting the server. This ensures a smoother server restart experience and prevents 'Address already in use' errors.

#### [#36048: util: keep wallet names literal in notification commands](https://github.com/bitcoin/bitcoin/pull/36048)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Wallet & User Tools]** *(Activity: 13 review events this week)*
> This Pull Request ensures that wallet names are passed literally into external notification commands, preventing misinterpretation of special characters. This improves the reliability of external scripts and tools that integrate with Bitcoin Core's notification system.

#### [#36130: test: add tests in transaction_tests.cpp covering live mutants](https://github.com/bitcoin/bitcoin/pull/36130)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 13 review events this week)*
> This PR adds new tests to `transaction_tests.cpp` to cover 'live mutants' identified during mutation testing. This enhances the test suite's effectiveness, ensuring critical transaction logic is robustly validated against subtle changes.

#### [#36118: test: tolerate race condition in interface_http.py](https://github.com/bitcoin/bitcoin/pull/36118)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Maintenance & Tech Debt]** *(Activity: 12 review events this week)*
> This PR addresses a race condition that intermittently caused failures in HTTP interface tests. By making the test more robust against timing variations, it eliminates flakiness and improves the reliability of the automated test suite.

## 🗓️ Dev Meeting
Summary of the core dev IRC meeting on 2026-09-03 with 88 participants.

- QA Working Group updates, including mutation analysis for silent payments and script interpreter, and parallelization efforts.
- QML GUI Working Group progress on staging branch and issue fixes.
- Benchmarking Working Group updates on quadratic iteration fixes, IBD performance improvements, and future profiling plans.
- Kernel Working Group progress on script evaluation tracer and collaboration with fuzzing efforts.
- Discussion on remaining items for the upcoming release milestone and HTTP agent findings.

**Action Items:**
- Review PR #36096 (rpc: avoid quadratic JSON construction).
- Review remaining items in milestone #84 before the branch-off.
- Provide feedback on `bitcoinfuzz/bitcoinfuzz/pull/647` for differential fuzzing.
- Continue triaging agent findings in the new HTTP implementation.
- Experiment with parallel undo/block flushes for IBD.
- Profile neutered validation-less and UTXO-less nodes to identify bottlenecks.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: PSBT/MuSig2 coordination over Nostr relays: transport invariants for nonce safety under at-least-once delivery](https://delvingbitcoin.org/t/psbt-musig2-coordination-over-nostr-relays-transport-invariants-for-nonce-safety-under-at-least-once-delivery/2852/4)
**Source:** Delving | **Started By:** {'username': 'Rafael Turon', 'uuid': 'auto_rafael_turon'} | **Messages:** 3
> Developers are discussing how to enhance the security and privacy of shared Bitcoin wallets by reducing reliance on central coordinators in multi-party custody setups.

**Technical Details:** The discussion highlights that current multi-party custody arrangements, including 2-of-3 multisig, MuSig2 Taproot aggregates, and covenant-free time-locked vaults, predominantly route PSBTs and signing rounds through a coordinator often run by a wallet vendor. This architecture presents a centralization point for coordination. The technical debate revolves around exploring alternative, more decentralized methods for exchanging PSBTs and managing signing rounds, aiming to improve privacy, robustness, and reduce reliance on third-party infrastructure. This could involve investigating peer-to-peer communication protocols or novel shared state mechanisms among signing participants.

### [Re: Block-wide Signature Aggregation via SNARKs](https://delvingbitcoin.org/t/block-wide-signature-aggregation-via-snarks/2875/3)
**Source:** Delving | **Started By:** {'username': 'conduition', 'uuid': 'auto_conduition'} | **Messages:** 2
> To prepare Bitcoin for quantum threats, developers are exploring ways to reduce the size of new, quantum-resistant transaction signatures. The goal is to make these future-proof transactions efficient, ensuring they don't bloat the network or increase costs for running nodes.

**Technical Details:** The discussion revolves around mitigating the large size of post-quantum (PQ) signatures to reduce their impact on block propagation and archival node resources. Evaluation of ZKP-based compression methods is underway, with Groth16 being dismissed due to its quantum insecurity and trusted setup. LeanVM is highlighted as a more interesting, potential alternative for compressing PQ signatures, while the relevance of BitVM in this context is also being questioned.

### [[bitcoindev] Re: SHRINCS: an efficient hash-based signature scheme
 for Bitcoin (first draft)](https://gnusha.org/pi/bitcoindev/2fb38fb8-2584-4550-b268-ee7138de419bn@googlegroups.com)
**Source:** Mailing List | **Started By:** {'username': 'conduition', 'uuid': 'auto_conduition'} | **Messages:** 2
> A new cryptographic proposal, SHRINCS, is being developed to potentially enhance Bitcoin's capabilities. It's designed to be flexible, focusing on core cryptographic improvements without dictating its final integration details.

**Technical Details:** The current discussion confirms the SHRINCS BIP is deliberately scoped as a purely cryptographic proposal, explicitly not specifying "cost accounting" details. This design choice makes the BIP deployment-agnostic, separating its core cryptographic primitives from specific economic or resource models. Future integration efforts will need to address how cost accounting and other system-level considerations interface with the SHRINCS framework, as these are out of its current scope.

### [Re: Silent Payments coinbase](https://delvingbitcoin.org/t/silent-payments-coinbase/2833/5)
**Source:** Delving | **Started By:** {'username': 'Marathon Gary', 'uuid': 'auto_marathon_gary'} | **Messages:** 2
> Bitcoin mining pools are exploring direct, on-chain miner payouts within the coinbase transaction. This innovation could simplify payment processes and potentially reduce transaction overhead for miners.

**Technical Details:** The discussion revolves around mining pools embedding multiple miner payouts directly into the coinbase transaction's scriptSig. A key architectural concern is the limited extranonce space within the coinbase, especially when accounting for future block height encoding requirements. `average_gary`'s recent comment suggests this space constraint might be manageable, particularly with the introduction of proposals like BIP323, which aims to provide more flexible methods for committing arbitrary data in the coinbase. Further work is needed to design and standardize how pools can utilize this space efficiently for direct payouts while maintaining network compatibility.

### [Implicit Deletions and Improvements in Utreexo IBD](https://delvingbitcoin.org/t/implicit-deletions-and-improvements-in-utreexo-ibd/2881/1)
**Source:** Delving | **Started By:** {'username': 'Davidson', 'uuid': 'auto_davidson'} | **Messages:** 1
> Utreexo is being explored as a method to significantly reduce the amount of data nodes need to store to verify transactions, making it easier and faster for new participants to join and secure the Bitcoin network. It aims to compact the entire set of unspent transactions.

**Technical Details:** The discussion introduces Utreexo, a dynamic accumulator designed to represent the full UTXO set with just a few hashes. Its core technical approach involves structuring the UTXO data as a forest of perfect Merkle trees, enabling this highly compact representation. This architecture promises substantial reductions in node state size, though the current context only outlines its foundational mechanism without delving into specific implementation challenges, integration strategies, or ongoing architectural debates within Bitcoin Core.

## 🏆 Contributor Shoutouts
### ✍️ Top Authors
The most active PR authors this week: [@hodlinator](https://github.com/hodlinator), [@fanquake](https://github.com/fanquake), [@maflcko](https://github.com/maflcko), [@hebasto](https://github.com/hebasto), [@pinheadmz](https://github.com/pinheadmz)

### 🕵️ Top Reviewers
Providing critical review and testing: [@jeanpablojp](https://github.com/jeanpablojp), [@maflcko](https://github.com/maflcko), [@sedited](https://github.com/sedited), [@l0rinc](https://github.com/l0rinc), [@hodlinator](https://github.com/hodlinator)
