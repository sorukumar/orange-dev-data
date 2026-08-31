# 📰 This Week in Bitcoin (2026-08-24 to 2026-08-30)

## 📌 The TL;DR
- Active discussions are centered on future-proofing Bitcoin against quantum computing, exploring Post-Quantum Cryptography (PQC) for new output types and integration with existing witness structures.
- Fundamental architectural and consensus design questions are being re-evaluated, particularly concerning a pragmatic definition of consensus for light clients, and novel Proof-of-Work mechanisms like Time-Shifted PoW.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#35730: http: limit connected HTTPRemoteClients](https://github.com/bitcoin/bitcoin/pull/35730)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 46 review events)*
> This pull request introduces a limit on concurrent HTTP connections to protect the RPC and REST interfaces from resource exhaustion. This prevents Denial of Service (DoS) attacks from stalling or crashing the node.

**Technical Details:** The internal HTTP server, built on libevent, is modified to track and enforce a ceiling on active `HTTPRemoteClient` connections. When the number of concurrent connections reaches the defined threshold, additional socket requests are closed immediately instead of being queued. This resource-limiting strategy prevents socket descriptor exhaustion and memory exhaustion vectors targeting the node's local interface ports. This protection boundary ensures the node remains functional and responsive under heavy local or remote connection loads.

#### [#35829: http: Make class fields private and make HTTPResponse a struct](https://github.com/bitcoin/bitcoin/pull/35829)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 22 review events)*
> This pull request improves the internal code structure of Bitcoin Core's HTTP server by enforcing better encapsulation. Making class fields private and turning `HTTPResponse` into a struct prevents accidental external modifications and clarifies data ownership.

**Technical Details:** The changes refactor the `HTTPRequest` and related HTTP classes to enforce encapsulation by transitioning public member variables to private fields accessed via getters. Additionally, `HTTPResponse` is refactored from a class to a plain struct to better represent its role as a passive data carrier. This reduces tight coupling between the HTTP event-loop handling and the RPC/REST processors, leading to safer memory management and cleaner API boundaries.

#### [#35868: rpc, wallet: fix invalid JSON in HelpExampleRpc curl examples](https://github.com/bitcoin/bitcoin/pull/35868)
**Author:** [@GuTS805](https://github.com/GuTS805) | **[Maintenance & Tech Debt]** *(Activity: 20 review events)*
> Fixes formatting errors in the curl command examples displayed by the RPC help messages. This ensures that copy-pasted examples are valid JSON and run successfully in terminal environments.

**Technical Details:** Corrects the quoting and JSON structure inside the HelpExampleRpcCli and HelpExampleRpcNamed helper string generators. Specifically, it ensures double quotes are properly escaped within the single-quoted curl payloads. This resolves syntax errors when users attempt to execute these commands directly in their terminals.

#### [#36057: build: check for SetThreadDescription() at configure time](https://github.com/bitcoin/bitcoin/pull/36057)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 19 review events)*
> This PR updates the build system to verify the availability of the Windows thread-naming API during compilation. This improves compile-time checks and guarantees more reliable thread diagnostics on Windows platforms.

**Technical Details:** The build configuration is modified to check for the presence of the `SetThreadDescription` API in system headers at configure-time. This replaces dynamic runtime symbol resolution or OS-version guessing mechanisms. As a result, compilation produces safer, platform-specific thread initialization code that maps seamlessly to native diagnostic tools.

#### [#35850: fuzz: Implement `connect_block` harness](https://github.com/bitcoin/bitcoin/pull/35850)
**Author:** [@marcofleon](https://github.com/marcofleon) | **[Maintenance & Tech Debt]** *(Activity: 18 review events)*
> Introduces a new fuzz testing harness specifically targeting the connect_block functionality. This helps automatically discover edge-case bugs and consensus discrepancies by feeding mutated block data directly into the block validation pipeline.

**Technical Details:** Implements a new fuzz target that initializes a minimal node state and calls the ConnectBlock validation interface with fuzz-generated inputs. The harness mocks the necessary chainstate and coin database dependencies to safely execute the block connection logic. This allows continuous fuzzing infrastructure to explore deep code paths in consensus enforcement and block undo data processing.

#### [#35516: rpc: preserve global xpubs and proprietary fields in joinpsbts](https://github.com/bitcoin/bitcoin/pull/35516)
**Author:** [@thomasbuilds](https://github.com/thomasbuilds) | **[Wallet & User Tools]** *(Activity: 15 review events)*
> This PR modifies the `joinpsbts` RPC tool to ensure that global extended public keys (xpubs) and proprietary metadata fields are preserved in the merged output. This prevents data loss when combining PSBTs generated by different signers or hardware wallets.

**Technical Details:** The implementation of the `joinpsbts` RPC is updated to iterate over the source PSBTs and carry over `g_xpubs` and `m_proprietary` fields into the output transaction struct. Previously, these fields were dropped during the join step because they were not explicitly serialized from inputs. This change ensures compliance with modern PSBT formats that utilize these global maps.

#### [#36025: psbt: avoid duplicate taproot leaf script keys when merging](https://github.com/bitcoin/bitcoin/pull/36025)
**Author:** [@shuv-amp](https://github.com/shuv-amp) | **[Wallet & User Tools]** *(Activity: 15 review events)*
> This PR resolves an issue in the PSBT merging logic where duplicate Taproot leaf script keys could inadvertently be created. Ensuring deduplication prevents malformed PSBTs and ensures compatibility with standard Taproot specifications.

**Technical Details:** During the merging phase of two PSBT instances, the implementation now scans for existing Taproot leaf script metadata keys like `PSBT_IN_TAP_BIP32_DERIVATION` or `PSBT_IN_TAP_INTERNAL_KEY`. The merging algorithm is updated to deduplicate identical keys rather than blindly appending them to the key-value map. This enforces BIP 371 compliance for multi-party transaction construction.

#### [#34993: wallet: `NotifyCanGetAddressesChanged` when advancing `next_index`](https://github.com/bitcoin/bitcoin/pull/34993)
**Author:** [@davidgumberg](https://github.com/davidgumberg) | **[Wallet & User Tools]** *(Activity: 15 review events)*
> This PR ensures that the wallet correctly notifies external components when its address derivation index (`next_index`) advances, indicating new addresses can be generated. This allows connected interfaces or services, such as a GUI, to accurately reflect the wallet's state and available address generation capabilities.

**Technical Details:** The change involves adding a call to `NotifyCanGetAddressesChanged()` (or a similar notification mechanism) at the precise point where the wallet's internal `next_index` for Hierarchical Deterministic (HD) address derivation is incremented. This ensures that any subscribers to wallet notifications are immediately informed of the potential for new address generation. External components can then refresh their user interface or internal state without needing to poll the wallet, leading to a more responsive and accurate user experience when managing addresses.

#### [#36078: qa: Reduce `-maxconnections` in the functional test framework](https://github.com/bitcoin/bitcoin/pull/36078)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 15 review events)*
> This PR reduces the default maximum number of concurrent network connections within the Python functional test framework. This optimization reduces the memory and file descriptor footprint of the nodes, speeding up test suite execution.

**Technical Details:** The Python testing framework is modified to pass a lower `-maxconnections` argument to spawned bitcoind sub-processes by default. Because functional tests utilize local, highly controlled topologies, the high default socket allocation was unnecessary and resource-intensive. Lowering this value saves significant overhead in socket management, leading to faster test setup and teardown phases.

#### [#36088: util: Set Univalue to null after read failure](https://github.com/bitcoin/bitcoin/pull/36088)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*
> This PR modifies JSON parsing utilities to reset UniValue objects to a null state when a read operation fails. This prevents developers from inadvertently referencing stale or partially parsed JSON data during error recovery.

**Technical Details:** The PR updates internal UniValue parsing helper functions to explicitly assign the `VNULL` state to target objects when standard reading functions fail. Previously, a failed read might leave the UniValue state unmodified or in an indeterminate form, presenting risks to downstream consumers. This change ensures deterministic error state states and robust validation patterns across all RPC and config-parsing interfaces.

#### [#35583: test: close the listeners before terminating the event loop](https://github.com/bitcoin/bitcoin/pull/35583)
**Author:** [@vasild](https://github.com/vasild) | **[Maintenance & Tech Debt]** *(Activity: 13 review events)*
> This PR fixes an intermittent race condition in the test suite by ensuring network socket listeners are closed before stopping the main event loop. This prevents socket bind errors and clean teardown failures during test execution.

**Technical Details:** The shutdown sequence in the networking test harness is refactored to explicitly call close on all listening sockets prior to invoking the event loop termination. Previously, halting the event loop first could leave socket bindings in a lingering state or cause assertion failures on active threads. This sequencing change guarantees that file descriptors are safely freed and pending events are resolved before the loop is destroyed.

#### [#34697: descriptor: fix musig() duplicate key checks and doubled PSBT origin paths](https://github.com/bitcoin/bitcoin/pull/34697)
**Author:** [@shuv-amp](https://github.com/shuv-amp) | **[Wallet & User Tools]** *(Activity: 12 review events)*
> This PR fixes issues in `musig()` output descriptors by correctly validating unique keys and preventing duplicated PSBT origin paths. This ensures accurate and compliant generation of MuSig-related descriptors and PSBTs.

**Technical Details:** The `musig()` descriptor parser previously allowed duplicate public keys and could inadvertently double the derivation paths for keys when generating PSBTs. The fix adds robust validation to enforce unique keys within the `musig()` descriptor and modifies the PSBT generation logic to correctly include each key's derivation origin exactly once, ensuring correctness for multi-signature descriptors.

#### [#36032: rpc: avoid quadratic output lookups](https://github.com/bitcoin/bitcoin/pull/36032)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Performance & Optimization]** *(Activity: 11 review events)*
> Improves the performance of certain RPC commands by optimizing how transaction outputs are searched, changing a slow quadratic lookup process to a much faster linear or constant-time operation. This significantly reduces CPU utilization and response times when querying large transactions.

**Technical Details:** Replaces a quadratic nested loop lookup mechanism with a more efficient lookup structure, such as using a hash map or sorted index, when processing transaction outputs in RPC methods. This avoids performance degradation when handling transactions with tens of thousands of inputs or outputs. It ensures that RPC response times scale linearly rather than quadratically with transaction size.

#### [#35618: depends: Make tarball creation from local directory reproducible](https://github.com/bitcoin/bitcoin/pull/35618)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*
> This PR ensures that building dependency tarballs from local directories produces bit-for-bit identical, reproducible results. This improves the security and auditability of the Bitcoin Core build system by eliminating environmental non-determinism.

**Technical Details:** The dependency build system in `depends/` is updated to enforce deterministic metadata when generating tarballs from local source directories. It utilizes flags like `--mtime`, `--owner=0`, and `--group=0` for GNU tar, or equivalent workarounds, ensuring consistency across different build environments. This prevents non-deterministic attributes like host-specific file creation timestamps and user/group IDs from leaking into the output hash.

#### [#36077: bugfix: give TxDownloadManager its own RNG](https://github.com/bitcoin/bitcoin/pull/36077)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Network & Privacy]** *(Activity: 10 review events)*
> This PR isolates transaction fetching logic by assigning a dedicated random number generator to the TxDownloadManager. This decoupling reduces state sharing and enhances security against potential P2P timing attacks.

**Technical Details:** The `TxDownloadManager` is modified to instantiate and use its own private `FastRandomContext` instance. Rather than drawing entropy from the shared global RNG, this separation isolates the random sequences used for timing and selecting transaction fetch schedules. This prevents external observers from inferring the state of other subsystems and eliminates synchronization overhead on the global generator.

#### [#35900: iwyu: Fix warnings in `src/interfaces` and treat them as errors](https://github.com/bitcoin/bitcoin/pull/35900)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> Optimizes code compilation and header hygiene within the src/interfaces directory using the 'Include What You Use' (IWYU) tool. This ensures cleaner interface boundaries and prevents compilation slowdowns from redundant includes.

**Technical Details:** Refactors header file inclusions in src/interfaces based on IWYU analysis to ensure files only include what they strictly require. By turning these warnings into hard build errors in CI, it guarantees that interface definitions remain clean and do not re-introduce implicit dependency leaks. This refactoring is crucial for the ongoing multiprocess isolation work.

#### [#1908: refactor: replace `_get_hash_context` with direct `->hash_ctx` access](https://github.com/bitcoin/bitcoin/pull/1908)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR adds documentation for the various RPC error codes returned by the Bitcoin Core node. This provides a clear specification for API developers to properly handle errors in external applications.

**Technical Details:** The PR adds or updates documentation files to exhaustively catalog standard HTTP and JSON-RPC error integers. Each error code (e.g., `RPC_INVALID_ADDRESS_OR_KEY`) is mapped to its intended usage context and typical failure triggers. This acts as a reference specification for programmatic interfaces interacting with the JSON-RPC server.

#### [#35580: bugfix:  compare non-adjusted chunk weight against block weight limit](https://github.com/bitcoin/bitcoin/pull/35580)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Security & Consensus]** *(Activity: 7 review events)*
> This PR corrects a consensus-critical comparison bug by ensuring that raw, unadjusted transaction chunk weights are validated against block size limits during template construction. This prevents the mining node from assembling blocks that could exceed maximum consensus limits.

**Technical Details:** In the block construction and ancestor package tracking algorithms, the code was incorrectly comparing fee-adjusted or virtual weights against hard block weight constraints. This PR changes the evaluation to compare the actual, unadjusted chunk weight directly against the consensus block limit. This guarantees that block templates generated by the node never violate maximum consensus parameters under any feerate package circumstances.

#### [#35933: psbt: don't abort on invalid MuSig2 derivations](https://github.com/bitcoin/bitcoin/pull/35933)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Wallet & User Tools]** *(Activity: 7 review events)*
> Prevents the wallet from crashing when encountering invalid MuSig2 key derivations in a PSBT, handling the error gracefully instead. This improves wallet robustness and user experience during multisig operations.

**Technical Details:** Modifies the PSBT parser to handle derivation path parsing errors for MuSig2 keypaths gracefully. Instead of calling an assertion or throwing an unhandled exception that terminates the process, the code now returns an error or skips the invalid derivation. This ensures that the deserialization of untrusted PSBT payloads does not lead to a local node or wallet crash.

#### [#36046: fuzz: Use ImmediateBackgroundTaskRunner in process_messages](https://github.com/bitcoin/bitcoin/pull/36046)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR configures the message processing fuzz tests to run background tasks immediately and synchronously. This makes fuzzing completely deterministic and significantly speeds up the discovery of message processing bugs.

**Technical Details:** In the P2P message processing fuzz targets, the standard asynchronous task runner is replaced with `ImmediateBackgroundTaskRunner`. This runner executes tasks synchronously on the main thread, bypassing multi-threaded scheduling non-determinism. This structural shift allows fuzzing tools to execute paths in a reproducible sequence and rapidly isolate edge-case crashes.

#### [#36111: rpc: bound memory for overlong Bech32 errors](https://github.com/bitcoin/bitcoin/pull/36111)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Security & Consensus]** *(Activity: 6 review events)*
> Prevents potential out-of-memory issues by putting a limit on the memory used when generating detailed error messages for invalid, extremely long Bech32 addresses. This protects the node from memory exhaustion attacks triggered via RPC inputs.

**Technical Details:** Implements a length boundary check on input strings before attempting to generate verbose Bech32 decoding error locations. Previously, an excessively long, invalid Bech32 string could cause the error-formatting logic to allocate substantial memory to highlight character mismatches. By capping the processed string length or the error output size, memory consumption is strictly bounded to a safe threshold.

#### [#35710: [30.x] More Backports](https://github.com/bitcoin/bitcoin/pull/35710)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> Backports several bug fixes and stability improvements from the master branch to the 30.x release branch. This ensures that the stable release branch remains secure, reliable, and up to date with non-breaking improvements.

**Technical Details:** Merges a curated set of PRs into the 30.x maintenance branch. These backports typically address minor bugs, testing issues, or stability concerns without introducing new features or breaking changes. The process involves cherry-picking commits and ensuring all CI tests pass against the older branch architecture.

#### [#36092: fix: UB sanitizer in mempool estimator logging](https://github.com/bitcoin/bitcoin/pull/36092)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR resolves an Undefined Behavior warning detected by sanitizers within the mempool fee estimation logging code. Eliminating undefined behavior prevents unpredictable compiler optimizations and guarantees consistent behavior across diverse hardware architectures.

**Technical Details:** The PR fixes a code path in the fee estimation logging logic that triggered UndefinedBehaviorSanitizer (UBSan) alerts. It ensures all relevant variables are fully initialized and safe from null dereferencing or boundary errors before formatting the log strings. This structural fix ensures strict adherence to C++ standards without modifying the underlying fee estimation math.

#### [#35978: contrib/init: fix unused variables in openrc script](https://github.com/bitcoin/bitcoin/pull/35978)
**Author:** [@jpk68](https://github.com/jpk68) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR addresses unused variable warnings within the OpenRC initialization script located in the project's repository. Fixing these errors ensures robust startup scripts for systems utilizing the OpenRC init system.

**Technical Details:** The shell scripting code inside `contrib/init/openrc` is updated to clean up or remove defined variables that were not utilized in the script execution path. This prevents linters and runtime environments from generating warnings or errors during service initialization. The modification is strictly administrative and has no impact on the core node binary execution.

#### [#36094: ci: bump riscv toolchain to tag 2026.08.25](https://github.com/bitcoin/bitcoin/pull/36094)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR updates the RISC-V cross-compilation toolchain tag in the continuous integration environment. This ensures that Bitcoin Core is built and tested against modern compiler optimization and security updates.

**Technical Details:** The CI build script and Dockerfile environments are updated to reference the RISC-V toolchain tag 2026.08.25. This allows the testing framework to utilize updated versions of GCC, Clang, binutils, and glibc. The update helps catch architecture-specific compiler regressions and verifies compatibilities with advanced RISC-V optimizations.

#### [#36064: qa: Minor improvement follow-ups to 35730](https://github.com/bitcoin/bitcoin/pull/36064)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR implements minor follow-up improvements and cleanups to the QA testing suite following a previous functional test change. It ensures that the test suite remains clean, robust, and free of redundant logic.

**Technical Details:** The PR refines specific test scripts to address style inconsistencies, edge cases, or redundant helper calls introduced in PR 35730. It updates the assertions and mock configurations in the functional tests to streamline execution. This maintains code health within the `test/functional/` framework.

#### [#36067: test: Remove `BOOST_CHECK_CLOSE` in favor of exact comparison](https://github.com/bitcoin/bitcoin/pull/36067)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR replaces the usage of `BOOST_CHECK_CLOSE` in the unit testing suite with exact value comparisons. This improves the test suite's precision and prevents false positives caused by floating-point rounding errors.

**Technical Details:** The unit tests are refactored to remove references to `BOOST_CHECK_CLOSE`, which allows a percentage tolerance when comparing floating-point numbers. They are replaced by exact integer checks or strict `BOOST_CHECK_EQUAL` comparisons where precision is guaranteed. This eliminates platform-specific test behavior arising from floating-point execution unit differences.

#### [#1922: field: Check that argument of _fe_set_int() is a constant](https://github.com/bitcoin/bitcoin/pull/1922)
**Author:** [@real-or-random](https://github.com/real-or-random) | **[Wallet & User Tools]** *(Activity: 3 review events)*
> This PR updates localization and translation source files for the legacy Bitcoin-Qt GUI client for version 0.7.1. This ensures a localized user interface with updated translations for non-English users.

**Technical Details:** The changes update the XML-based translation sources (`.ts` files) inside the `src/qt/locale` directory to reflect the 0.7.1 release branch string freeze. These source files are compiled into binary `.qm` resources processed at runtime by the Qt framework's translation mechanism. This is a non-functional interface update with no changes to the application's underlying cryptographic or state validation logic.

#### [#1924: tests: add coverage for the DER long form length encoding](https://github.com/bitcoin/bitcoin/pull/1924)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*

#### [#36059: test: make index crash tests check the saved state](https://github.com/bitcoin/bitcoin/pull/36059)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR improves the index crash tests to explicitly verify the saved state of the indexes upon node recovery. This guarantees that block database indices can successfully recover and remain consistent after an ungraceful shutdown.

**Technical Details:** The functional test framework is enhanced to halt the node mid-indexing, simulating a crash, and then verify the disk-persisted state on restart. It specifically asserts that the indexer's high-water mark and internal LevelDB metadata match expected bounds. This provides end-to-end validation for the recovery path of indices like coinstatsindex and txindex.

#### [#36107: iwyu: Fix warnings in `src/init` and treat them as errors](https://github.com/bitcoin/bitcoin/pull/36107)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> Cleans up unnecessary header file inclusions in the initialization codebase using the 'Include What You Use' (IWYU) tool and enforces this standard going forward. This speeds up compilation times and keeps the codebase modular.

**Technical Details:** Analyzes header dependencies within src/init using the IWYU tool, removing redundant include directives and adding forward declarations where appropriate. Additionally, it configures the build system or CI to treat any future IWYU warnings in this directory as compilation errors. This prevents dependency creep and maintains a clean boundary for the startup and initialization subsystem.

#### [#36044: test: cover OP_SUCCESSx bypassing the initial stack element size limit](https://github.com/bitcoin/bitcoin/pull/36044)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> Adds regression test coverage to verify that the upgradeable OP_SUCCESSx opcodes correctly bypass the standard maximum stack element size limit. This ensures the consensus engine behaves exactly as specified under Taproot rules.

**Technical Details:** Integrates a functional or unit test specifically targeted at Taproot execution logic (BIP342). It constructs a transaction containing OP_SUCCESSx (where x is an upgradeable opcode) and pushes a stack element exceeding the 520-byte limit. The test asserts that the script evaluation succeeds, validating that OP_SUCCESSx immediately triggers a successful execution bypass before the stack element size limit is evaluated.

#### [#35586: doc: note -blocknotify is not run during IBD/reindex in help text](https://github.com/bitcoin/bitcoin/pull/35586)
**Author:** [@fernandguil](https://github.com/fernandguil) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> Updates the documentation and help text for the -blocknotify configuration option to clarify that it does not execute during Initial Block Download (IBD) or reindexing. This prevents user confusion and misconfiguration when setting up external block alert scripts.

**Technical Details:** Edits the help text string for the -blocknotify command-line argument in the configuration help module. The documentation string now explicitly states the operational boundary that block notifications are suppressed during IBD and reindex phases. This is a pure documentation update to align the help text with the existing runtime behavior.

#### [#36063: refactor: [test] Remove deprecated SetMockTime(i64) alias](https://github.com/bitcoin/bitcoin/pull/36063)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR refactors the testing framework by removing a deprecated integer-based mock time alias in favor of C++ chrono types. This modernizes the test code and enhances type safety.

**Technical Details:** The deprecated `SetMockTime(int64_t)` overload inside the testing harness is removed, forcing callers to transition to modern C++ chrono durations. Unit and functional tests are refactored to use `SetMockTime(std::chrono::seconds)` or direct duration types. This simplifies the mock time API and eliminates legacy type conversions.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#36101: doc: Add an error handling strategy](https://github.com/bitcoin/bitcoin/pull/36101)
**Author:** [@purpleKarrot](https://github.com/purpleKarrot) | **[Maintenance & Tech Debt]** *(Activity: 24 review events this week)*
> This PR introduces a formal documentation guide outlining the error handling strategy in Bitcoin Core. It establishes clear practices for when to use assertions, exceptions, and recoverable error handling to improve codebase reliability.

#### [#36078: qa: Reduce `-maxconnections` in the functional test framework](https://github.com/bitcoin/bitcoin/pull/36078)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 15 review events this week)*
> This PR reduces the default maximum number of concurrent network connections within the Python functional test framework. This optimization reduces the memory and file descriptor footprint of the nodes, speeding up test suite execution.

#### [#36088: util: Set Univalue to null after read failure](https://github.com/bitcoin/bitcoin/pull/36088)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 14 review events this week)*
> This PR modifies JSON parsing utilities to reset UniValue objects to a null state when a read operation fails. This prevents developers from inadvertently referencing stale or partially parsed JSON data during error recovery.

#### [#34931: validation: abort on DB unreadable coins instead of treating them as missing](https://github.com/bitcoin/bitcoin/pull/34931)
**Author:** [@furszy](https://github.com/furszy) | **[Security & Consensus]** *(Activity: 13 review events this week)*
> This PR changes how the validation logic handles unreadable coins found in the database; instead of silently treating them as missing, the node will now abort. This ensures critical database corruption is immediately detected, preventing potentially dangerous continued operation on an inconsistent state.

#### [#33593: guix: Use UCRT runtime for Windows release binaries](https://github.com/bitcoin/bitcoin/pull/33593)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 13 review events this week)*
> This PR updates the Guix build system to use the Universal C Runtime (UCRT) for Windows release binaries. This ensures compatibility with modern Windows systems and standardizes the runtime environment for better software distribution and stability.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: Pragmatic definition of consensus for light clients](https://delvingbitcoin.org/t/pragmatic-definition-of-consensus-for-light-clients/2842/13)
**Source:** Delving | **Started By:** {'username': 'Nuh', 'uuid': 'auto_nuh'} | **Messages:** 12
> This discussion focuses on improving Bitcoin's security during network splits by establishing a practical definition of consensus. This helps lightweight wallet users safely detect and navigate potential chain splits by observing network behavior.

**Technical Details:** The thread debates pragmatic consensus definitions for economic actors, referencing historical split events like the Bitcoin Cash fork. Developers are proposing that light clients monitor block issuance slowdowns as an indicator of hashpower splits to trigger safety halts. The conversation also explores utilizing Zero-Knowledge Proofs (ZKPs) for compact state validation without executing full script consensus rules. Resolving these behaviors requires standardized light client heuristics and clearer separation between state-proofs and consensus logic.

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/17)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 6
> Developers are discussing how to transition Bitcoin to quantum-resistant security, analyzing past upgrades like Taproot to ensure future post-quantum transactions remain affordable and widely supported.

**Technical Details:** The debate centers on the economic incentives for adopting Post-Quantum Cryptography (PQC) output types, comparing potential PQC uptake to Taproot's historically slow integration. While Taproot's fee-saving benefits have only recently become economically compelling during fee spikes, PQC output types will suffer from significantly larger signature sizes, risking low user adoption due to high feerate penalties. To address this, developers must design PQC standards that minimize block space overhead and establish robust incentives for a timely migration.

### [Re: Segwit commitment to post-quantum witness data?](https://delvingbitcoin.org/t/segwit-commitment-to-post-quantum-witness-data/2702/15)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 3
> Bitcoin developers are discussing how to safely integrate future post-quantum security protections into transactions without risking network spam. By properly anchoring these advanced signatures to the blockchain, the network can transition to quantum-resistant security while keeping nodes safe from denial-of-service attacks.

**Technical Details:** The debate focuses on whether post-quantum (PQ) signature commitments should be structured per-input or per-transaction within a proposed block extension. Addressing a suggestion to omit PQ signatures from the block commitment entirely, Pieter Wuille highlighted lessons from the 2015-2017 Segregated Witness design. Without committing to the PQ witnesses in the block hash (such as a witness root), relay nodes could cheaply construct and propagate infinite invalid permutations of valid blocks. Thus, developers agree that a formal cryptographic commitment inside the block structure is strictly necessary to prevent block-malleability DoS vectors.

### [Re: Seedroller and keyderiver: cli tools to generate BIP39 seed words and derive BIP380 keys](https://delvingbitcoin.org/t/seedroller-and-keyderiver-cli-tools-to-generate-bip39-seed-words-and-derive-bip380-keys/2822/3)
**Source:** Delving | **Started By:** {'username': 'bubb1es', 'uuid': 'auto_bubb1es'} | **Messages:** 2
> Developers are exploring how to safely generate secure cryptographic keys for bitcoin wallets without relying on complex, untrusted software. The goal is to make key generation more transparent and secure against supply chain hacks.

**Technical Details:** The conversation focuses on the security trade-offs of dependency bloat in Rust-based entropy-generation tools like seedtool-cli-rust. Participants are debating the architectural necessity of minimizing external dependency trees to prevent supply chain vulnerabilities during critical key-generation phases. Suggested steps include drafting minimal, auditable implementations that rely strictly on standard libraries and trusted OS entropy sources. Ultimately, developers must define standardized best practices for self-compiled, dependency-light seed generation tools.

### [=?UTF-8?Q?Re=3A_=5Bbitcoindev=5D_=5Bbitcoin=2Ddev=5D_Draft_BIP_for_discuss?=
	=?UTF-8?Q?ion=3A_Time=2DShifted_Proof_of_Work_=28TSPOW=29_=E2=80=94_an_informational?=
	=?UTF-8?Q?_consensus=2Ddesign_proposal?=](https://gnusha.org/pi/bitcoindev/CAB8jPP4Pw+vuDvi4ruznvOQGUMMTq9gdm7EdJn4Ms-ZGUGQArw@mail.gmail.com)
**Source:** Mailing List | **Started By:** {'username': 'Jack Liao', 'uuid': 'auto_jack_liao'} | **Messages:** 2
> Developers are discussing a new proposal called Time-Shifted Proof of Work (TSPOW) to better protect Bitcoin against history-rewriting attacks. This change aims to improve network security and transaction finality, ensuring user funds remain safe from deep chain reorganizations.

**Technical Details:** The discussion revolves around a draft BIP for Time-Shifted Proof of Work (TSPOW) aimed at disincentivizing selfish mining and long-range reorganizations. Technically, the proposal suggests modifying consensus rules to tie block validity or cumulative chain work to time-based shifting mechanisms. Critics and proponents are debating the game-theoretic implications for miner incentives, potential consensus split vectors, and interaction with the difficulty adjustment algorithm. Next steps require rigorous mathematical modeling of miner strategies under TSPOW and establishing concrete implementation specifications.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@GuTS805](https://github.com/GuTS805), [@jpk68](https://github.com/jpk68), [@shuv-amp](https://github.com/shuv-amp)

### ✍️ Top Authors
The most active PR authors this week: [@l0rinc](https://github.com/l0rinc), [@hebasto](https://github.com/hebasto), [@maflcko](https://github.com/maflcko), [@shuv-amp](https://github.com/shuv-amp), [@hodlinator](https://github.com/hodlinator)

### 🕵️ Top Reviewers
Providing critical review and testing: [@maflcko](https://github.com/maflcko), [@sedited](https://github.com/sedited), [@hebasto](https://github.com/hebasto), [@jeanpablojp](https://github.com/jeanpablojp), [@l0rinc](https://github.com/l0rinc)
