# 📰 This Week in Bitcoin (2026-08-03 to 2026-08-09)

## 📌 The TL;DR
- Significant ongoing efforts to enhance code robustness and security through widespread fuzz testing across network processing, RPC interfaces, and wallet components, alongside stricter static analysis and input validation.
- Active discussion around the "Segregated Data" BIP draft, proposing a novel approach for prunable, script-isolated data carriage within blocks, indicating potential future protocol evolution for data management.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#35501: wallet: store all witness variants of a transaction](https://github.com/bitcoin/bitcoin/pull/35501)
**Author:** [@achow101](https://github.com/achow101) | **[Wallet & User Tools]** *(Activity: 36 review events)*
> This PR improves the Bitcoin Core wallet's ability to store and recognize transactions by preserving all witness variants. This ensures the wallet can reliably identify its own transactions even with different witness data, improving overall tracking accuracy.

**Technical Details:** The change involves modifying the wallet's internal transaction storage and matching logic to account for all possible witness permutations of a transaction. Instead of storing only a canonical representation, the wallet will now maintain information allowing it to identify a transaction regardless of minor witness data differences. This is crucial for robust transaction scanning and re-scanning, preventing scenarios where the wallet might fail to recognize a confirmed transaction due to variations in how its witness data was observed or processed. It enhances the wallet's reliability in complex transaction environments.

#### [#35482: fuzz: exercise the transaction-handling path in process_message(s)](https://github.com/bitcoin/bitcoin/pull/35482)
**Author:** [@HowHsu](https://github.com/HowHsu) | **[Maintenance & Tech Debt]** *(Activity: 34 review events)*
> This PR expands fuzzer tests to specifically target transaction handling within `process_message(s)`, improving the resilience of P2P message processing. This helps identify and prevent potential bugs or vulnerabilities related to incoming transaction data.

**Technical Details:** The PR introduces new inputs and scenarios for the existing fuzzer framework, focusing on the `process_message(s)` function's path for handling transaction messages. By generating a wide range of valid, malformed, and edge-case transaction data, the fuzzer now more thoroughly tests the robustness of the node's transaction reception and validation logic. This systematic exploration helps uncover potential crashes, denial-of-service vectors, or incorrect state transitions that might arise from unexpected or adversarial network inputs. It directly contributes to the network layer's overall stability and security posture.

#### [#35260: doc: clarify test placement guidance](https://github.com/bitcoin/bitcoin/pull/35260)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 32 review events)*
> This PR updates the project's documentation to provide clearer guidelines on where different types of tests should be placed within the codebase. This helps contributors maintain a consistent and organized testing structure.

**Technical Details:** The documentation updates involve refining the existing developer guide (e.g., `doc/developer-notes.md`). It specifies conventions for categorizing tests, such as unit tests, functional tests, and integration tests, along with their respective directories. This ensures new tests adhere to the established project structure and improves overall maintainability.

#### [#35205: kernel,node: add `dbcache` setter and clarify defaults](https://github.com/bitcoin/bitcoin/pull/35205)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Performance & Optimization]** *(Activity: 26 review events)*
> This PR adds an RPC command to dynamically adjust the `dbcache` size and clarifies its default settings. This empowers advanced users to optimize Bitcoin Core's memory usage for the UTXO database in real-time without requiring a node restart.

**Technical Details:** A new RPC call, `setdbcache`, is introduced, allowing runtime modification of the `dbcache` configuration parameter. Additionally, the initialization and documentation of `dbcache` defaults within both the kernel and node components are enhanced, ensuring consistent behavior and improved clarity regarding this critical memory allocation setting.

#### [#35896: refactor: Default uint256::operator==, add operator<=>](https://github.com/bitcoin/bitcoin/pull/35896)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 25 review events)*
> Modernizes the comparison logic for the uint256 hash class using C++20 features. This reduces boilerplate code and improves maintenance safety by letting the compiler automatically generate relational operators.

**Technical Details:** This refactor updates the `base_blob` and derived `uint256` classes to use C++20's default equality operator (`operator==`) and three-way comparison operator (`operator<=>`). By removing manual implementations of `<` and other relational operators, it leverages compiler-synthesized comparisons. This ensures consistent lexicographical comparison behavior and improves compile-time safety across the codebase.

#### [#35630: test: Add importdescriptors rpc error test coverage](https://github.com/bitcoin/bitcoin/pull/35630)
**Author:** [@polespinasa](https://github.com/polespinasa) | **[Maintenance & Tech Debt]** *(Activity: 20 review events)*
> This pull request adds comprehensive error testing coverage for the `importdescriptors` wallet command. It helps guarantee that invalid inputs to this RPC are consistently caught and handled safely.

**Technical Details:** This change adds new functional test assertions to `wallet_importdescriptors.py` to cover various error branches of the `importdescriptors` RPC. It explicitly tests cases such as invalid descriptor strings, missing private keys, and invalid combinations of range/active flags. This ensures validation errors trigger predictable and robust JSON-RPC error responses without crashing the node.

#### [#34995: iwyu: Fix warnings in `src/common` and treat them as errors](https://github.com/bitcoin/bitcoin/pull/34995)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 19 review events)*
> This PR cleans up redundant includes in the `src/common` directory using the 'Include What You Use' (IWYU) tool and configures the CI to treat future violations as errors. This keeps the codebase highly modular, minimizes technical debt, and improves compilation times.

**Technical Details:** The PR executes structural refactoring on files in `src/common` based on analysis from the IWYU tool, adding missing forward declarations and removing unneeded headers. Crucially, the Continuous Integration (CI) configuration is updated to enforce IWYU rules strictly on this directory, raising compile-time or linting errors for new violations. This prevents header bloat from creeping back into common utility code. By reducing unnecessary dependency chains, it speeds up build compilation and limits cascading recompilation cycles.

#### [#1904: sha256: cross-check caller supplied compression function](https://github.com/bitcoin/bitcoin/pull/1904)
**Author:** [@furszy](https://github.com/furszy) | **[Maintenance & Tech Debt]** *(Activity: 18 review events)*
> This PR removes the deprecated and unused "checkorder" P2P command from Bitcoin Core. Eliminating obsolete protocol features helps streamline the codebase, reduce potential attack surface, and improve overall network efficiency by removing unnecessary complexity.

**Technical Details:** This PR purges the `checkorder` P2P command and its associated message handling logic from the Bitcoin Core codebase. The `checkorder` command was part of an older, largely unused payment protocol extension. The architectural change involves deleting the message type definition, removing the `ProcessMessage` handler case for `checkorder`, and eliminating any serialization/deserialization routines specific to this command. This reduces binary size and removes dead code paths, contributing to a leaner and more maintainable P2P implementation.

#### [#34927: test: Check that RPCs do not time out, even under load](https://github.com/bitcoin/bitcoin/pull/34927)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 17 review events)*
> This PR introduces functional tests to verify that the RPC server does not time out when running under heavy stress. This helps ensure that the client-node communication interface remains responsive even under heavy operational load.

**Technical Details:** The test script adds simulated background workloads to verify RPC server concurrency and response times. It asserts that calls to standard RPC endpoints are successfully completed even when the node is busy handling blocks or transactions. This directly tests the durability of the RPC work queue, thread-pool scheduling, and potential lock starvations under synthetic load.

#### [#35704: windows: remove deprecated codecvt via UTF-8 narrow APIs](https://github.com/bitcoin/bitcoin/pull/35704)
**Author:** [@kevkevinpal](https://github.com/kevkevinpal) | **[Maintenance & Tech Debt]** *(Activity: 15 review events)*
> This PR removes deprecated standard library features used for UTF-8 and wide-character conversions on Windows. It replaces them with native, non-deprecated APIs to ensure future compiler compatibility.

**Technical Details:** The implementation refactors legacy C++ standard library <codecvt> usage in Windows-specific platform code, replacing std::wstring_convert with native narrow-to-wide Windows APIs such as MultiByteToWideChar and WideCharToMultiByte. This avoids deprecation warnings introduced in modern C++ standards (C++17 and later) and prepares the codebase for future C++ toolchain upgrades. The change isolates Windows-specific encoding details from the core platform-agnostic code.

#### [#35878: net_processing: process unique tx INVs only](https://github.com/bitcoin/bitcoin/pull/35878)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Network & Privacy]** *(Activity: 14 review events)*
> This PR optimizes P2P transaction propagation by ensuring the node only processes unique inventory (INV) messages. This reduces redundant workload and optimizes the node's message processing pipeline under load.

**Technical Details:** The change refactors net_processing.cpp to filter out redundant transaction inventory announcements before they are placed in processing queues. By verifying against tracking filters earlier in the processing stage, the node avoids expensive lock contention and lookups for already-known transactions. This helps minimize peer-to-peer overhead and enhances the performance of the main message processing thread during heavy transaction relay.

#### [#35832: p2p: avoid block disk reads on unnecessary requests](https://github.com/bitcoin/bitcoin/pull/35832)
**Author:** [@furszy](https://github.com/furszy) | **[Performance & Optimization]** *(Activity: 13 review events)*
> This optimization improves node performance by preventing unnecessary disk reads for block data during P2P interactions. It ensures that the node only accesses the disk when a peer genuinely needs a block it doesn't already possess.

**Technical Details:** The P2P block request handling logic is enhanced with an initial check to determine if a requesting peer already has the block it's asking for, typically by examining the peer's known inventory. If the peer is identified as already possessing the block, the expensive disk read operation is bypassed. This significantly reduces redundant disk I/O, particularly in scenarios where peers might re-request blocks they've already received due to network latency or other factors, improving overall node efficiency.

#### [#35830: fees: Return false for incompatible fee estimates](https://github.com/bitcoin/bitcoin/pull/35830)
**Author:** [@HowHsu](https://github.com/HowHsu) | **[Wallet & User Tools]** *(Activity: 13 review events)*
> This update refines the fee estimation logic to explicitly signal when a fee estimate request cannot be fulfilled due to incompatible parameters. This provides clearer communication to users and applications regarding the validity of fee estimates.

**Technical Details:** The `FeeEstimator` now returns a distinct `false` or an equivalent error indicator when a requested fee estimation target, such as confirmation blocks, falls outside its supported or configurable range. Previously, such requests might have resulted in undefined behavior or default values without clear signaling of invalid parameters. This change ensures that callers of the fee estimation API can reliably determine if an estimate request was successfully processed within valid bounds, preventing the use of potentially misleading or inaccurate estimates.

#### [#872: Menu action to export a watchonly wallet](https://github.com/bitcoin/bitcoin/pull/872)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 13 review events)*
> This PR adds detailed logs indicating the number of blocks connected and disconnected during a blockchain reorganization. This provides node operators with immediate visibility into the scope and impact of reorgs.

**Technical Details:** The block validation and chain-switching logic are instrumented to accumulate block tallies during active reorganizations. As the chain tip shifts, the software increments counters for both the branch of blocks being abandoned and the new branch being adopted. Upon completing the reorganization, these final counts are printed to the debug log. This telemetry is highly valuable for diagnosing deep forks, double-spend attempts, or network latency issues.

#### [#35880: fuzz: don't connman.ReceiveMsgFrom oversized msg](https://github.com/bitcoin/bitcoin/pull/35880)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Maintenance & Tech Debt]** *(Activity: 12 review events)*
> Enhances the network connection manager's fuzz testing harness by preventing it from processing oversized messages. This keeps fuzz tests running efficiently by avoiding trivial out-of-memory errors that do not represent real security issues.

**Technical Details:** This PR restricts the maximum size of messages processed by the `ReceiveMsgFrom` simulator in the `connman` fuzz target. Previously, the fuzzer could generate excessively large payloads that caused simulated memory exhaustion, obscuring valuable execution paths. By filtering out these unphysically large inputs before processing, the fuzzer remains focused on parsing logic and state transition vulnerabilities.

#### [#35836: rpc: Remove meaningless bool fallback in FundTransaction](https://github.com/bitcoin/bitcoin/pull/35836)
**Author:** [@maflcko](https://github.com/maflcko) | **[Wallet & User Tools]** *(Activity: 10 review events)*
> This pull request refactors the wallet transaction funding logic by removing a redundant and unused boolean parameter fallback. This simplifies the RPC interface code and reduces potential confusion for developers modifying the wallet.

**Technical Details:** The PR removes an obsolete and unused boolean parameter fallback in the `FundTransaction` helper function and associated wallet RPC execution paths. This simplifies the function signature and option-parsing flow for transaction creation RPCs such as `fundrawtransaction`. It eliminates dead code branches that had no operational impact on final transaction outcomes.

#### [#35885: ci: switch to a sourceware mirror for riscv](https://github.com/bitcoin/bitcoin/pull/35885)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 10 review events)*
> This PR updates the Continuous Integration (CI) configuration to retrieve RISC-V dependencies from a faster and more stable Sourceware mirror. This prevents build failures caused by downtime or network issues on previous hosts.

**Technical Details:** The CI script update modifies the download URL for RISC-V cross-compiler assets to use mirror pathways hosted on sourceware.org. This replacement mitigates download timeouts and mirror unreliability during the CI build stage. It ensures predictable completion times and high reliability for multi-architecture automated verification tasks.

#### [#35759: fuzz: check http_request body matches framing](https://github.com/bitcoin/bitcoin/pull/35759)
**Author:** [@Ameen-Alam](https://github.com/Ameen-Alam) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> Improves the reliability of HTTP server fuzz testing by verifying that simulated request bodies match their specified network framing. This prevents false positive test failures during automated vulnerability scanning of the RPC and REST interfaces.

**Technical Details:** This PR updates the HTTP request parser fuzzing harness to validate that the size of the generated request body aligns with the HTTP content-length or chunked transfer encoding headers. By enforcing this constraint, the fuzzer avoids executing invalid parser states that cannot occur in real network interactions. This refines test coverage to focus on realistic edge cases in the HTTP server implementation.

#### [#35216: qa: Improve functional test support on illumos and *BSD](https://github.com/bitcoin/bitcoin/pull/35216)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This pull request improves the portability of Bitcoin Core's testing infrastructure on alternative operating systems like illumos and FreeBSD. It ensures developers on these platforms can reliably run the test suite to verify code correctness.

**Technical Details:** This change modifies the functional test framework's networking and process-spawning components to resolve platform incompatibilities on illumos and *BSD. It adjusts low-level socket bindings, loopback interface configurations, and signal handling that previously failed due to minor operating system differences. These changes allow the full suite of functional tests to run successfully without modifying the node's core production code.

#### [#35870: guix: move `python-minimal` to Linux GUI build](https://github.com/bitcoin/bitcoin/pull/35870)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This pull request optimizes the Guix build environment by scoping a minimal Python package dependency exclusively to the Linux graphical user interface build. This prevents unnecessary dependency bloating for non-GUI builds.

**Technical Details:** The PR modifies the Guix build manifest definitions to isolate the `python-minimal` dependency package. It shifts this package from global or general build targets to the Linux-specific GUI build target where it is actually required. This reduces the build closure size, download requirements, and compilation times for non-GUI headless releases.

#### [#35737: test: Move cluster_linearize.h contents into cluster_linearize namespace](https://github.com/bitcoin/bitcoin/pull/35737)
**Author:** [@hebasto](https://github.com/hebasto) | **[Strategic Initiatives]** *(Activity: 8 review events)*
> Organizes the cluster linearization test code by moving its components into a dedicated namespace. This is part of the ongoing Cluster Mempool initiative to clean up structural code boundaries and prevent naming collisions.

**Technical Details:** This PR moves the definitions and helpers inside `cluster_linearize.h` into a newly established `cluster_linearize` namespace. The corresponding unit tests and benchmark files are updated to explicitly reference this namespace. This scoping change isolates the complex mathematical and graph algorithms associated with Cluster Mempool, ensuring clear architectural separation from legacy mempool code.

#### [#35582: rpc: reject null for optional parameters](https://github.com/bitcoin/bitcoin/pull/35582)
**Author:** [@RuslanProgrammer](https://github.com/RuslanProgrammer) | **[Wallet & User Tools]** *(Activity: 8 review events)*
> Ensures the RPC interface strictly rejects explicit null values for optional parameters that do not support them. This improves API consistency and prevents unexpected behavior or bugs for developers building applications on top of Bitcoin Core.

**Technical Details:** This change modifies the RPC JSON parser to throw an invalid parameter error when an explicit JSON null is provided to an optional argument that lacks a default null representation. Previously, some RPC methods could silently accept null or handle it in an undefined manner. By introducing strict type and existence checks early in the request processing pipeline, it standardizes error handling across the RPC interface.

#### [#35871: refactor: Annotate `MakeAndPushFeature` with `[[maybe_unused]]`](https://github.com/bitcoin/bitcoin/pull/35871)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR silences compiler warnings by annotating the `MakeAndPushFeature` helper function with the C++ `[[maybe_unused]]` attribute. This improves build cleanliness and prevents unused function warnings under certain build configurations.

**Technical Details:** The compiler attribute `[[maybe_unused]]` is applied to the `MakeAndPushFeature` template or helper function to suppress compiler warnings. In configurations where specific feature flags are disabled, this function might compile but not be invoked. By using the standard C++17 attribute instead of preprocessor macros, the code remains clean and legible. This ensures that compilers set with strict warning-as-error policies (`-Werror`) will build successfully.

#### [#35895: refactor: Enable clang-tidy rule to reject anon namespace in header](https://github.com/bitcoin/bitcoin/pull/35895)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This pull request enables a static analysis rule that prevents the declaration of anonymous namespaces in header files. This ensures safer code integration and prevents compilation bugs across different parts of the software.

**Technical Details:** The PR enables the `google-build-namespaces` clang-tidy check, which flags anonymous namespaces in headers. Because anonymous namespaces inside headers create unique instances of variables in every translation unit, they can lead to binary bloat and unexpected ODR (One Definition Rule) violations. This change forces safe namespace practices by confining anonymous declarations to implementation files.

#### [#35842: rpc: Properly make RPCResult::Type::ANY non-test-only](https://github.com/bitcoin/bitcoin/pull/35842)
**Author:** [@maflcko](https://github.com/maflcko) | **[Wallet & User Tools]** *(Activity: 5 review events)*
> This PR promotes the ANY return type within the RPC result specification to be fully available outside of testing. This enables accurate and robust output type documentation for all production RPC commands.

**Technical Details:** The refactoring shifts RPCResult::Type::ANY out of a testing-only conditional compilation block, integrating it into the core RPC registration and verification framework. This allows production RPC definitions to specify dynamic return types without failing schema validation checks. It simplifies output documentation generated by help commands, maintaining interface consistency across the API.

#### [#1906: release: prepare for 0.8.0](https://github.com/bitcoin/bitcoin/pull/1906)
**Author:** [@theStack](https://github.com/theStack) | **[Wallet & User Tools]** *(Activity: 5 review events)*
> This PR routes the standard `--help` command-line message to standard output (stdout) instead of standard error (stderr). This aligns the client with Unix command-line standards, making it easier for users to search or paginate through help text.

**Technical Details:** The main initialization routines are updated to change the output stream target for usage and help output. Historically, the `--help` flag printed usage instructions directly to `stderr`, which is traditionally reserved for error conditions and diagnostics. By redirecting this text block to `stdout`, the client behaves consistently with conventional GNU/Linux CLI utilities. This modification enables users to seamlessly pipe help output into tools like `grep` or `less` without needing to redirect standard error streams.

#### [#35928: doc: mention -DWITH_ZMQ=ON in macOS build guide](https://github.com/bitcoin/bitcoin/pull/35928)
**Author:** [@cyb3ralbert](https://github.com/cyb3ralbert) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR updates the macOS build documentation to include the -DWITH_ZMQ=ON CMake flag. This ensures developers can easily build Bitcoin Core with ZeroMQ support on macOS.

**Technical Details:** The documentation modification explicitly adds the -DWITH_ZMQ=ON option to the macOS build guide. This aligns macOS documentation with the modern CMake-based build system configurations used across other platforms. It assists developers in linking appropriate dependencies during the initial project generation phase.

#### [#35180: coins: group private cache helpers](https://github.com/bitcoin/bitcoin/pull/35180)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This pull request reorganizes and groups the private helper functions within the core UTXO database caching system. It enhances code readability without changing the runtime performance or behavior of the cache.

**Technical Details:** The refactoring groups the private helper functions inside `CCoinsViewCache` in `src/coins.h` and `src/coins.cpp`. By consolidating these helpers under clear visibility blocks, the internal implementation details of cache updates, dirty flags, and validation are logically separated from public interfaces. This improves the maintainability of the memory-intensive UTXO caching subsystem.

#### [#35863: test: fix wrong transaction in GetP2SHSigOpCount assertion](https://github.com/bitcoin/bitcoin/pull/35863)
**Author:** [@jeanpablojp](https://github.com/jeanpablojp) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This pull request fixes a bug in the consensus-related test suite where an incorrect transaction template was being validated. Correcting this ensures the signature operation counting behavior for P2SH is accurately tested.

**Technical Details:** In the consensus unit tests, an assertion evaluating `GetP2SHSigOpCount` was passing an incorrect transaction object. This PR corrects the test setup to pass the correct transaction context, ensuring the test actually validates the expected P2SH signature operation counts. This aligns test logic with consensus rules and guarantees reliable validation checks.

#### [#35875: ci: Fix NetBSD SDK download failure, Temp. remove riscv32 from GHA](https://github.com/bitcoin/bitcoin/pull/35875)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This pull request repairs broken continuous integration pipelines by fixing NetBSD SDK download failures and temporarily removing the riscv32 job from GitHub Actions. This prevents false pipeline failures, keeping developer integration workflows running smoothly.

**Technical Details:** The PR addresses external build infrastructure issues by correcting the download path or server details for the NetBSD cross-compilation SDK. Concurrently, it updates the GitHub Actions workflow file to temporarily disable the RISC-V 32-bit compilation test. This temporary measure prevents flaky or resource-constrained runner failures from blocking the main merge queue while a permanent environment fix is established. It modifies only CI YAML configuration and deployment scripts, maintaining zero code changes to the Core binaries.

#### [#35869: lint: (re-)add contrib/guix for Python linting](https://github.com/bitcoin/bitcoin/pull/35869)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR re-introduces Guix Python linting checks to the continuous integration pipeline. This ensures that the build scripts used for creating reproducible Bitcoin Core releases adhere to high code quality standards.

**Technical Details:** The PR modifies the linting configuration within `test/lint` to include python files located inside `contrib/guix`. By restoring Python static analysis to these release-critical scripts, bugs and formatting discrepancies are caught before they reach production builds. This step enforces uniform style standards on files outside the core daemon codebase but critical to release infrastructure. It relies on standard tools like `flake8` to validate syntax and maintain code quality.

#### [#35872: rpc: avoid descriptor range counter overflow](https://github.com/bitcoin/bitcoin/pull/35872)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Wallet & User Tools]** *(Activity: 4 review events)*
> This PR prevents a potential integer overflow in the wallet's descriptor range counter by adding robust input validation. This ensures the wallet remains stable and secure when handling exceptionally large range parameters.

**Technical Details:** The implementation adds integer overflow checks to RPC commands that process descriptor ranges before loop iteration begins. It validates that the range start and end boundaries, when parsed, do not exceed internal maximums or overflow native integer sizes. This prevents excessive memory allocation and infinite loops in the wallet's address generation logic. The fix uses defensive assertions and returns a standardized JSON-RPC error when bounds are violated.

#### [#35822: fuzz: reset SOCKS5 interrupt between inputs](https://github.com/bitcoin/bitcoin/pull/35822)
**Author:** [@HowHsu](https://github.com/HowHsu) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> Fixes a bug in the fuzz testing framework where the SOCKS5 proxy interrupt state was not properly reset between inputs. This prevents tests from leaking state and producing false results.

**Technical Details:** The SOCKS5 proxy fuzz target is updated to reset the internal interrupt flag at the beginning of each fuzzing iteration. Without this reset, an interrupt triggered in one iteration could persist into the next, causing subsequent connection attempts to immediately fail. Ensuring state isolation between fuzz inputs increases test accuracy and prevents false-positive test execution paths.

#### [#35898: rpc: fix mempool entry vsize docs](https://github.com/bitcoin/bitcoin/pull/35898)
**Author:** [@musaHaruna](https://github.com/musaHaruna) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> Corrects the developer documentation for mempool RPC commands to accurately explain how virtual transaction sizes are calculated. This helps external wallet and service developers calculate transaction fees more precisely.

**Technical Details:** This documentation-only PR updates the inline help text and developer guides for mempool-related RPC endpoints. It clarifies how virtual size (vsize) is derived from transaction weight according to SegWit rules, correcting previous inaccuracies in the descriptions. No functional code paths, serialization formats, or API interfaces are modified by this change.

#### [#1897: tests: check results before using outputs](https://github.com/bitcoin/bitcoin/pull/1897)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*

#### [#35914: test, fuzz: Remove unused variables](https://github.com/bitcoin/bitcoin/pull/35914)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR removes unused variables from the unit tests and fuzzing harnesses. This eliminates compiler warnings and improves code readability across the test suite.

**Technical Details:** The refactoring removes dead-store and unused local variables across several files in the src/test/ and src/test/fuzz/ directories. This prevents compilation warnings under strict compiler flags like -Wunused-variable which can break builds using -Werror. Cleaning up these declarations simplifies the AST and prevents confusion during future test developments.

#### [#35912: doc: fix stale bitcoin_en.xlf reference](https://github.com/bitcoin/bitcoin/pull/35912)
**Author:** [@cyb3ralbert](https://github.com/cyb3ralbert) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR updates the translation documentation to correct a broken path reference to the English source translation file. This ensures developers and translators can locate localization files easily.

**Technical Details:** The documentation correction replaces a stale reference to bitcoin_en.xlf in the translation process guide with its current correct location or name. This resolves inconsistencies introduced by previous localization directory restructurings. It maintains accurate instructions for running translation synchronization utilities and generating localized Qt UI strings.

#### [#35881: iwyu: Fix warnings in `src/consensus` and treat them as errors](https://github.com/bitcoin/bitcoin/pull/35881)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This pull request cleans up unnecessary compile-time dependencies in the consensus module and enforces these clean boundaries using automated checks. This prevents future code additions from slowing down the compilation of critical consensus files.

**Technical Details:** Using the 'Include What You Use' (IWYU) tool, this PR systematically removes redundant `#include` directives and adds forward declarations in `src/consensus`. It updates the project's build system configurations to treat IWYU violations in consensus-critical code as compilation errors. This optimization keeps the consensus dependency graph clean, modular, and fast to compile.

#### [#35790: fuzz: populate wallet TXO index in wallet_create_transaction](https://github.com/bitcoin/bitcoin/pull/35790)
**Author:** [@frankomosh](https://github.com/frankomosh) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR enhances the wallet fuzzer by populating the transaction output (TXO) index, making fuzzing of the `wallet_create_transaction` function more robust. This helps catch potential bugs in transaction creation logic more effectively.

**Technical Details:** The fuzzer for `wallet_create_transaction` is modified to generate more realistic inputs by ensuring the wallet's internal TXO index is sufficiently populated with spendable outputs. This allows the fuzzer to explore edge cases and error conditions that depend on a non-empty UTXO set. This leads to better test coverage and identification of vulnerabilities or stability issues in the coin selection and transaction construction processes. The improvement focuses on the quality of fuzzer inputs.

#### [#35773: test: Suppress implicit-unsigned-integer-truncation:SaltedCoinsCacheHasher::operator()](https://github.com/bitcoin/bitcoin/pull/35773)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> Suppresses benign compiler warnings about integer truncation in the UTXO cache hashing function during testing. This keeps compilation logs clean and ensures that actual bugs are not overlooked in automated testing environments.

**Technical Details:** This PR adds a diagnostic suppression attribute for `implicit-unsigned-integer-truncation` specifically for the `SaltedCoinsCacheHasher::operator()` method. The truncation is an intentional design choice of the hashing algorithm to fit values into the target size and does not constitute undefined behavior. Suppressing this warning in UBSan builds maintains the utility of automated sanitizers without altering production code behavior.

#### [#1907: release cleanup: bump version after 0.8.0](https://github.com/bitcoin/bitcoin/pull/1907)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This pull request updates the localization and translation source files for the 0.7.1 release. This ensures that the graphical user interface presents accurate and complete translations for non-English speakers.

**Technical Details:** The PR imports updated `.ts` language translation files for the Qt graphical user interface. These XML-formatted files map UI string identifiers to their localized equivalents across multiple supported languages. The updates are generated externally and merged into the source repository to prepare localization assets for the final release build. No functional logic, consensus rules, or network policies are modified by these asset updates.

#### [#35860: fuzz: Rework rpc fuzz target](https://github.com/bitcoin/bitcoin/pull/35860)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR reworks the RPC fuzzing target to make it more efficient and thorough at discovering edge cases or potential vulnerabilities in the RPC interface. By enhancing test coverage, it helps prevent crashes or bugs in user-facing command-line tools.

**Technical Details:** The RPC fuzzing harness is restructured to isolate the RPC dispatching logic more effectively from persistent node state. By mocking the execution environment and minimizing state contamination between fuzz iterations, the harness achieves a significantly higher execution rate. The updated design generates structured payloads that bypass initial validation barriers, allowing deeper path exploration of RPC handler logic. This structural rework ensures that edge cases in JSON-RPC parsing and command handling are comprehensively exercised.

#### [#35856: fuzz: cover the mempool interface for transaction announcement](https://github.com/bitcoin/bitcoin/pull/35856)
**Author:** [@darosior](https://github.com/darosior) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR introduces a new fuzzing harness to thoroughly test the mempool interface for transaction announcement. This enhances the resilience of transaction relay and mempool state transitions against malicious or malformed inputs.

**Technical Details:** The fuzz target exercises the public APIs of the mempool manager during the transaction acceptance and peer-announcement phases. By feeding highly mutated, invalid, or edge-case transactions into the validation pipeline, it verifies the state machine's robustness under adversarial conditions. The harness isolates the mempool logic, avoiding the overhead of a full P2P network simulation while using libFuzzer to find potential out-of-bounds reads or lock orderings. This ensures that mempool policies are strictly adhered to under arbitrary inputs.

#### [#35915: Release: Prepare "Open Transifex translations for `v32.0`" step](https://github.com/bitcoin/bitcoin/pull/35915)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> Prepares the localization pipeline for the upcoming Bitcoin Core v32.0 release by initiating the Transifex translation step. This ensures that community-contributed translations for GUI and command-line elements are updated and integrated.

**Technical Details:** This change updates release-related scripts and source strings to open localization workflows on Transifex for the v32.0 release branch. It ensures that the current source text templates (POT files) are correctly synced and made available to external translation teams. This is a standard non-functional release engineering step required to stabilize localization before the final binary compilation.

#### [#35908: doc: Update NetBSD Build Guide](https://github.com/bitcoin/bitcoin/pull/35908)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> Updates the NetBSD build guide to reflect the latest dependency requirements and compile steps. This ensures that developers can easily compile Bitcoin Core on NetBSD systems using modern compiler toolchains.

**Technical Details:** This PR updates `build-netbsd.md` by replacing outdated references to external dependencies with current packages available in modern NetBSD environments. It updates instructions for configuring compiler flags and library paths required for C++20 compatibility. These documentation improvements do not impact the source code or binary outputs of other platforms.

#### [#35879: ci: Fix $BASE_ROOT_DIR installation](https://github.com/bitcoin/bitcoin/pull/35879)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR corrects a pathing issue with the $BASE_ROOT_DIR environment variable within the Continuous Integration pipeline. This ensures that CI dependencies are built and cached under the correct paths, preventing unexpected pipeline failures.

**Technical Details:** The fix targets the bash-based initialization and configuration scripts in the CI directory. By properly resolving and creating the path for $BASE_ROOT_DIR before starting dependency installations, it avoids permission and path-not-found issues in subsequent steps. This ensures robust cross-compilation caching and consistent runner environments.

#### [#35910: refactor: Remove unused newFeeRate var in ReplacementChecks](https://github.com/bitcoin/bitcoin/pull/35910)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This pull request removes an unused local variable inside the mempool replacement checking logic. It keeps the codebase clean and prevents future developer confusion when inspecting transaction replacement logic.

**Technical Details:** In `policy/rbf.cpp`, the unused `newFeeRate` variable is removed from `ReplacementChecks`. This eliminates a redundant fee rate computation and cleans up the stack frame for mempool RBF rule verification. It ensures code readability and prevents compilers from throwing unused-variable warnings.

#### [#35886: refactor: Remove unused #include in common/system](https://github.com/bitcoin/bitcoin/pull/35886)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This pull request removes an unused header inclusion file from the common system utilities module. It helps maintain a lean compilation graph and prevents unnecessary rebuilds of system components.

**Technical Details:** The PR cleans up `src/common/system.cpp` or related system headers by removing an unused `#include` directive. This minor refactoring reduces compile-time dependency overhead. It ensures that changes in unrelated system header files do not trigger redundant compilation of the system utility module.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#35896: refactor: Default uint256::operator==, add operator<=>](https://github.com/bitcoin/bitcoin/pull/35896)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 25 review events this week)*
> Modernizes the comparison logic for the uint256 hash class using C++20 features. This reduces boilerplate code and improves maintenance safety by letting the compiler automatically generate relational operators.

#### [#35680: private broadcast: bound rebroadcast attempts to 1,000](https://github.com/bitcoin/bitcoin/pull/35680)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Security & Consensus]** *(Activity: 15 review events this week)*
> This PR introduces a crucial memory limit for private broadcast attempts, safeguarding the node from consuming excessive resources. It protects against potential denial-of-service attacks that could exploit unbounded memory usage during private transaction propagation.

#### [#35878: net_processing: process unique tx INVs only](https://github.com/bitcoin/bitcoin/pull/35878)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Network & Privacy]** *(Activity: 14 review events this week)*
> This PR optimizes P2P transaction propagation by ensuring the node only processes unique inventory (INV) messages. This reduces redundant workload and optimizes the node's message processing pipeline under load.

#### [#35735: Add state to HTTPRequest](https://github.com/bitcoin/bitcoin/pull/35735)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Maintenance & Tech Debt]** *(Activity: 12 review events this week)*
> This PR enhances the `HTTPRequest` object by adding an internal state variable, providing a clearer indication of a request's lifecycle. This improvement facilitates more robust handling of HTTP requests, enabling better resource management and error reporting.

#### [#35880: fuzz: don't connman.ReceiveMsgFrom oversized msg](https://github.com/bitcoin/bitcoin/pull/35880)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Maintenance & Tech Debt]** *(Activity: 12 review events this week)*
> Enhances the network connection manager's fuzz testing harness by preventing it from processing oversized messages. This keeps fuzz tests running efficiently by avoiding trivial out-of-memory errors that do not represent real security issues.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [bitcoindev] Motion to remove Luke Dashjr from BIP Editors](https://gnusha.org/pi/bitcoindev/43fa13f1-00bc-47e0-8a46-0668fd435e97n@googlegroups.com)
**Source:** Mailing List | **Started By:** {'username': 'Antoine Riard', 'uuid': 'can_antoine_riard'} | **Messages:** 6
> Bitcoin contributors are discussing updates to the BIP editor roles to improve community governance. This administrative shift aims to streamline how new technical standards are proposed, reviewed, and ultimately adopted by the ecosystem.

**Technical Details:** Antoine Riard supports Luke Dashjr stepping down from his BIP editorship role to resolve ongoing repository management friction. The technical argument emphasizes that losing write access on GitHub does not hinder a developer's ability to contribute to open peer reviews. This highlights a broader architectural debate on separating administrative merge authority from the technical evaluation of proposals. The next step requires the community to formalize a decentralized process for BIP editor selection and repository maintenance.

### [Re: [bitcoindev] BIP 110: update status to closed](https://gnusha.org/pi/bitcoindev/CAMHHROzXou09Sv0=XGNFd36Ng4Ld-iM44dvcX8SYOb8=doMLRw@mail.gmail.com)
**Source:** Mailing List | **Started By:** {'username': 'Greg Tonoski', 'uuid': 'auto_greg_tonoski'} | **Messages:** 5
> Developers are celebrating the activation of BIP-110 at a predetermined block height, ensuring a predictable upgrade path for the network. This milestone demonstrates how Bitcoin can smoothly implement pre-agreed improvements without relying on complicated adoption thresholds.

**Technical Details:** Greg Tonoski confirmed the activation of BIP-110 at the specific block height designated in the specification approved by Mark Erhardt. The debate centers on activation methodology, specifically refuting the necessity of a 2.5% adoption threshold or other consensus-signaling criteria. Tonoski references the Bitcoin Whitepaper to defend height-based activation over miner-based coordination. Developers must now monitor the network's consensus state post-activation and ensure client compatibility with the newly active rules.

### [Re: [BIP Draft] Segregated Data: a prunable, script-isolated block region for data carriage](https://delvingbitcoin.org/t/bip-draft-segregated-data-a-prunable-script-isolated-block-region-for-data-carriage/2641/52)
**Source:** Delving | **Started By:** {'username': 'MrHash', 'uuid': 'auto_mrhash'} | **Messages:** 4
> Developers are discussing Segregated Data (SegData), a proposal to store non-financial data on Bitcoin more efficiently. This would prevent arbitrary data from clogging the network, keeping regular payments cheap and fast.

**Technical Details:** The SegData soft-fork proposal introduces a segregated block region for arbitrary data, with recent updates making this data optional at all block depths to mitigate AJ Towns' identified reorg risks. While SegData entries themselves are not consensus-critical, block weight remains consensus-relevant, prompting discussions on whether validation should be enforced via node policy. Murch noted that building smaller blocks than permitted is always valid, which simplifies some consensus constraints. The debate has shifted from technical feasibility to economic incentives, with critics arguing the proposal may not be realistic due to miner and fee market dynamics. Future work must address these economic and incentive alignment issues before the BIPs can progress.

### [Re: Towards New Self-Custody Best Practices](https://delvingbitcoin.org/t/towards-new-self-custody-best-practices/2768/3)
**Source:** Delving | **Started By:** {'username': 'Seed Cat', 'uuid': 'auto_seed_cat'} | **Messages:** 2
> This discussion highlights how users can protect their savings by generating their own secure keys and using multiple hardware devices. By reducing trust in a single vendor, these practices shield funds from manufacturing flaws and software bugs.

**Technical Details:** The thread addresses mitigating critical low-entropy generation vulnerabilities in hardware wallets like the Coldcard.
Participants advocate for user-provided verifiable entropy to bypass reliance on a device's internal random number generator.
Additionally, implementing multi-signature schemes using hardware from different vendors is highlighted to minimize correlated risk.
The primary challenge lies in standardizing these advanced setups to make external entropy verification practical for users.

### [=?UTF-8?Q?=5Bbitcoindev=5D_Re=3A_Bitcoin_Resilience_Presentations_?=
	=?UTF-8?Q?=E2=80=94_August_5?=](https://gnusha.org/pi/bitcoindev/215e626f-972d-493e-97de-b43259c8403bn@googlegroups.com)
**Source:** Mailing List | **Started By:** {'username': 'Shannon Appelcline', 'uuid': 'auto_shannon_appelcline'} | **Messages:** 2
> Developers recently met to discuss improving Bitcoin's interoperability and network resilience, sharing video recordings and slides with the community. These efforts aim to make Bitcoin more secure, robust, and easier to integrate with other systems.

**Technical Details:** The working group meeting focused on the architectural requirements for enhancing Bitcoin's network resilience and interoperability standards. Discussions centered on mitigating routing and partitioning attacks while establishing robust node communication protocols. Developers are currently reviewing the shared materials to draft actionable specifications for future Bitcoin Core integration. The primary technical focus remains on optimizing peer-to-peer connectivity and cross-chain interaction models.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@Ameen-Alam](https://github.com/Ameen-Alam), [@RuslanProgrammer](https://github.com/RuslanProgrammer), [@cyb3ralbert](https://github.com/cyb3ralbert), [@jeanpablojp](https://github.com/jeanpablojp)

### ✍️ Top Authors
The most active PR authors this week: [@maflcko](https://github.com/maflcko), [@hebasto](https://github.com/hebasto), [@l0rinc](https://github.com/l0rinc), [@HowHsu](https://github.com/HowHsu), [@theStack](https://github.com/theStack)

### 🕵️ Top Reviewers
Providing critical review and testing: [@maflcko](https://github.com/maflcko), [@sedited](https://github.com/sedited), [@hebasto](https://github.com/hebasto), [@l0rinc](https://github.com/l0rinc), [@fanquake](https://github.com/fanquake)
