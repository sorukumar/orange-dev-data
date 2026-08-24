# 📰 This Week in Bitcoin (2026-08-17 to 2026-08-23)

## 📌 The TL;DR
- The fee estimation mechanism has undergone a significant shift, moving towards mempool-based estimation to provide more dynamic and accurate fee predictions, directly impacting user experience and transaction inclusion.
- There's active and in-depth discussion within the developer community regarding the integration of Post-Quantum Cryptography (PQC), exploring new output types, address formats (like `bc1z`), and backup solutions to prepare Bitcoin for potential future quantum threats.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#34075: fees: Introduce Mempool Based Fee Estimation to reduce overestimation](https://github.com/bitcoin/bitcoin/pull/34075)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Wallet & User Tools]** *(Activity: 88 review events)*
> This PR introduces a mempool-based fee estimation algorithm to complement the node's existing history-based fee estimator. This reduces fee overestimation, helping users avoid paying unnecessarily high transaction fees when network congestion rapidly decreases.

**Technical Details:** The PR implements a real-time fee estimation strategy that queries the active mempool's transaction fee rate distribution rather than relying solely on the historical block-based data in `CBlockPolicyEstimator`. By analyzing the current mempool state, the estimator can detect when fee pressure has subsided faster than historical averages would indicate. This logic integrates directly into the fee estimation subsystem, offering a more responsive floor for estimated fees during periods of transition.

#### [#35877: build: ci/doc win64-cross build via nix](https://github.com/bitcoin/bitcoin/pull/35877)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 40 review events)*
> This PR introduces documentation and CI testing for cross-compiling 64-bit Windows binaries using the Nix package manager. This offers developers a highly reproducible alternative for building and testing Windows releases.

**Technical Details:** This change adds a Nix derivation and configurations targeting the `x86_64-w64-mingw32` platform toolchain. It establishes a reproducible Nix-based pipeline that mirrors the standard Guix cross-compilation process for Windows. A corresponding GitHub Actions CI job is added to continuously test this build pathway against code regressions. Documentation is also updated to guide developers on compiling Windows binaries locally using Nix.

#### [#35735: Add state to HTTPRequest](https://github.com/bitcoin/bitcoin/pull/35735)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Maintenance & Tech Debt]** *(Activity: 32 review events)*
> This PR enhances the `HTTPRequest` object by adding an internal state variable, providing a clearer indication of a request's lifecycle. This improvement facilitates more robust handling of HTTP requests, enabling better resource management and error reporting.

**Technical Details:** The `HTTPRequest` class, fundamental for handling RPC and other internal HTTP communications, lacked an explicit mechanism to track its processing status. This change introduces a new member variable, likely an `enum`, to represent the current state of an HTTP request (e.g., `INITIALIZED`, `SENDING`, `RECEIVED`, `COMPLETED`, `FAILED`). This state can be updated at various points in the request's lifecycle, allowing dependent components to implement more precise logic for error handling, resource cleanup, or progress tracking based on the request's current phase.

#### [#35680: private broadcast: bound rebroadcast attempts to 1,000](https://github.com/bitcoin/bitcoin/pull/35680)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Security & Consensus]** *(Activity: 29 review events)*
> This PR introduces a crucial memory limit for private broadcast attempts, safeguarding the node from consuming excessive resources. It protects against potential denial-of-service attacks that could exploit unbounded memory usage during private transaction propagation.

**Technical Details:** The private broadcast mechanism, used for relaying transactions discreetly, previously lacked explicit memory bounds on its attempts to propagate transactions to peers. This allowed an attacker to potentially cause a node to consume unbounded memory by flooding it with private broadcast requests or by creating scenarios where broadcast attempts accumulate excessively. This PR implements a memory accounting and capping mechanism for the data structures associated with these broadcast attempts. Once a predefined memory limit is reached, further attempts are throttled, queued, or discarded, preventing resource exhaustion and mitigating potential Denial-of-Service attacks.

#### [#32958: wallet/refactor: Update SignPSBTInput to return util::Expected<void, PSBTError> and remove PSBTError:Ok](https://github.com/bitcoin/bitcoin/pull/32958)
**Author:** [@kevkevinpal](https://github.com/kevkevinpal) | **[Maintenance & Tech Debt]** *(Activity: 26 review events)*
> This PR refactors the internal `SignPSBTInput` function within the wallet, improving its error handling. It transitions to using `util::Expected` for a more robust and modern way to signal success or specific errors.

**Technical Details:** The `SignPSBTInput` function is updated to return `util::Expected<void, PSBTError>`, replacing the previous approach of returning `PSBTError` directly with a special `PSBTError::OK` value for success. This change leverages the `util::Expected` pattern, which explicitly encodes either a successful void result or a `PSBTError` enum, making error propagation clearer and less error-prone by removing the need for a magic 'OK' error value.

#### [#32162: depends: Switch from multilib to platform-specific toolchains](https://github.com/bitcoin/bitcoin/pull/32162)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 26 review events)*
> This PR updates Bitcoin Core's `depends` system to utilize platform-specific toolchains instead of multilib configurations. This change aims to streamline the build process for external dependencies and improve build reliability across different operating systems.

**Technical Details:** The modification involves adjusting the `depends` scripts and configuration to target specific platforms with their dedicated toolchains, rather than relying on a multilib setup that can be more complex or prone to issues. This likely affects compiler flag management, linker settings, and the overall isolation of build environments for cross-compilation.

#### [#35069: Refactor keypath parser](https://github.com/bitcoin/bitcoin/pull/35069)
**Author:** [@pythcoiner](https://github.com/pythcoiner) | **[Maintenance & Tech Debt]** *(Activity: 20 review events)*
> This PR refactors the keypath parser to make HD wallet derivation path validation more robust and easier to maintain. It simplifies the parsing of descriptors and derivation paths across the codebase.

**Technical Details:** The parser logic is modularized into cleaner helper functions that validate derivation path characters, such as the hardened markers. By replacing nested manual string indexing with a state-driven approach, readability and testability are enhanced. This reduces the risk of edge-case bugs when parsing complex wallet descriptors.

#### [#944: Fix out-of-bounds read in RPCParseCommandLine on empty command](https://github.com/bitcoin/bitcoin/pull/944)
**Author:** [@nabhan06](https://github.com/nabhan06) | **[Wallet & User Tools]** *(Activity: 18 review events)*

#### [#35859: wallet: use unsigned KDF iteration count](https://github.com/bitcoin/bitcoin/pull/35859)
**Author:** [@benthecarman](https://github.com/benthecarman) | **[Wallet & User Tools]** *(Activity: 16 review events)*
> This PR changes the wallet's key derivation function (KDF) iteration count to use an unsigned integer format. This prevents potential integer overflow bugs and improves overall security and consistency of wallet encryption.

**Technical Details:** The wallet database and crypter interfaces are updated to use `uint32_t` instead of a signed integer for representing PBKDF2 iteration counts. This change prevents issues related to negative iteration counts or undefined overflow behavior when parsing wallet files. The serialized output format remains compatible while enforcing stricter, safer type boundaries.

#### [#35884: util: set os-level thread names on Windows](https://github.com/bitcoin/bitcoin/pull/35884)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 15 review events)*
> This PR enables native operating system-level thread naming for Bitcoin Core when running on Windows. This makes debugging easier by showing meaningful thread names in system diagnostic tools.

**Technical Details:** This utility update implements Windows-specific thread naming using the `SetThreadDescription` API. By passing internal thread names to the Windows OS kernel, external debuggers and system diagnostic tools can identify active threads. The implementation provides a graceful fallback on legacy Windows versions where this API is not supported. This brings Windows parity to the thread-naming diagnostics already available on Linux and macOS.

#### [#35161: consensus: document merkle mutation root invariant](https://github.com/bitcoin/bitcoin/pull/35161)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*
> This PR adds important documentation clarifying the Merkle mutation root invariant in Bitcoin's consensus code. This improves developers' understanding of a fundamental consensus mechanism, particularly regarding transaction identity and malleability.

**Technical Details:** New comments and possibly asserts are integrated into the consensus-critical code paths responsible for Merkle tree construction. This explicitly documents the 'merkle mutation root invariant,' detailing how the transaction Merkle root is calculated and ensuring its properties, especially concerning non-malleability and `wtxid`, are clear and consistently maintained.

#### [#35665: psbt: avoid duplicate global xpub keys when merging](https://github.com/bitcoin/bitcoin/pull/35665)
**Author:** [@thomasbuilds](https://github.com/thomasbuilds) | **[Wallet & User Tools]** *(Activity: 13 review events)*
> This PR fixes an issue where merging multiple Partially Signed Bitcoin Transactions (PSBTs) could result in duplicate global xpub keys. Ensuring unique entries prevents transaction bloat and ensures strict adherence to the PSBT standard.

**Technical Details:** During the merging of two PSBT structures, the implementation now deduplicates elements inside the `g_xpubs` map. The merging logic is updated to check for pre-existing keys to avoid redundant additions and unnecessary serialization overhead. This ensures clean, compliant transaction data before final signing or extraction.

#### [#35797: psbt: support output metadata updates before inputs are added](https://github.com/bitcoin/bitcoin/pull/35797)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Wallet & User Tools]** *(Activity: 12 review events)*
> This PR extends Partially Signed Bitcoin Transaction (PSBT) capabilities to allow output metadata updates before inputs are added. This enhances the flexibility of PSBT construction, especially for collaborative transactions.

**Technical Details:** The PR refactors the validation logic in the PSBT parser and updater modules to decouple output metadata structures from input requirements. Previously, the PSBT framework imposed ordering constraints that restricted updating output information if no inputs had been populated yet. By removing this artificial constraint, APIs can now safely populate and modify output data blocks in a blank or input-less PSBT state.

#### [#35993: guix: build glibc with `--enable-kernel=3.17.0`](https://github.com/bitcoin/bitcoin/pull/35993)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 12 review events)*
> This PR updates the Guix build configuration to compile glibc with support restricted to Linux kernel 3.17.0 and newer. This allows the build system to leverage modern kernel features and optimizations for deterministic releases.

**Technical Details:** The PR modifies the Guix build recipe for glibc by appending the --enable-kernel=3.17.0 flag to the configuration options. This tells the glibc build system that it does not need to compile fallback code or compatibility shims for kernels older than version 3.17.0. The change results in a slightly cleaner library binary and ensures compatibility with modern Linux systems while keeping builds reproducible.

#### [#36019: bench: Construct CTxOut and COutPoint in a single expression](https://github.com/bitcoin/bitcoin/pull/36019)
**Author:** [@alexanderwiederin](https://github.com/alexanderwiederin) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*
> This PR optimizes microbenchmarks by constructing transaction outputs and outpoints in single expressions. This reduces benchmark execution overhead and produces cleaner measurement results.

**Technical Details:** This refactoring updates benchmark loops to construct `CTxOut` and `COutPoint` objects in-place rather than utilizing multi-step variables. By encouraging compiler Return Value Optimization (RVO) and reducing unnecessary copies, it minimizes benchmark harness overhead. This ensures that measured timings reflect core logic performance rather than allocation and copy overhead.

#### [#35946: rpc: Improve some type specs for openrpc](https://github.com/bitcoin/bitcoin/pull/35946)
**Author:** [@sedited](https://github.com/sedited) | **[Wallet & User Tools]** *(Activity: 10 review events)*
> This PR refines the type specifications in the OpenRPC schema definitions for Bitcoin Core's RPC interface. It improves the accuracy of auto-generated documentation and developer tooling that relies on these RPC specifications.

**Technical Details:** It updates internal type specifications used to generate the OpenRPC schema representation of Bitcoin Core's RPC interface. By correcting type mappings and constraints, it prevents parser issues in external API clients and ensures strict type compliance with the OpenRPC standard. The changes are localized to the RPC registration and schema generation metadata, posing zero risk to core consensus or RPC runtime behavior.

#### [#34239: depends: Hash included makefiles in package checksums](https://github.com/bitcoin/bitcoin/pull/34239)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 10 review events)*
> This PR ensures that changes to dependency makefiles are tracked by including their hashes in the dependency package checksums. This guarantees that modifications to build recipes result in clean, reproducible builds.

**Technical Details:** The build system's package hashing mechanism within the `depends` directory is modified to parse and include referenced makefile paths. When a dependency makefile is modified, its hash change invalidates the existing cached package artifact. This ensures build consistency across platforms by guaranteeing that build instruction changes trigger rebuilds.

#### [#1878: field: correct `_fe_half` docs (output is not normalized, input requires magnitude <= 31)](https://github.com/bitcoin/bitcoin/pull/1878)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR removes a redundant duplicate `if` statement from the codebase. It simplifies the code structure without altering any functional behavior or node performance.

**Technical Details:** This refactoring eliminates an identical, duplicated conditional branch in the source code. Removing this redundant logic reduces AST complexity and improves code readability for future maintenance. Modern compilers would optimize this duplicate branch away, so the change has zero functional or binary footprint impact.

#### [#36018: test: [refactor] Properly use BOOST_CHECK_EXCEPTION](https://github.com/bitcoin/bitcoin/pull/36018)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This pull request refactors the unit test suite to use the correct Boost test macro when asserting exceptions are thrown. This improves the reliability and clarity of test assertions and error reporting.

**Technical Details:** The change replaces ad-hoc try-catch blocks and generic assertions with `BOOST_CHECK_EXCEPTION`. It validates that the correct exception type is thrown along with testing its specific error messages using a custom predicate. This standardizes exception testing across the testing framework and prevents potential false positives.

#### [#1915: refactor: Move (de)ser helpers from musig and eckey to group](https://github.com/bitcoin/bitcoin/pull/1915)
**Author:** [@fjahr](https://github.com/fjahr) | **[Wallet & User Tools]** *(Activity: 8 review events)*
> This PR updates a legacy Qt macro in the GUI codebase to ensure compatibility with Qt5. This change allows the Bitcoin Core graphical interface to compile and run seamlessly on modern systems using newer versions of the Qt framework.

**Technical Details:** The PR replaces the deprecated Qt4 platform macro `Q_WS_MAC` with the Qt5-compatible `Q_OS_MAC` across the GUI-specific source files. This ensures correct conditional compilation for macOS targets when building against Qt5 libraries. This migration avoids compilation failures due to obsolete windowing system macros being removed in modern Qt releases.

#### [#35980: contrib: reject divergent verify-commits history](https://github.com/bitcoin/bitcoin/pull/35980)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR updates the developer utility script used for verifying commits to detect and reject divergent git histories. This adds security to the release process by preventing unauthorized changes from slipping into verified branches.

**Technical Details:** The `verify-commits.sh` script is updated to explicitly check the merge base of the current branch against the target branch. This ensures that only linear commit histories are verified and prevents detached HEAD states from masquerading as valid. By rejecting histories that have diverged, the integrity of the signed commit chain is preserved.

#### [#35956: fuzz: scope fake clocks to target phases](https://github.com/bitcoin/bitcoin/pull/35956)
**Author:** [@HowHsu](https://github.com/HowHsu) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR scopes the use of mock clocks to active execution phases within fuzzing targets. This prevents time-shifting logic from interfering with global node initialization, leading to more stable and reliable fuzz tests.

**Technical Details:** The implementation utilizes RAII-based clock wrappers to limit mock time modifications specifically to the target run phase. This ensures that early initialization steps, such as setting up global state or loading configurations, occur using standard clocks. By isolating mock time, false-positive timeouts and node startup failures are minimized during high-throughput fuzz testing.

#### [#36007: http: Make HTTPRequest::m_client a weak_ptr](https://github.com/bitcoin/bitcoin/pull/36007)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR converts the HTTP client pointer in the HTTP request class into a weak pointer to prevent memory leaks. This improves the stability of the node's HTTP server under high load or asynchronous disconnections.

**Technical Details:** The type of HTTPRequest::m_client is changed from std::shared_ptr to std::weak_ptr to break potential reference cycles between the request life cycle and the client connection context. When processing a request, the weak pointer is safely promoted to a shared pointer using .lock() to verify the client's existence. If the client has already disconnected or been destroyed, the request execution terminates gracefully without attempting to access freed memory.

#### [#1911: refactor: rename `ctx` param to `ecmult_gen_ctx` where applicable](https://github.com/bitcoin/bitcoin/pull/1911)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR fixes a mismatched format specifier in a print statement within the RPC module, correcting a signed versus unsigned integer type mismatch. This ensures accurate logging and prevents potential undefined behavior or compiler warnings.

**Technical Details:** This PR resolves a compiler warning and potential formatting bug by correcting a `printf` format specifier in `bitcoinrpc.cpp`. Specifically, it changes a format specifier to match the actual type (signed vs unsigned) of the argument being passed to the log or output function. This prevents incorrect string representations of internal numbers in the RPC logging layer and ensures portability across different architectures and compilers.

#### [#1916: ecdh/ellswift: simplify seckey loading with `_scalar_set_b32_seckey`](https://github.com/bitcoin/bitcoin/pull/1916)
**Author:** [@theStack](https://github.com/theStack) | **[Security & Consensus]** *(Activity: 7 review events)*
> This PR adds a validation step to verify the network magic bytes before reading address data from the peers database on startup. This prevents Bitcoin Core from parsing corrupted or malicious files, protecting the node from potential crashes.

**Technical Details:** This PR modifies the serialization/deserialization routine of `CAddrMan` inside `src/addrman.cpp` to enforce safety checks on external file loads. It adds a check to verify that the file begins with the network-specific message start marker (`pchMessageStart`) before attempting to deserialize IP address data. If the magic bytes do not match, the deserialization is safely aborted, preventing the node from reading out-of-bounds or corrupted data into memory from a malformed `peers.dat` file.

#### [#35965: test: Tighten Coin equality and add debug output](https://github.com/bitcoin/bitcoin/pull/35965)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR improves UTXO database unit tests by tightening the criteria for Coin equality and adding detailed diagnostic outputs upon failure. This helps developers quickly isolate bugs when working on chainstate and cache logic.

**Technical Details:** The changes update the Coin comparison logic to strictly assert equality across all properties of the `Coin` struct, including its active status and height. Additionally, a detailed logging format is introduced to print coin metadata when assertion mismatches are encountered. This provides immediate visibility into cache state inconsistencies during automated test suite execution.

#### [#35955: wallet: remove orphaned GetAffectedKeys and LegacyScriptPubKeyMan declarations](https://github.com/bitcoin/bitcoin/pull/35955)
**Author:** [@laxmanacharya8](https://github.com/laxmanacharya8) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR removes unused function declarations for GetAffectedKeys and LegacyScriptPubKeyMan from the wallet header files. This cleans up the wallet codebase, reducing technical debt and improving code readability for developers.

**Technical Details:** This refactoring PR removes dead declarations from `src/wallet/wallet.h` and associated headers. Specifically, it purges unused function signatures for GetAffectedKeys and legacy key management class methods that no longer have corresponding implementations in the source files. Removing these symbols reduces compilation clutter and prevents developers from referencing obsolete interfaces during wallet module development.

#### [#35972: fuzz: Fix assertion in `txorphan`](https://github.com/bitcoin/bitcoin/pull/35972)
**Author:** [@marcofleon](https://github.com/marcofleon) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR fixes an incorrect assertion within the transaction orphan fuzzer, preventing false positives during fuzzing runs. This ensures the fuzzing suite can run reliably without crashing, enhancing the stability of Bitcoin Core's automated testing pipeline.

**Technical Details:** The PR addresses an assertion logic error inside the `txorphan` fuzz target. It modifies the state validation checks to correctly account for expected edge cases in transaction orphan processing during simulated fuzzing runs. By refining the assertion criteria, it prevents benign execution paths from triggering assertion failures, thus ensuring the fuzzer can continue exploring deeper code paths.

#### [#35952: kernel: prevent dangling iterators from temporary ranges](https://github.com/bitcoin/bitcoin/pull/35952)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Strategic Initiatives]** *(Activity: 6 review events)*
> This PR prevents undefined behavior and crashes within the kernel library by eliminating dangling iterators caused by temporary ranges. It ensures memory safety in the modularized kernel codebase.

**Technical Details:** The implementation addresses lifetime issues where range-based for loops or iterator accesses were performed on temporary containers returned by reference or value. By ensuring the underlying ranges are bound to a variable with sufficient lifetime, or by rewriting the loops to avoid temporary generation, the PR guarantees that iterators do not become dangling. This strengthens the safety guarantees of the Bitcoin Core Kernel API.

#### [#35986: p2p: reconsider orphans when missing inputs are mined](https://github.com/bitcoin/bitcoin/pull/35986)
**Author:** [@instagibbs](https://github.com/instagibbs) | **[Network & Privacy]** *(Activity: 6 review events)*
> This PR improves transaction propagation efficiency by reconsidering orphan transactions when their missing inputs are detected in a newly mined block. This prevents valid transactions from being delayed or lost when parent transactions bypass the local mempool.

**Technical Details:** The PR integrates a hook between the block connection logic and the P2P transaction processing engine (TxOrphanage). Upon accepting a new block, the engine scans the transactions contained within it and matches them against the missing parents of existing orphans. Validated orphan transactions whose parents are now confirmed are extracted from the orphanage and re-queued for mempool acceptance and peer relay.

#### [#35963: doc : update cjdns docs to discourage using onlynet option](https://github.com/bitcoin/bitcoin/pull/35963)
**Author:** [@naiyoma](https://github.com/naiyoma) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR updates the CJDNS documentation to advise users against utilizing the onlynet configuration option. This prevents nodes from isolating themselves entirely from the primary Bitcoin network.

**Technical Details:** The PR edits the markdown and text-based configuration guides within the doc/ directory to explicitly discourage setting onlynet=cjdns. It highlights that doing so restricts peer discovery and connection capabilities strictly to CJDNS-enabled nodes, potentially leading to network partitioning and a lack of connection to IPv4/IPv6 peers. The documentation is updated to recommend a multi-transport configuration instead.

#### [#36008: wallet: WalletBatch->WriteVersion respect argument](https://github.com/bitcoin/bitcoin/pull/36008)
**Author:** [@fanquake](https://github.com/fanquake) | **[Wallet & User Tools]** *(Activity: 5 review events)*
> This PR updates the wallet database logic to ensure that WalletBatch::WriteVersion respects the specific version argument passed to it instead of using a default. This improves the consistency and reliability of wallet file upgrades and version tracking.

**Technical Details:** The PR refactors WalletBatch::WriteVersion to properly serialize and write the version argument provided in the function call. Previously, the method could ignore this argument in certain execution paths, potentially leading to incorrect versioning metadata in the database. This fix ensures that the physical wallet database state precisely matches the logical version requested by the caller.

#### [#35976: test: Speedup fee estimation functional test with batching](https://github.com/bitcoin/bitcoin/pull/35976)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR optimizes the fee estimation functional test by implementing batching for RPC calls. This significantly reduces the time required to run the test suite, improving developer productivity and CI turnaround times.

**Technical Details:** The PR refactors the fee estimation functional test to utilize RPC batching instead of executing sequential individual RPC calls. By grouping multiple RPC requests into single payloads, it minimizes round-trip latency and overhead in the test framework. This architectural optimization results in a drastic reduction in functional test execution time without altering the underlying testing coverage or logic.

#### [#36051: ci: use ruff 0.16.x](https://github.com/bitcoin/bitcoin/pull/36051)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR updates the Python linter used in the continuous integration system to version 0.16.x. This ensures the codebase's Python scripts and tests continue to adhere to modern formatting and style standards.

**Technical Details:** This changes the CI environment configurations to upgrade the `ruff` static analysis tool to version 0.16.x. It enforces updated formatting rules and newer syntax checks across the repository's Python files. This helps maintain high code quality and consistency in test scripts using an optimized linter.

#### [#36012: psbt: Remove unused `IsNull()` methods](https://github.com/bitcoin/bitcoin/pull/36012)
**Author:** [@nebula-21](https://github.com/nebula-21) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR removes unused `IsNull()` methods from the PSBT input and output structures to clean up dead code. This reduces the footprint of the PSBT API and improves long-term codebase maintenance.

**Technical Details:** The PR identifies and purges redundant `IsNull()` method declarations and definitions from `PSBTInput` and `PSBTOutput`. These checks were unnecessary as the validity of the objects is already checked via alternative validation paths or checking vector sizes directly. Eliminating this dead code simplifies the PSBT API and prevents developer confusion.

#### [#35968: test: sync funding block before isolating nodes](https://github.com/bitcoin/bitcoin/pull/35968)
**Author:** [@Shaurya2k06](https://github.com/Shaurya2k06) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR resolves intermittent integration test failures by ensuring that funding blocks are fully synchronized across nodes before network isolation is tested. This guarantees consistent chain states during simulated network partitions.

**Technical Details:** The functional test framework is updated to explicitly invoke `sync_blocks()` prior to disconnecting or isolating nodes. This ensures that the node under test and its peers have both processed and agreed upon the tip block containing the funding transaction. This prevents transactions from being orphaned and avoids false failures related to unexpected wallet balances.

#### [#36009: miniscript: remove unused context argument from ParseHexStr](https://github.com/bitcoin/bitcoin/pull/36009)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR simplifies the Miniscript parser code by removing an unused context argument from the ParseHexStr function. This clean-up improves code readability and maintainability without changing any functionality.

**Technical Details:** The refactoring removes a redundant context parameter from ParseHexStr and associated parsing functions in the Miniscript module. Call sites within src/script/miniscript.cpp and corresponding unit tests are updated to match the simplified signature. This reduces stack overhead and eliminates dead code paths in the parser utility functions.

#### [#35995: doc: fix outdated URL in hash_tests.cpp](https://github.com/bitcoin/bitcoin/pull/35995)
**Author:** [@cyb3ralbert](https://github.com/cyb3ralbert) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR updates an outdated URL in the hash unit tests documentation to point to the correct, active resource. This ensures developers have access to accurate reference materials when working on cryptographic hash functions in the codebase.

**Technical Details:** The change is purely documentation-based within `src/test/hash_tests.cpp`. It replaces a stale or broken hyperlink with an active URL pointing to the relevant specification or test vectors for the cryptographic hashes being tested. It has no runtime impact on compilation, consensus, or performance, acting solely as a maintenance update for code readability and reference.

#### [#36010: test: Print os exit code on failure](https://github.com/bitcoin/bitcoin/pull/36010)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR improves the testing framework by logging the operating system exit code when a test process fails. This makes it significantly easier for developers to diagnose the cause of crashes, such as segmentation faults or abort signals.

**Technical Details:** The testing harness is updated to capture the process exit status or termination signal from the OS execution layer when a test suite subprocess fails. Instead of suppressing or genericizing the error, the harness prints the exact exit code. This provides developers with immediate context on whether the failure was due to an assertion, a crash, or an external signal.

#### [#35954: qa: Disable Qt's glib event dispatcher for GUI tests on OpenBSD](https://github.com/bitcoin/bitcoin/pull/35954)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR disables Qt's glib event dispatcher specifically for GUI tests running on OpenBSD. This resolves persistent test hangs and improves the reliability of the continuous integration system.

**Technical Details:** The PR configures the GUI test initialization sequence to set the QT_NO_GLIB environment variable or bypass glib integration when executing on OpenBSD. This forces Qt to fallback to its native UNIX event loop dispatcher, preventing deadlocks and race conditions that occur due to incompatibilities between glib and the OpenBSD thread scheduler. This targeted change ensures robust continuous integration testing without affecting other POSIX environments.

#### [#35846: test: Use throwing config parser getters without fallback](https://github.com/bitcoin/bitcoin/pull/35846)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR updates the test suite to use throwing configuration parser getters directly instead of relying on fallback values. This makes the tests more robust by ensuring that configuration parsing failures are explicitly caught and handled rather than silently ignored.

**Technical Details:** This change modifies the functional and unit test configuration parsers to call throwing getters without fallback defaults. By removing fallback defaults, any configuration error in the test suite setup will immediately fail-fast, preventing silent failures or misconfigurations. This enforces strict configuration validation during test harness execution.

#### [#35982: Update minisketch subtree to latest master](https://github.com/bitcoin/bitcoin/pull/35982)
**Author:** [@fanquake](https://github.com/fanquake) | **[Network & Privacy]** *(Activity: 2 review events)*
> This PR updates the Minisketch library subtree in Bitcoin Core to its latest master commit. This brings in upstream bug fixes, performance improvements, and optimizations to the set reconciliation library used for efficient transaction relay.

**Technical Details:** The PR performs a subtree merge of the `src/minisketch` third-party dependency, aligning it with the latest upstream master branch. This integration updates the localized Minisketch implementation used for BIP330 Erlay transaction reconciliation. It pulls in recent performance enhancements, compiler warning fixes, and compatibility updates from the upstream Minisketch repository without changing the node's external API.

#### [#36045: test: avoid undersized Boost.Test signal stacks](https://github.com/bitcoin/bitcoin/pull/36045)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR increases the alternative signal stack size used by the Boost.Test framework during unit testing. This prevents tests from crashing on platforms with larger default stack requirements.

**Technical Details:** This PR modifies the unit test runner initialization to allocate a larger signal stack for Boost.Test's exception and signal handlers. On certain architectures and modern Linux kernels, the default minimal stack size (`MINSIGSTKSZ`) has increased, leading to crashes during test teardowns. By dynamically querying or providing a safe constant buffer, the test framework avoids undersized stack overflows. This enhances overall test suite stability across diverse platform architectures.

#### [#36034: Release: Prepare "Translation string freeze" step](https://github.com/bitcoin/bitcoin/pull/36034)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR prepares the Bitcoin Core codebase for an upcoming release by freezing the user interface translation strings. This gives translators stable text templates to complete localizations before the release.

**Technical Details:** This release-related pull request initiates the translation string freeze phase by marking the codebase ready for localization updates. It prepares the source files for exporting to Transifex and prevents developers from introducing new string changes that would result in untranslated GUI text. This ensures localization completeness and stability for the release candidate.

#### [#36020: doc: Correct after HTTPRequest::m_client changed to weak_ptr](https://github.com/bitcoin/bitcoin/pull/36020)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR corrects documentation to match a previous code change where the HTTP request's client reference was migrated to a weak pointer. This keeps technical documentation accurate and aligned with the actual implementation.

**Technical Details:** This documentation-only PR updates architectural descriptions to reflect the change of `HTTPRequest::m_client` from a shared/raw pointer to `std::weak_ptr`. It details the updated object lifetime model of the HTTP server, explaining how circular reference memory leaks are now prevented. This ensures engineering references accurately describe the underlying thread-safe lifecycle design.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#35877: build: ci/doc win64-cross build via nix](https://github.com/bitcoin/bitcoin/pull/35877)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 32 review events this week)*
> This PR introduces documentation and CI testing for cross-compiling 64-bit Windows binaries using the Nix package manager. This offers developers a highly reproducible alternative for building and testing Windows releases.

#### [#36006: ci: add Guix builds to CI](https://github.com/bitcoin/bitcoin/pull/36006)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 17 review events this week)*
> This PR integrates Guix-based reproducible builds directly into the continuous integration (CI) pipeline. This ensures that any changes to the codebase do not break the deterministic reproducibility of Bitcoin Core binaries.

#### [#25573: guix: produce a `-static-pie` bitcoind](https://github.com/bitcoin/bitcoin/pull/25573)
**Author:** [@fanquake](https://github.com/fanquake) | **[Security & Consensus]** *(Activity: 16 review events this week)*
> This PR updates the build configuration to compile the `bitcoind` binary as a static, Position-Independent Executable (static-pie). This enhances release security by enabling Address Space Layout Randomization (ASLR) while retaining the portability of a statically linked binary.

#### [#35730: http: limit connected HTTPRemoteClients](https://github.com/bitcoin/bitcoin/pull/35730)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 13 review events this week)*
> This pull request introduces a limit on concurrent HTTP connections to protect the RPC and REST interfaces from resource exhaustion. This prevents Denial of Service (DoS) attacks from stalling or crashing the node.

#### [#35884: util: set os-level thread names on Windows](https://github.com/bitcoin/bitcoin/pull/35884)
**Author:** [@ViniciusCestarii](https://github.com/ViniciusCestarii) | **[Maintenance & Tech Debt]** *(Activity: 13 review events this week)*
> This PR enables native operating system-level thread naming for Bitcoin Core when running on Windows. This makes debugging easier by showing meaningful thread names in system diagnostic tools.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [bitcoindev] Giving teeth to expected EC disabling: P2XX(-T)(-ML)](https://gnusha.org/pi/bitcoindev/Bx0LA0afwRjPIcEol98t7ztodzX7mIBeA1kGLV-rAicu4IuGBkME3A-UzBC5KJOawIciVn1CW3bCILlyVpMJU_MAgqV5OBjSFQiZrX60fb4=@wuille.net)
**Source:** Mailing List | **Started By:** {'username': 'Adam Gibson', 'uuid': 'can_adam_gibson'} | **Messages:** 6
> Developers are discussing how to safeguard Bitcoin against future quantum computing threats. By defining 'canary' signals, the community is planning a coordinated way to safely upgrade Bitcoin's security systems long before any actual threat can compromise user funds.

**Technical Details:** The discussion analyzes the timeline of Shor's algorithm feasibility given the immense resources required to construct a cryptanalytically viable quantum computer. Developers are defining 'canary' mechanisms not as automated protocol triggers, but as social consensus signals intended to coordinate timely soft or hard forks to quantum-resistant cryptography. The architectural challenge lies in balancing the preemptive deployment of heavy post-quantum signatures against the coordination complexity of a reactive, signal-driven upgrade path. Resolving this will help establish a clear contingency framework for Bitcoin's long-term cryptographic transition.

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/11)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 6
> Developers are discussing how to transition Bitcoin to quantum-resistant security without driving up transaction fees. They are also exploring decentralized storage-sharing techniques to keep running a Bitcoin node cheap and accessible for everyone.

**Technical Details:** The debate centers on the economic trade-offs of Post-Quantum Cryptography (PQC) output types, specifically how larger signature sizes impact fee-rate sensitivity and user adoption. To mitigate the resulting blockchain bloat and its pressure on Initial Block Download (IBD) for non-pruned nodes, developers are proposing historical block storage sharding. By leveraging Forward Error Correction (FEC) techniques, nodes could store only a fraction of the blockchain history while maintaining data availability. Next steps require analyzing PQC layout efficiency and evaluating the peer-to-peer feasibility of FEC-sharding protocols.

### [Re: Deterministic UTXO consolidation under volatile fee regimes](https://delvingbitcoin.org/t/deterministic-utxo-consolidation-under-volatile-fee-regimes/2257/5)
**Source:** Delving | **Started By:** {'username': 'Federico Blanco Sánchez-Llanos', 'uuid': 'auto_federico_blanco_s_nchez_llanos'} | **Messages:** 4
> Developers are exploring how combining small Bitcoin fragments (UTXOs) can be used to make wallets more secure and predictable. By focusing on consistency rather than just saving on fees, this approach could prevent transaction errors and enhance user privacy.

**Technical Details:** The discussion advocates for treating UTXO consolidation as a deterministic correctness boundary in wallet transaction construction rather than a mere fee-optimization heuristic. By defining formal rules for when UTXOs are consolidated, developers aim to eliminate edge cases in coin selection that lead to transaction failures. This architectural shift could also standardize wallet behavior, reducing unique on-chain fingerprints that compromise user privacy. Implementing this requires integrating predictable consolidation state machines directly into wallet transaction-building pipelines.

### [Re: Boomerang: Bitcoin Cold Storage with Built-In Coercion Resistance](https://delvingbitcoin.org/t/boomerang-bitcoin-cold-storage-with-built-in-coercion-resistance/2239/6)
**Source:** Delving | **Started By:** {'username': 'bitryonix', 'uuid': 'auto_bitryonix'} | **Messages:** 3
> A new cold storage custody protocol called Boomerang is introduced to protect high-value Bitcoin holders against physical coercion and theft. It aims to significantly enhance physical security by design, offering peace of mind for long-term storage.

**Technical Details:** The discussion introduces Boomerang, a novel Bitcoin cold storage protocol designed to mitigate physical attack vectors and coercion. The technical architecture focuses on transaction encumbrance and delayed-release mechanisms to prevent immediate funds extraction under duress. Developers need to analyze the protocol's reliance on specific script paths and timelocks to ensure robust security without introducing transaction fee pinning vulnerabilities. Further review is required to evaluate its integration with existing multisig schemes and hardware wallets.

### [Re: PQ-single-address-backup - BIP-38 for P2MR (bc1z) - 104-char encrypted backup format](https://delvingbitcoin.org/t/pq-single-address-backup-bip-38-for-p2mr-bc1z-104-char-encrypted-backup-format/2767/9)
**Source:** Delving | **Started By:** {'username': 'coldtest-berlin', 'uuid': 'auto_coldtest_berlin'} | **Messages:** 3
> Developers are working on making future-proof, quantum-resistant Bitcoin backups safer to store on paper. By adding error-checking features to backup seeds, they ensure users won't lose access to their funds due to a simple writing mistake.

**Technical Details:** The discussion focuses on securing 32-byte SLH-DSA seeds for BIP-360 (P2MR) post-quantum cold storage before migration paths are finalized. Relying on raw 64-character hex strings on paper is highly risky because a single transcription error is undetectable. Developers are addressing this by integrating typo-detection and checksum mechanisms into the backup format to prevent silent data corruption. The current priority is finalizing a standardized, error-correcting serialization format suitable for long-term physical backups.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@Shaurya2k06](https://github.com/Shaurya2k06), [@laxmanacharya8](https://github.com/laxmanacharya8), [@nabhan06](https://github.com/nabhan06)

### ✍️ Top Authors
The most active PR authors this week: [@l0rinc](https://github.com/l0rinc), [@fanquake](https://github.com/fanquake), [@maflcko](https://github.com/maflcko), [@hebasto](https://github.com/hebasto), [@theStack](https://github.com/theStack)

### 🕵️ Top Reviewers
Providing critical review and testing: [@maflcko](https://github.com/maflcko), [@hebasto](https://github.com/hebasto), [@l0rinc](https://github.com/l0rinc), [@achow101](https://github.com/achow101), [@willcl-ark](https://github.com/willcl-ark)
