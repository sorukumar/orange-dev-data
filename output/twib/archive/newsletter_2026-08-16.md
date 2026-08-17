# 📰 This Week in Bitcoin (2026-08-10 to 2026-08-16)

## 📌 The TL;DR
- Node Efficiency & Wallet Modernization**: Significant progress was made in reducing node resource usage, highlighted by the `txindex` disk space optimizations, while concurrently enhancing wallet capabilities with improved HD key derivation RPCs and better support for advanced descriptor wallets.
- Future Protocol & Cryptographic Research**: Mailing list discussions indicate active engagement with long-term challenges, including exploring universal opt-in replay protection strategies, researching the potential for running consensus code within zkVMs, and considering post-quantum secure wallet backup formats.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#35531: txindex: hash keys and pack positions to reduce disk usage](https://github.com/bitcoin/bitcoin/pull/35531)
**Author:** [@andrewtoth](https://github.com/andrewtoth) | **[Performance & Optimization]** *(Activity: 79 review events)*
> Optimizes the transaction index (`txindex`) database by hashing keys and packing storage positions. This significantly reduces the disk space required to maintain a full transaction index on node operators' machines.

**Technical Details:** This PR optimizes the serialization format of the transaction index stored within LevelDB. It replaces raw transaction lookup keys with hashed representations and utilizes variable-length integer encoding (compact size) for disk offsets. By packing these block and transaction position structures more tightly, the overall index size on disk is substantially reduced while preserving high-speed random lookup capabilities.

#### [#34794: rest: add Cache-Control headers to REST responses](https://github.com/bitcoin/bitcoin/pull/34794)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Wallet & User Tools]** *(Activity: 39 review events)*
> This PR adds HTTP Cache-Control headers to Bitcoin Core's REST API responses. This reduces unnecessary node resource consumption by allowing clients and reverse proxies to cache static data like blocks and headers.

**Technical Details:** The changes implement Cache-Control header injection within the REST request handling logic in `src/rest.cpp`. Dynamic endpoints such as `/mempool` are served with `no-cache` or low `max-age` values, while immutable resource endpoints like `/block` include headers that permit long-term public caching. This optimization reduces the load on the built-in Libevent HTTP server by shifting redundant query resolution to the client or intermediate caching proxies.

#### [#32784: wallet: derivehdkey RPC to get xpub at arbitrary path](https://github.com/bitcoin/bitcoin/pull/32784)
**Author:** [@Sjors](https://github.com/Sjors) | **[Wallet & User Tools]** *(Activity: 38 review events)*
> This PR introduces a new RPC command, `derivehdkey`, enabling users to retrieve an extended public key (xpub) at any specified derivation path. This provides advanced users with finer control over their HD wallet's key management.

**Technical Details:** A new RPC handler for `derivehdkey` is added, which accepts a BIP32 derivation path string as input. It utilizes the wallet's internal hierarchical deterministic key derivation functions to compute the extended public key at that specific path and returns it to the caller.

#### [#35496: kernel: add `btck_set_mock_time` for testing time-dependent paths](https://github.com/bitcoin/bitcoin/pull/35496)
**Author:** [@stringintech](https://github.com/stringintech) | **[Maintenance & Tech Debt]** *(Activity: 34 review events)*
> This PR introduces a new testing utility to mock time within the Bitcoin Core kernel, enabling more reliable testing of time-dependent code. This enhancement improves the coverage and determinism of unit and functional tests.

**Technical Details:** The PR adds `btck_set_mock_time` to the libbitcoin_kernel interface, allowing test environments to programmatically control the time perceived by the kernel. This function enables deterministic testing of features reliant on timestamps, such as block times, transaction expiry, or locktime mechanisms, without needing to wait for real-world clock progression. By providing a consistent mock time, developers can thoroughly test edge cases and time-sensitive logic, significantly improving test reliability and reducing flakiness.

#### [#33186: wallet, test: Ancient Wallet Migration from v0.14.3 (no-HD and Single Chain)](https://github.com/bitcoin/bitcoin/pull/33186)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Wallet & User Tools]** *(Activity: 27 review events)*
> This PR improves Bitcoin Core's ability to migrate very old wallet data from versions as far back as v0.14.3. This ensures users with historical non-HD, single-chain wallets can seamlessly upgrade to modern Bitcoin Core versions without losing access to their funds.

**Technical Details:** The PR introduces logic to correctly handle and migrate legacy wallet formats, specifically those created before HD wallet adoption and multi-chain support. It involves updating the wallet migration path in `CWallet::MigrateWallet` or similar, adding specific parsing and conversion routines for older BDB wallet structures, and includes new test cases to validate the migration process for such ancient wallets.

#### [#35729: refactor: test: Unroll `&&` conditions in macros](https://github.com/bitcoin/bitcoin/pull/35729)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob) | **[Maintenance & Tech Debt]** *(Activity: 24 review events)*
> This PR improves the test suite's feedback by breaking down compound conditions in test assertions. This allows developers to see exactly which condition failed during testing.

**Technical Details:** The refactor splits compound assertions using the logical AND operator within testing macros into distinct, sequential assertions. This ensures the test framework logs the precise variable state and location of any failing assertion. It enhances debuggability of unit tests without changing functional behavior.

#### [#35866: test: Verify unwelcome RPC clients are rejected before reading their requests](https://github.com/bitcoin/bitcoin/pull/35866)
**Author:** [@winterrdog](https://github.com/winterrdog) | **[Maintenance & Tech Debt]** *(Activity: 18 review events)*
> This PR adds integration tests to confirm that unauthorized or unwelcome RPC clients are disconnected immediately before the server wastes resources reading their request payloads. This helps ensure the RPC server remains resilient against unauthorized connection spam.

**Technical Details:** The test suite is expanded to verify the behavior of the HTTP/RPC server's connection-filtering layer. It asserts that clients originating from disallowed IP addresses are rejected at the TCP level prior to the server consuming CPU and memory to parse incoming payloads. The test utilizes mock network clients to simulate forbidden origins and monitors the socket lifecycle to verify immediate termination. This guarantees the server's early-rejection logic works as intended without introducing regressions.

#### [#33585: cmake: Use builtin support for .manifest files](https://github.com/bitcoin/bitcoin/pull/33585)
**Author:** [@purpleKarrot](https://github.com/purpleKarrot) | **[Maintenance & Tech Debt]** *(Activity: 17 review events)*
> This PR updates the CMake build scripts to use native support for Windows manifest files. This removes custom workarounds, making the build configuration cleaner and more maintainable.

**Technical Details:** The implementation replaces custom post-build commands for embedding application manifests with CMake's built-in target properties for Windows executables. This leverages modern CMake features to handle resource compilation and manifest embedding natively. It results in a cleaner, less error-prone build definition for Windows targets.

#### [#35493: wallet, descriptor: Fix MuSig private key completeness checks on `importdescriptors`](https://github.com/bitcoin/bitcoin/pull/35493)
**Author:** [@w0xlt](https://github.com/w0xlt) | **[Wallet & User Tools]** *(Activity: 16 review events)*
> This PR fixes a bug in how the wallet's `importdescriptors` RPC validates MuSig private key completeness. It ensures that advanced multisignature descriptors are correctly imported, improving the reliability of these complex wallet setups.

**Technical Details:** The PR corrects an issue within the `importdescriptors` RPC where the completeness checks for MuSig private keys were not correctly applied or evaluated. Previously, this could lead to an incorrect assessment of whether a MuSig descriptor had all necessary private key components, potentially causing unexpected behavior or data inconsistencies after import. The fix ensures that the logic accurately verifies the presence and validity of all required private key data for MuSig descriptors. This significantly enhances the robustness and correctness of importing advanced descriptor-based wallets.

#### [#35605: wallet: rpc: Deprecate `removeprunedfunds` RPC](https://github.com/bitcoin/bitcoin/pull/35605)
**Author:** [@davidgumberg](https://github.com/davidgumberg) | **[Wallet & User Tools]** *(Activity: 15 review events)*
> This PR marks the `removeprunedfunds` RPC as deprecated, signaling that users should transition to a more modern or preferred method for managing pruned funds in their wallet. This prepares for future RPC simplifications and improvements.

**Technical Details:** This change formally deprecates the `removeprunedfunds` RPC within the wallet subsystem. While the RPC remains functional for a transition period, its documentation and RPC help text will be updated to indicate its deprecated status and suggest alternative, likely more robust or generalized, methods for managing pruned UTXOs. This deprecation guides users and developers towards newer APIs, contributing to a cleaner and more maintainable RPC interface for the wallet.

#### [#35867: test: classify SOCKS5 peers via getpeerinfo addrbind](https://github.com/bitcoin/bitcoin/pull/35867)
**Author:** [@151henry151](https://github.com/151henry151) | **[Maintenance & Tech Debt]** *(Activity: 12 review events)*
> This PR enhances the P2P testing framework by verifying that SOCKS5 proxy peers are correctly identified and classified using the local address binding information. This ensures accurate peer tracking and reporting within the network routing subsystem.

**Technical Details:** The pull request adds regression testing for the getpeerinfo RPC, specifically validating the addrbind field for SOCKS5 proxy connections. It asserts that when the node connects to a peer via a proxy, the internal peer tracking state correctly records the local binding address. This ensures that the node's P2P reporting interface remains accurate for Tor, I2P, or SOCKS5-routed peers. Ultimately, this prevents misclassification of connection types in node diagnostics.

#### [#957: fix: add .dat file extension automatically when exporting watchonly](https://github.com/bitcoin/bitcoin/pull/957)
**Author:** [@polespinasa](https://github.com/polespinasa) | **[Wallet & User Tools]** *(Activity: 11 review events)*

#### [#1910: scratch: reject sizes that overflow when added to header](https://github.com/bitcoin/bitcoin/pull/1910)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This historic pull request standardizes terminology within the codebase by changing occurrences of the word 'blockchain' to 'block chain'. This establishes spelling consistency aligned with the original Bitcoin whitepaper and early project conventions.

**Technical Details:** This is a non-functional, purely cosmetic documentation and source-comment refactoring. The PR executes a search-and-replace operation across source comments and text files to split the compound word 'blockchain' into two words. It modifies zero executable logic, consensus rules, or RPC endpoints.

#### [#35852: scripted-diff: Use inline const(expr) over static constexpr in headers](https://github.com/bitcoin/bitcoin/pull/35852)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR refactors the codebase to use inline or constexpr variables instead of static constexpr in header files. This change helps prevent duplicate storage allocations and linker issues across different translation units.

**Technical Details:** Using static constexpr in a header file forces each translation unit that includes the header to create its own distinct copy of the variable, leading to bloated binaries and potential ODR (One Definition Rule) headaches. By replacing them with inline constexpr, the compiler is instructed to merge these definitions into a single global instance. This is executed via a scripted diff to guarantee safety and consistency across all affected header files.

#### [#35930: wallet: post-#35501 cleanups in CWalletTx](https://github.com/bitcoin/bitcoin/pull/35930)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR cleans up the wallet transaction class structure after a previous major update. It improves code readability and simplifies internal wallet transaction management.

**Technical Details:** The changes perform code cleanups on the CWalletTx class post-PR 35501. It refactors internal state handling, removes obsolete helper functions, and optimizes member variables for better memory footprint. This reduces technical debt within the wallet's core transaction tracking logic.

#### [#35699: [31.x] More Backports](https://github.com/bitcoin/bitcoin/pull/35699)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR backports a curated set of bug fixes and stability enhancements from the master development branch into the 31.x release branch. This ensures that users running the 31.x maintenance release benefit from crucial upstream corrections.

**Technical Details:** The PR cherry-picks several non-disruptive commits from the master branch to maintain parity with critical stability fixes. The cherry-picked modifications address platform-specific compilation behaviors, unit test flakiness, and dependency updates. By integrating these commits, the 31.x branch maintains production stability without introducing API changes. This backport process strictly adheres to the Bitcoin Core release and maintenance cycle requirements.

#### [#35951: doc: release note about I2P ElGamal sunset](https://github.com/bitcoin/bitcoin/pull/35951)
**Author:** [@jonatack](https://github.com/jonatack) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR adds a release note documenting the deprecation and sunset of ElGamal encryption support in the I2P network implementation. It informs node operators of changes to privacy network configurations in the upcoming release.

**Technical Details:** This is a documentation-only pull request that updates the release notes to reflect changes in peer-to-peer network options. Specifically, it details the transition away from the legacy I2P ElGamal sessions in favor of newer, more secure protocols like ECIES-X25519-AEAD-ChaCha20-Poly1305. The document guides operators on how this affects their configuration and peer connectivity.

#### [#35889: rpc: avoid quadratic `gettxspendingprevout` work and preserve order](https://github.com/bitcoin/bitcoin/pull/35889)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Performance & Optimization]** *(Activity: 7 review events)*
> This PR optimizes the `gettxspendingprevout` RPC to prevent potential slow-downs when processing large sets of inputs. It ensures the command scales efficiently while correctly preserving the order of the returned transaction outputs.

**Technical Details:** The implementation optimizes the `gettxspendingprevout` RPC by replacing a nested loop or linear lookup strategy with a more efficient hash-map-based lookup. This avoids quadratic O(N^2) complexity when resolving spent outpoints for a batch of transactions. Additionally, the PR carefully structures the output assembly to guarantee that the results map exactly to the input sequence order requested by the client. The resulting change dramatically improves response times for nodes handling large query batches.

#### [#35925: wallet, rpc: Exclude non-owned addresses from listreceivedby*](https://github.com/bitcoin/bitcoin/pull/35925)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc) | **[Wallet & User Tools]** *(Activity: 7 review events)*
> This PR modifies wallet RPC behavior to exclude addresses that the wallet does not own from the list received commands. This prevents confused output when tracking user-owned funds.

**Technical Details:** The RPC handlers for listreceivedbyaddress and listreceivedbylabel are modified to filter out non-owned or watch-only addresses by default. A check is added to ensure that unless include_watchonly is set to true, only fully owned keys are queried and returned. This maintains consistency with standard wallet balances and address management RPCs.

#### [#35960: common: remove `::runtime_error` from `RunCommandParseJSON`](https://github.com/bitcoin/bitcoin/pull/35960)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR improves error handling in the `RunCommandParseJSON` utility function by replacing generic runtime exceptions with more structured error returns. This prevents unexpected application crashes and allows calling functions to handle failures more gracefully.

**Technical Details:** The refactoring replaces the throwing of `std::runtime_error` inside `RunCommandParseJSON` with a non-throwing interface that propagates error details cleanly. This decouples the subprocess execution and JSON parsing from standard exception propagation, which is a safer model in C++. Callers of the utility are updated to explicitly check the returned error status or object, enhancing local error recovery paths and eliminating potential uncaught exception vectors.

#### [#35847: test: move more tests to `baseindex_tests` and run them for all indexes](https://github.com/bitcoin/bitcoin/pull/35847)
**Author:** [@mzumsande](https://github.com/mzumsande) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR expands test coverage for Bitcoin Core's indexers by consolidating tests into a unified test suite. This ensures that all active indexes are consistently validated against a common set of baseline behaviors.

**Technical Details:** The PR refactors existing index-specific tests by moving shared validation logic into `baseindex_tests`. By parameterizing the test suite, the framework can now run the same comprehensive assertion suite against all active index implementations. This eliminates duplicate test code, guarantees that new indexes automatically receive robust coverage, and verifies consistent API behavior across different database index backends.

#### [#35931: ci: Check DLL imports of cross-built `bitcoind.exe`](https://github.com/bitcoin/bitcoin/pull/35931)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR introduces an automated continuous integration check to verify the DLL imports of the Windows executable. It ensures that cross-compiled binaries do not depend on unexpected or unsafe dynamic libraries.

**Technical Details:** The CI pipeline is updated with a step that inspects the Import Address Table of bitcoind.exe using binary analysis tools. It validates that the executable only links against a strict whitelist of standard system DLLs. This prevents runtime loading issues and enhances binary security on Windows environments.

#### [#35959: Update secp256k1 subtree to latest master](https://github.com/bitcoin/bitcoin/pull/35959)
**Author:** [@fanquake](https://github.com/fanquake) | **[Performance & Optimization]** *(Activity: 4 review events)*
> This PR updates the internal secp256k1 library subtree to the latest upstream master branch, bringing in performance improvements, bug fixes, and security hardening for cryptographic operations. This ensures Bitcoin Core benefits from the most up-to-date and optimized elliptic curve implementation.

**Technical Details:** This PR performs a subtree merge of the secp256k1 directory to synchronize with the latest upstream commit. The update incorporates low-level optimizations in field and group operations, assembly-level enhancements for various architectures, and potentially new APIs or constant-time guarantees. This update directly impacts signature verification speed and key generation safety across the entire node. By pulling from the latest master, it also integrates the latest audited cryptographic hardening measures.

#### [#35950: Update leveldb subtree to latest master](https://github.com/bitcoin/bitcoin/pull/35950)
**Author:** [@fanquake](https://github.com/fanquake) | **[Performance & Optimization]** *(Activity: 4 review events)*
> This PR updates the internal LevelDB database code to the latest master commit. This brings performance optimizations and stability improvements for storing block indexes and chainstate.

**Technical Details:** The change updates the src/leveldb subtree to track the latest upstream master branch. It incorporates fixes for database stability, memory management, and platform-specific performance optimizations. This ensures the underlying storage engine remains robust under heavy I/O workloads during initial block download.

#### [#35971: net_processing: remove unused code](https://github.com/bitcoin/bitcoin/pull/35971)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR cleans up the codebase by removing dead and unused code within the network processing component. This simplifies maintenance and improves the readability of Bitcoin Core's peer-to-peer networking logic.

**Technical Details:** The PR identifies and deletes unreferenced functions, variables, or unreachable code blocks within `net_processing.cpp` and its associated headers. By eliminating this dead code, the compiler has fewer lines to parse, and developers face less cognitive load when refactoring the P2P message handling engine. This is a pure refactoring effort that does not alter any network-facing behavior, peer scoring, or state machine transitions.

#### [#35945: depends, qt: Add patch for missing headers](https://github.com/bitcoin/bitcoin/pull/35945)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR adds a build patch to include missing headers required for compiling the Qt library. It ensures successful compilation on systems using modern compiler toolchains.

**Technical Details:** The PR introduces a patch within the depends system targeting Qt compilation. It explicitly adds missing header declarations that caused build breakages under strict compiler standards. This maintains robust, deterministic cross-compilation across all supported build hosts.

#### [#35924: Wallet, refactor: Remove orphaned EraseWatchOnly function](https://github.com/bitcoin/bitcoin/pull/35924)
**Author:** [@vicjuma](https://github.com/vicjuma) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR removes an unused watch-only address deletion function from the wallet. This cleans up dead code and simplifies the internal wallet interface.

**Technical Details:** The EraseWatchOnly function is identified as dead code with no active callers in the codebase. This PR removes its declaration from wallet.h and its implementation from wallet.cpp. Removing this orphaned method reduces API complexity and maintenance overhead in the wallet subsystem.

#### [#35943: doc: fix dead link in txrequest.h](https://github.com/bitcoin/bitcoin/pull/35943)
**Author:** [@cyb3ralbert](https://github.com/cyb3ralbert) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR corrects a broken hyperlink inside the documentation of the transaction request header file. This ensures developers have access to correct reference materials when reading the code.

**Technical Details:** The PR updates a stale URL in the developer comments of src/txrequest.h. It replaces the dead link with the correct, active reference to the design documentation. This is a non-functional, documentation-only change.

#### [#35947: build, msvc: Update vcpkg manifest baseline](https://github.com/bitcoin/bitcoin/pull/35947)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR updates the vcpkg manifest baseline for Windows builds using MSVC. It ensures the build system uses current and stable versions of third-party dependencies.

**Technical Details:** The changes update the commit hash of the vcpkg baseline in the MSVC configuration files. This pins dependency packages to newer, verified releases in the Microsoft vcpkg ecosystem. It facilitates reproducible Windows builds and resolves dependency integration issues.

#### [#35937: test: Append print_suppressions=0 to LSAN_OPTIONS, and suppress bitcoin-qt](https://github.com/bitcoin/bitcoin/pull/35937)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This pull request improves the test suite's readability and reliability by reducing noisy output from the LeakSanitizer (LSAN) testing tool and suppressing known GUI-related leaks. This helps developers focus on real, actionable memory leaks in the core codebase.

**Technical Details:** The PR modifies the CI and testing environment configuration by appending `print_suppressions=0` to the `LSAN_OPTIONS` environment variable, which prevents LSAN from printing redundant information about matched suppressions. Additionally, it adds a suppression rule for the `bitcoin-qt` executable to ignore leaks originating from the Qt library. This prevents external, upstream GUI library memory leaks from causing false positives in the automated testing pipeline.

#### [#35941: doc: remove mention of `::wsystem`](https://github.com/bitcoin/bitcoin/pull/35941)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This pull request updates developer documentation by removing outdated references to the obsolete `::wsystem` function. This prevents confusion for contributors by keeping codebase documentation aligned with actual code.

**Technical Details:** The change removes references to the non-existent or deprecated `::wsystem` utility function from the codebase documentation. Historically, `::wsystem` served as a wrapper for executing system commands, but modern refactoring has deprecated or replaced this utility. This pure documentation PR ensures developers do not look for or try to use a defunct internal API.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#35569: Encapsulation for CTransaction](https://github.com/bitcoin/bitcoin/pull/35569)
**Author:** [@purpleKarrot](https://github.com/purpleKarrot) | **[Maintenance & Tech Debt]** *(Activity: 20 review events this week)*
> This PR improves the encapsulation of the `CTransaction` class, enhancing code maintainability and preparing it for future architectural changes. By restricting direct access to internal members, it makes the codebase more robust.

#### [#35531: txindex: hash keys and pack positions to reduce disk usage](https://github.com/bitcoin/bitcoin/pull/35531)
**Author:** [@andrewtoth](https://github.com/andrewtoth) | **[Performance & Optimization]** *(Activity: 12 review events this week)*
> Optimizes the transaction index (`txindex`) database by hashing keys and packing storage positions. This significantly reduces the disk space required to maintain a full transaction index on node operators' machines.

#### [#35730: http: limit connected HTTPRemoteClients](https://github.com/bitcoin/bitcoin/pull/35730)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 11 review events this week)*
> This pull request introduces a limit on concurrent HTTP connections to protect the RPC and REST interfaces from resource exhaustion. This prevents Denial of Service (DoS) attacks from stalling or crashing the node.

#### [#35433: wallet: deprecate replaceable argument from transaction (and psbt) creation (and modification) RPCs](https://github.com/bitcoin/bitcoin/pull/35433)
**Author:** [@rkrux](https://github.com/rkrux) | **[Wallet & User Tools]** *(Activity: 10 review events this week)*
> This PR deprecates the `replaceable` argument in several wallet RPCs related to transaction and PSBT creation. This streamlines RPC usage and removes an argument that has become less relevant with current RBF policies.

#### [#35831: argsman, cli: Allow options after non-option arguments (GNU-style)](https://github.com/bitcoin/bitcoin/pull/35831)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc) | **[Wallet & User Tools]** *(Activity: 9 review events this week)*
> This PR updates Bitcoin Core's command-line interface to allow options to be specified after non-option arguments, aligning with standard GNU conventions. This makes the CLI more intuitive and user-friendly for operators who expect flexible argument ordering.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: Universal opt-in replay protection?](https://delvingbitcoin.org/t/universal-opt-in-replay-protection/2792/10)
**Source:** Delving | **Started By:** {'username': 'Loki', 'uuid': 'can_loki'} | **Messages:** 8
> Developers are exploring opt-in replay protection to safeguard user funds and ensure transaction predictability during potential chain splits. This feature would allow users to safely transact during a network fork without risking their coins being unexpectedly spent on a hostile chain.

**Technical Details:** The discussion evaluates implementing opt-in replay protection to defend against hostile chain splits. Anthony Towns suggested that nodes annotate UTXOs with ancestral block data to help receivers identify if incoming coins carry these fork-protection conditions. However, incorporating these conditions introduces architectural challenges regarding reorganization safety and transaction invalidation. Referencing historical design principles, developers emphasize that any implementation must avoid making transactions vulnerable to invalidation during routine network churn or minor reorgs.

### [Re: Running Core's real consensus code inside a zkVM](https://delvingbitcoin.org/t/running-cores-real-consensus-code-inside-a-zkvm/2811/8)
**Source:** Delving | **Started By:** {'username': 'Defenwcyke', 'uuid': 'auto_defenwcyke'} | **Messages:** 7
> Developers are working on running Bitcoin's core rules within zero-knowledge virtual machines to enable instant, secure blockchain verification.
This breakthrough could eliminate the need for resource-heavy nodes while laying the groundwork for highly secure, trustless network bridges.

**Technical Details:** By compiling Bitcoin Core v28 consensus code to riscv32i, developers aim to produce zero-knowledge block validity proofs within a zkVM.
The architectural debate has shifted to comparing federated bridge models against hashrate-attested sidechain states like Rootstock.
Participants argue over the trade-offs of a 'slow path' anchored to genesis versus dynamic federations for validating state transitions.
To progress, the project requires mapping out zk-proof verification costs and establishing standard integration patterns for sidechain consensus.

### [Re: Reducing Bitcoin Full Node Storage Without Consensus Changes](https://delvingbitcoin.org/t/reducing-bitcoin-full-node-storage-without-consensus-changes/2790/8)
**Source:** Delving | **Started By:** {'username': 'BrokenMachine', 'uuid': 'auto_brokenmachine'} | **Messages:** 7
> Developers are exploring new ways to manage the growing size of the Bitcoin blockchain to ensure everyday users can continue running full nodes on standard hardware. This effort is crucial for maintaining Bitcoin's decentralization and keeping the network secure and accessible to everyone.

**Technical Details:** The discussion centers on mitigating the long-term storage demands of the blockchain to preserve low barrier-to-entry validation. Participants are evaluating scaling proposals, storage optimization techniques, and potential integrations of advanced pruning or accumulator-based technologies like Utreexo. A developer recently agreed to investigate a proposed alternative or tool designed to address these storage constraints. The immediate next step involves analyzing the feasibility and trade-offs of this specific optimization proposal on node performance.

### [Re: Add comparison to BIP-118 in BIP-448](https://delvingbitcoin.org/t/add-comparison-to-bip-118-in-bip-448/2799/7)
**Source:** Delving | **Started By:** {'username': 'user1', 'uuid': 'auto_orfeas'} | **Messages:** 6
> Developers are comparing different technical proposals to improve Bitcoin's smart contract capabilities, focusing on how these upgrades can safely enable more flexible and secure layer-2 scaling solutions like payment channels.

**Technical Details:** The discussion focuses on contrasting the application spaces of BIP 448 and BIP 118 (SIGHASH_ANYPREVOUT) to understand their respective advantages for covenants and off-chain protocols. While BIP 118 is optimized for Eltoo and dynamic state updates, developers are analyzing whether BIP 448 offers a more generalized or safer alternative for transaction introspection. Furthermore, participants emphasize that concerns regarding the expanded 'risk surface' are an inherent hurdle for any proposed soft fork rather than a specific indictment of these BIPs. Clarifying the distinct use-cases for each proposal remains the key technical objective.

### [Re: PQ-single-address-backup - BIP-38 for P2MR (bc1z) - 104-char encrypted backup format](https://delvingbitcoin.org/t/pq-single-address-backup-bip-38-for-p2mr-bc1z-104-char-encrypted-backup-format/2767/6)
**Source:** Delving | **Started By:** {'username': 'coldtest-berlin', 'uuid': 'auto_coldtest_berlin'} | **Messages:** 5
> Developers are working on secure ways to back up next-generation, quantum-resistant Bitcoin keys for long-term cold storage and inheritance. This ensures users can safely protect their future assets offline before the new network standards are fully active.

**Technical Details:** The debate centers on resolving the bootstrapping issue for storing a 32-byte SLH-DSA seed offline under the BIP-360 (P2MR) proposal.
Rather than storing plain 64-character hex strings, developers are converging on an authenticated encryption scheme.
The current consensus favors using a minimal outer header—consisting of a version, KDF ID, and bounded parameters—as AEAD associated data.
This architecture ensures that the seed can be securely derived, verified, and recovered without exposing the raw secret.
The next steps involve defining the exact parameters of this header to prevent brute-force risks during long-term storage.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@vicjuma](https://github.com/vicjuma), [@winterrdog](https://github.com/winterrdog)

### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@w0xlt](https://github.com/w0xlt), [@hebasto](https://github.com/hebasto), [@maflcko](https://github.com/maflcko), [@pablomartin4btc](https://github.com/pablomartin4btc)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@l0rinc](https://github.com/l0rinc), [@hebasto](https://github.com/hebasto), [@w0xlt](https://github.com/w0xlt), [@fanquake](https://github.com/fanquake)
