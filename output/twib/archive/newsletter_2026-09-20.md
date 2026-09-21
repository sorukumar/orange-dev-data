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
> This PR updates the Bitcoin Core version to 32.0rc1, signifying the release candidate phase of the upcoming major version. This marks a crucial step towards the official release, indicating the software is stable for broader testing.

**Technical Details:** The change primarily involves modifying the version definition files, such as `src/version.h`, to update the `CLIENT_VERSION_MAJOR`, `CLIENT_VERSION_MINOR`, `CLIENT_VERSION_REVISION`, and `CLIENT_VERSION_BUILD` constants, along with the `CLIENT_VERSION_IS_RELEASE` flag. This ensures that all compiled binaries and user interfaces correctly reflect the '32.0rc1' version string, aligning with the project's release cycle.

#### [#36207: kernel: add wtxid accessor](https://github.com/bitcoin/bitcoin/pull/36207)
**Author:** [@KY-U](https://github.com/KY-U) | **[Strategic Initiatives]** *(Activity: 16 review events)*
> This PR adds an accessor for the wtxid (witness transaction ID) within the Bitcoin Core kernel. This provides a standardized way for kernel components to retrieve this identifier, which is crucial for SegWit-enabled transactions.

**Technical Details:** A new method is introduced to a relevant kernel structure (e.g., `CTransaction` or `CTransactionRef`) to compute and return the wtxid. This accessor encapsulates the logic for calculating the wtxid, which differs from the traditional txid for SegWit transactions, making it readily available for internal kernel use without redundant computation. This is a key step in formalizing the kernel's transaction interface.

#### [#36230: wallet: Improve `HasWalletDescriptor` performance and other canonical descriptor string followups](https://github.com/bitcoin/bitcoin/pull/36230)
**Author:** [@achow101](https://github.com/achow101) | **[Performance & Optimization]** *(Activity: 16 review events)*
> This PR significantly improves the performance of the `HasWalletDescriptor` function and other descriptor string operations within the wallet component. This optimization leads to faster wallet loading, scanning, and overall responsiveness, especially for wallets with many descriptors.

**Technical Details:** The optimization targets the internal representation and comparison logic for wallet descriptors, particularly improving the efficiency of canonicalizing descriptor strings. This likely involves optimizing string hashing, caching canonical forms, or using more efficient data structures for descriptor lookups and comparisons. The changes aim to reduce the computational overhead associated with managing and querying descriptor strings within the wallet's descriptor pool, leading to quicker operations for descriptor-based wallets.

#### [#36256: guix: Update osslsigncode to 2.14](https://github.com/bitcoin/bitcoin/pull/36256)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 16 review events)*
> This PR updates the `osslsigncode` dependency within the Guix build system to version 2.14. This ensures that the reproducible builds for Windows binaries use the latest signing tool, potentially incorporating security fixes or compatibility improvements.

**Technical Details:** The Guix manifest for reproducible builds is updated to specify `osslsigncode` version 2.14. This change affects the build environment used to create signed Windows executables, ensuring that the signing process benefits from the latest features or bug fixes in the `osslsigncode` utility. It's a dependency update specific to the build system, maintaining build tool currency.

#### [#36194: kernel: expose block header Merkle root](https://github.com/bitcoin/bitcoin/pull/36194)
**Author:** [@lucasdbr05](https://github.com/lucasdbr05) | **[Strategic Initiatives]** *(Activity: 14 review events)*
> This PR exposes the Merkle root from the block header within the Bitcoin Core kernel interface. This allows other components or future extensions to easily access this fundamental block identifier without re-parsing the header.

**Technical Details:** A new accessor method is added to the `CBlockHeader` or a related kernel structure, specifically to retrieve the Merkle root hash. This change provides a direct programmatic interface to this critical piece of block data, simplifying its use in other kernel modules or higher-level components that interact with block headers. This is a step towards a more modular and well-defined kernel API.

#### [#36297: rpc: Correct invalid OpenRPC defaults](https://github.com/bitcoin/bitcoin/pull/36297)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Wallet & User Tools]** *(Activity: 13 review events)*
> This PR corrects inaccuracies in the OpenRPC schema definitions for various RPC methods. Accurate OpenRPC documentation improves the usability and discoverability of Bitcoin Core's RPC interface for developers.

**Technical Details:** The change involves updating the OpenRPC schema, which provides a machine-readable description of the RPC API, to reflect the correct default values for parameters in several RPC calls. This ensures that tools consuming the OpenRPC specification, such as client generators or documentation viewers, receive accurate information. It's a documentation-level fix that improves API consistency and developer experience.

#### [#36083: test: cover getrawtransaction on a stale block via txindex](https://github.com/bitcoin/bitcoin/pull/36083)
**Author:** [@arejula27](https://github.com/arejula27) | **[Maintenance & Tech Debt]** *(Activity: 13 review events)*
> This PR adds a test to verify that the `getrawtransaction` RPC can correctly retrieve transactions from stale blocks when `txindex` is enabled. This ensures reliable transaction lookups even during chain reorganizations.

**Technical Details:** The PR introduces a new functional test that simulates a chain reorganization scenario. It first mines a transaction into a block, then creates a fork to make that block stale. With `txindex` enabled, the test then calls `getrawtransaction` for the transaction in the now-stale block. The test asserts that the RPC successfully retrieves the transaction data, confirming `txindex`'s ability to track transactions across different chain branches.

#### [#36081: rpc: add bestblockhash to getmininginfo](https://github.com/bitcoin/bitcoin/pull/36081)
**Author:** [@jakubtrnka](https://github.com/jakubtrnka) | **[Wallet & User Tools]** *(Activity: 11 review events)*
> This PR enhances the `getmininginfo` RPC by including the hash of the best known block. This provides miners and pool operators with immediate, critical information about the current chain tip directly from the mining information RPC.

**Technical Details:** The `getmininginfo` RPC handler is modified to query the current chain state for the block hash of the active chain tip. This `BlockHash` is then added as a new key, `bestblockhash`, to the JSON object returned by the RPC call. The implementation involves accessing the `m_chain_tip` from `CChainState` and serializing its hash into the RPC response structure.

#### [#35819: test: add coverage for untested descriptor parse error paths](https://github.com/bitcoin/bitcoin/pull/35819)
**Author:** [@azuchi](https://github.com/azuchi) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*
> This PR expands test coverage for previously untested error paths in descriptor parsing. By ensuring these error conditions are properly handled and tested, it improves the robustness and reliability of descriptor functionality.

**Technical Details:** New unit tests are added to specifically target various invalid or malformed descriptor strings that previously lacked explicit test coverage. These tests verify that the descriptor parsing logic correctly identifies and reports errors for these edge cases, preventing unexpected behavior or crashes when encountering invalid inputs. This enhances the overall quality and resilience of descriptor handling.

#### [#36260: torcontrol: Use reconnect backoff after dropped connections](https://github.com/bitcoin/bitcoin/pull/36260)
**Author:** [@fjahr](https://github.com/fjahr) | **[Network & Privacy]** *(Activity: 9 review events)*
> This PR enhances Bitcoin Core's Tor control connection stability by implementing a reconnect backoff mechanism. This prevents rapid, repeated connection attempts that could overload the Tor control port after a dropped connection.

**Technical Details:** The `torcontrol` module is modified to introduce an exponential backoff strategy for reconnecting to the Tor control port after a connection failure or drop. Instead of immediately retrying, the client will wait for increasing intervals, reducing resource consumption and potential rate-limiting issues with the Tor daemon. This improves the robustness of Tor service management and overall network reliability.

#### [#36285: refactor: Use static const over inline const to work around ld64 bug](https://github.com/bitcoin/bitcoin/pull/36285)
**Author:** [@maflcko](https://github.com/maflcko) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR refactors code to use `static const` instead of `inline const` to work around a specific bug in the `ld64` linker. This ensures compatibility with certain build environments and prevents compilation issues.

**Technical Details:** The change modifies variable declarations from `inline const` to `static const` in specific contexts. This refactoring is a workaround for a known bug in the `ld64` linker, which can misinterpret `inline const` variables in certain scenarios, leading to linker errors. By using `static const`, the linker's behavior is normalized, ensuring successful compilation and linking on affected platforms without altering the program's runtime semantics.

#### [#35961: depends: Update `boost` package to 1.92.0](https://github.com/bitcoin/bitcoin/pull/35961)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR updates the Boost C++ libraries dependency to version 1.92.0. This ensures Bitcoin Core benefits from the latest bug fixes, performance enhancements, and security updates provided by this fundamental library.

**Technical Details:** The `depends` system configuration is updated to fetch and build Boost version 1.92.0. This involves modifying the build scripts and potentially adjusting any code that relies on specific Boost features or APIs if there were breaking changes (though typically minor for patch versions). This is a standard dependency upgrade, keeping the project current with its foundational libraries.

#### [#36093: test: ipc should reject invalid json](https://github.com/bitcoin/bitcoin/pull/36093)
**Author:** [@Sjors](https://github.com/Sjors) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR adds a test to confirm that the Inter-Process Communication (IPC) interface properly rejects invalid JSON inputs. This ensures the IPC system is robust against malformed requests and handles errors gracefully.

**Technical Details:** The PR introduces a new functional test within the IPC test suite. This test specifically crafts and sends malformed JSON strings to the IPC server. It then asserts that the IPC interface responds with an appropriate error message or status, indicating that the invalid JSON was correctly identified and rejected, preventing potential parsing issues or unexpected behavior.

#### [#36249: net, rpc: Asmap version improvements/follow-ups](https://github.com/bitcoin/bitcoin/pull/36249)
**Author:** [@fjahr](https://github.com/fjahr) | **[Network & Privacy]** *(Activity: 8 review events)*
> This PR introduces improvements and follow-ups related to the Asmap versioning, enhancing how Bitcoin Core understands and utilizes network topology. These changes contribute to more robust peer selection and network resilience.

**Technical Details:** The changes likely involve refining the handling and interpretation of Asmap (Autonomous System map) data, potentially including updates to the versioning scheme or the logic for applying Asmap-based peer selection rules. This affects both the network layer (net) for peer management and potentially RPC interfaces for querying or configuring Asmap behavior. Improved Asmap handling helps nodes connect to a more diverse set of peers, reducing the risk of eclipse attacks and improving network decentralization.

#### [#35472: test: add coverage for feebumper uncomputable cluster error path](https://github.com/bitcoin/bitcoin/pull/35472)
**Author:** [@151henry151](https://github.com/151henry151) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR adds test coverage for an error path in the feebumper logic where a transaction cluster's feerate cannot be computed. This improves the robustness of the feebumper by ensuring it handles complex and uncomputable scenarios gracefully.

**Technical Details:** The PR introduces a new functional test case that specifically constructs a mempool state designed to trigger the 'uncomputable cluster feerate' error path within the feebumper. The test then invokes the feebumper and asserts that it correctly identifies this error condition, returning an expected error code or message, rather than crashing or producing an incorrect feerate calculation. This validates the error handling for complex transaction graph scenarios.

#### [#36267: [32.x] Backports for rc2](https://github.com/bitcoin/bitcoin/pull/36267)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR bundles a set of backported changes intended for the second release candidate of the 32.x branch. It prepares the upcoming release by integrating important fixes and improvements from the main development branch.

**Technical Details:** This is a meta-PR that aggregates multiple individual commits from the master branch into the 32.x release branch, specifically targeting the `rc2` milestone. The backporting process involves carefully cherry-picking or rebasing relevant changes, resolving any conflicts, and ensuring compatibility with the target branch's codebase. This is a standard procedure in release management to stabilize a release candidate with necessary updates.

#### [#36160: refactor: Minor improvements to HTTP unit tests](https://github.com/bitcoin/bitcoin/pull/36160)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR introduces minor refactoring and improvements to the HTTP unit tests. These changes enhance the clarity and robustness of existing tests, making them easier to maintain and more effective at catching regressions.

**Technical Details:** The HTTP unit test suite is refactored to improve readability and maintainability. This likely involves consolidating common test patterns, clarifying assertions, or restructuring test cases to better isolate specific functionalities. The changes are purely within the test code and do not alter any production logic, focusing on code quality within the testing framework.

#### [#36286: crypto: Fix MuHash3072 division by itself](https://github.com/bitcoin/bitcoin/pull/36286)
**Author:** [@fjahr](https://github.com/fjahr) | **[Security & Consensus]** *(Activity: 6 review events)*
> This PR fixes a bug in the MuHash3072 cryptographic algorithm where a division by itself could occur. Correcting this ensures the integrity and reliability of the MuHash implementation, which is critical for certain internal data structures.

**Technical Details:** The fix addresses a specific edge case in the MuHash3072 implementation where a division operation might incorrectly use the divisor as the dividend. This correction ensures the mathematical correctness of the hash function, preventing potential miscalculations or vulnerabilities that could arise from incorrect hash outputs. While MuHash3072 is not currently part of the consensus rules, its correctness is vital for features like assumeutxo, where it's used for UTXO set commitments.

#### [#36206: build: drop use of `OBJC_OLD_DISPATCH_PROTOTYPES`](https://github.com/bitcoin/bitcoin/pull/36206)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR removes an outdated Objective-C compiler flag from the build system. This cleanup modernizes the build process and removes compatibility cruft that is no longer necessary.

**Technical Details:** The change specifically targets the `OBJC_OLD_DISPATCH_PROTOTYPES` preprocessor definition, which was historically used for compatibility with older Objective-C runtimes. Modern compilers and SDKs no longer require this flag, so its removal simplifies the build configuration. This is a minor build system refactoring that improves maintainability without altering runtime behavior.

#### [#36251: rest: add `generated` and `height` to spenttxouts JSON](https://github.com/bitcoin/bitcoin/pull/36251)
**Author:** [@0xB10C](https://github.com/0xB10C) | **[Wallet & User Tools]** *(Activity: 5 review events)*
> This PR enhances the `spenttxouts` REST endpoint by adding `generated` and `height` fields to its JSON output. This provides more comprehensive information about spent transaction outputs, which is valuable for block explorers and other applications.

**Technical Details:** The `spenttxouts` REST handler is modified to include two new fields, `generated` (boolean indicating coinbase) and `height` (block height), in the JSON response for each spent transaction output. This involves querying the UTXO set or block index for this additional data and serializing it into the existing REST API structure, providing richer context for spent outputs without requiring additional lookups by the client.

#### [#36186: test: return False for a too-short ECDSA signature](https://github.com/bitcoin/bitcoin/pull/36186)
**Author:** [@fametrano](https://github.com/fametrano) | **[Maintenance & Tech Debt]** *(Activity: 5 review events)*
> This PR adds a test to ensure that ECDSA signature validation correctly returns `False` for signatures that are too short. This improves the robustness of signature processing by verifying early rejection of malformed inputs.

**Technical Details:** The change introduces a new unit test case specifically designed to provide an ECDSA signature byte array that is shorter than the minimum required length. The test asserts that the signature verification function, when presented with such an input, correctly returns `False`, confirming the expected behavior of rejecting invalidly sized signatures without attempting further cryptographic operations. This validates the early exit condition for malformed signature data.

#### [#35849: init: don't suggest -reindex-chainstate for recovery on a pruned node](https://github.com/bitcoin/bitcoin/pull/35849)
**Author:** [@kwsantiago](https://github.com/kwsantiago) | **[Wallet & User Tools]** *(Activity: 4 review events)*
> This PR refines the node initialization process to avoid suggesting `-reindex-chainstate` for recovery on pruned nodes. This prevents user confusion and guides them towards relevant recovery options for their specific node configuration.

**Technical Details:** The change modifies the startup and error handling logic within the `init` module. Before suggesting `-reindex-chainstate` as a recovery option, the code now checks if the node is operating in pruned mode. If pruning is enabled, the suggestion for `-reindex-chainstate` is suppressed, as it is not applicable or effective for a pruned blockchain state, thereby improving user guidance.

#### [#36288: build: Revert remove `cmake/script/CoverageFuzz.cmake`"](https://github.com/bitcoin/bitcoin/pull/36288)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR reverts a previous change that removed a CMake script related to coverage and fuzzing. Reinstating this script ensures that important testing infrastructure remains available for development and quality assurance.

**Technical Details:** The PR specifically reverts the removal of `cmake/script/CoverageFuzz.cmake`, which likely contains CMake logic for configuring code coverage analysis and fuzzing targets. By bringing this file back, the build system can properly integrate and execute these testing methodologies. This ensures that the project's continuous integration and testing pipelines remain fully functional for identifying bugs and vulnerabilities.

#### [#36282: Update leveldb subtree to latest master](https://github.com/bitcoin/bitcoin/pull/36282)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR updates the embedded LevelDB library to its latest master version. This ensures Bitcoin Core benefits from the most recent bug fixes and performance improvements in its UTXO database.

**Technical Details:** The `src/leveldb` subtree is updated to the latest upstream master branch. This involves pulling changes from the LevelDB repository, potentially resolving any known issues or incorporating optimizations made to the key-value store used for the UTXO set. This is a routine dependency update that maintains the currency of a critical internal component.

#### [#36253: fuzz: Use `LIMITED_WHILE` in `connect_block`](https://github.com/bitcoin/bitcoin/pull/36253)
**Author:** [@marcofleon](https://github.com/marcofleon) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR enhances the `connect_block` fuzzer by utilizing `LIMITED_WHILE`. This change improves the efficiency and effectiveness of fuzz testing for block connection logic, helping to uncover edge cases and potential vulnerabilities more thoroughly.

**Technical Details:** The `connect_block` fuzzer is updated to incorporate the `LIMITED_WHILE` macro. This macro provides a bounded loop mechanism for fuzzing inputs, preventing infinite loops or excessive execution times during fuzzing runs while still exploring a wide range of inputs. This makes the fuzzer more robust and performant, improving the quality of automated testing.

#### [#36198: http: Add missing LIFETIMEBOUND annotations](https://github.com/bitcoin/bitcoin/pull/36198)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR adds `LIFETIMEBOUND` annotations to HTTP-related code, which helps static analysis tools detect potential lifetime issues. This improves code quality and reduces the risk of subtle memory-related bugs.

**Technical Details:** The `LIFETIMEBOUND` attribute is a Clang-specific annotation used to inform the static analyzer about the lifetime relationship between function arguments and return values. By adding these annotations to HTTP-related functions, the compiler can more effectively identify potential use-after-free or dangling pointer issues. This is a code quality improvement that aids in compile-time bug detection.

#### [#36247: iwyu: Always keep `bitcoin-build-info.h` header](https://github.com/bitcoin/bitcoin/pull/36247)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 2 review events)*
> This PR adjusts the `include-what-you-use` (IWYU) configuration to always retain the `bitcoin-build-info.h` header. This ensures that essential build-time information is consistently included across the codebase, improving build system reliability.

**Technical Details:** The change involves adding a specific rule to the IWYU configuration files, typically via a `.iwyu` mapping file or command-line argument. This rule explicitly marks `bitcoin-build-info.h` as a 'keep' header, preventing IWYU from suggesting its removal even if its direct symbols are not explicitly used in a translation unit. This guarantees the presence of build-specific metadata in all relevant compilation units.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#36233: guix: Update time-machine to `60f6956aeffa7f30285745bd0ea615e9acfc74f8`](https://github.com/bitcoin/bitcoin/pull/36233)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 20 review events this week)*
> This PR updates the Guix build system's time-machine reference, ensuring that reproducible builds use the latest specified environment. This helps maintain the integrity and consistency of Bitcoin Core's deterministic build process.

#### [#36246: [32.x] Bump to 32.0rc1](https://github.com/bitcoin/bitcoin/pull/36246)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 18 review events this week)*
> This PR updates the Bitcoin Core version to 32.0rc1, signifying the release candidate phase of the upcoming major version. This marks a crucial step towards the official release, indicating the software is stable for broader testing.

#### [#36182: fees: return `block_policy` fee rate estimate when `mempool_policy` is not ready](https://github.com/bitcoin/bitcoin/pull/36182)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Wallet & User Tools]** *(Activity: 17 review events this week)*
> This PR improves Bitcoin Core's fee estimation service by providing a `block_policy` estimate when the preferred `mempool_policy` estimate is unavailable. This ensures users consistently receive a reliable fee recommendation, even during startup or network congestion.

#### [#36256: guix: Update osslsigncode to 2.14](https://github.com/bitcoin/bitcoin/pull/36256)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 16 review events this week)*
> This PR updates the `osslsigncode` dependency within the Guix build system to version 2.14. This ensures that the reproducible builds for Windows binaries use the latest signing tool, potentially incorporating security fixes or compatibility improvements.

#### [#36250: rpc: clarify sendrawtransaction decode error](https://github.com/bitcoin/bitcoin/pull/36250)
**Author:** [@MrHodlX](https://github.com/MrHodlX) | **[Wallet & User Tools]** *(Activity: 15 review events this week)*
> This PR improves the error message for the `sendrawtransaction` RPC when a transaction fails to decode. It provides clearer feedback to users, making it easier to diagnose issues with malformed transactions.

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
> Developers are actively working on new rules to prevent manipulation of block timestamps, which is crucial for maintaining the Bitcoin network's security and predictable operation. This effort aims to make the network more resilient against specific timestamp-related vulnerabilities.

**Technical Details:** The discussion focuses on BIP-54's proposed rules to mitigate timewarp vulnerabilities by restricting block header timestamps, particularly for retarget periods. Pieter Wuille recently provided a 'Lean proof' demonstrating the safety of these rules under specific conditions related to block count, timestamp bounds, and work limits. This formal analysis contributes to the architectural debate on the robustness of the proposed fixes, with Zawy's recent retraction indicating a potential shift in understanding or consensus regarding the technical implications.

### [Re: Depots: Theft-Proof, Self-Custodial Bitcoin For Billions Of Users](https://delvingbitcoin.org/t/depots-theft-proof-self-custodial-bitcoin-for-billions-of-users/2892/11)
**Source:** Delving | **Started By:** {'username': 'JohnLaw', 'uuid': 'auto_johnlaw'} | **Messages:** 10
> Bitcoin developers are exploring advanced scaling solutions to enable billions of users to transact with extremely low on-chain costs and effortless, automated fund management.

**Technical Details:** The thread discusses scaling Bitcoin for billions of users, emphasizing a minimal on-chain footprint (1-2 vbytes/user/year). A key architectural challenge is enabling automated fund rollover between 'depots' without requiring user interaction. JohnLaw introduced a 'timeout trees' protocol as a potential solution to allow operators to perform these rollovers non-interactively, addressing the need for seamless, low-cost scaling.

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/36)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 10
> Bitcoin developers are exploring ways to integrate quantum-resistant cryptography, ensuring your funds remain secure against future quantum computer threats. The focus is on practical designs that work well with how people use Bitcoin today.

**Technical Details:** The ongoing discussion on Post-Quantum Cryptography (PQC) transaction output types in Bitcoin is grappling with the practical implications of user behavior. A key architectural debate centers on designing PQC schemes that remain secure despite common address reuse, a behavior difficult to change. Participants are evaluating the utility of PQC spending paths under specific threat models, like the 'orange threat model,' to determine their actual security benefits and necessary design considerations, given the challenge of enforcing ideal user practices.

### [Re: Standardizing an exposure classification for existing outputs (pre-BIP)](https://delvingbitcoin.org/t/standardizing-an-exposure-classification-for-existing-outputs-pre-bip/2866/11)
**Source:** Delving | **Started By:** {'username': 'Duncan0k', 'uuid': 'auto_duncan0k_1'} | **Messages:** 7
> To enable future Bitcoin upgrades that enhance privacy and security, a new informational standard is being developed. This standard will classify how public a key is, paving the way for advanced output types and improved signature schemes.

**Technical Details:** BIP 360 and BIP 361, which propose advanced output types with off-chain keys and a phased sunset for legacy signatures, respectively, both require a foundational specification for classifying output public key exposure. Murch confirmed the value of an informational BIP for this purpose, and Duncan0k has now submitted a draft, 'Output Public Key Exposure Classification' (PR #2294). This informational BIP aims to provide the architectural prerequisite for future proposals relying on nuanced key exposure levels, and now requires community review and refinement.

### [Re: Block-wide Signature Aggregation via SNARKs](https://delvingbitcoin.org/t/block-wide-signature-aggregation-via-snarks/2875/10)
**Source:** Delving | **Started By:** {'username': 'conduition', 'uuid': 'auto_conduition'} | **Messages:** 4
> Developers are discussing how to integrate quantum-resistant signatures into Bitcoin efficiently. The focus is on minimizing the impact of larger signature sizes on network performance and storage, ensuring Bitcoin remains secure and accessible in the quantum era.

**Technical Details:** The central technical challenge revolves around mitigating the large size of post-quantum (PQ) signatures to prevent adverse effects on block propagation and archival node resource requirements. A significant architectural debate has arisen concerning whether the PQ signature *prover* should be integrated directly into Bitcoin Core, alongside the verifier, to benefit from Core's rigorous review process. Initial performance benchmarks for provers are being considered, with preliminary data from the Flock paper indicating CPU-based proving capabilities. Further work is required to establish concrete CPU/GPU requirements and finalize the integration strategy for prover logic.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@KY-U](https://github.com/KY-U), [@arejula27](https://github.com/arejula27), [@jakubtrnka](https://github.com/jakubtrnka), [@lucasdbr05](https://github.com/lucasdbr05)

### ✍️ Top Authors
The most active PR authors this week: [@fjahr](https://github.com/fjahr), [@fanquake](https://github.com/fanquake), [@hebasto](https://github.com/hebasto), [@achow101](https://github.com/achow101), [@sedited](https://github.com/sedited)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@maflcko](https://github.com/maflcko), [@fanquake](https://github.com/fanquake), [@willcl-ark](https://github.com/willcl-ark), [@hebasto](https://github.com/hebasto)
