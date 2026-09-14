# 📰 This Week in Bitcoin (2026-09-07 to 2026-09-13)

## 📌 The TL;DR
- Ongoing fundamental research into future protocol evolution, with discussions centered on integrating Post-Quantum Cryptography (PQC) output types and exploring Block-wide Signature Aggregation via SNARKs to address long-term security and scalability challenges.
- A significant refinement in how advanced script expressions are handled within wallets, marked by the decision to drop the concept of a validatable Descriptor ID for Miniscript, alongside active technical discussions on improving multi-party signing workflows, specifically for PSBT/MuSig2 coordination using decentralized communication relays like Nostr.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core, ordered by community review activity.

#### [#1923: group: VERIFY input/output ge/gej/fe exhaustively](https://github.com/bitcoin/bitcoin/pull/1923)
**Author:** [@real-or-random](https://github.com/real-or-random) | **[Wallet & User Tools]** *(Activity: 33 review events)*
> This enhancement improves the Bitcoin-Qt user interface by adding "statustips" to various elements, providing more descriptive information in the status bar alongside traditional tooltips. It makes the GUI more informative and user-friendly by offering additional context for actions and controls.

**Technical Details:** The change involves integrating `QStatusTipEvent` handling or similar mechanisms within the Bitcoin-Qt UI components. For various widgets (e.g., buttons, text fields), a `statustip` property is set, which then populates the application's status bar with a contextual message when the user hovers over or focuses on that element. This complements the existing tooltip system, offering a more prominent and persistent display of helpful information to the user.

#### [#35445: wallet, descriptor: Revert `StringType::COMPAT` for Miniscript expressions and drop the concept of a Descriptor ID that can be validated](https://github.com/bitcoin/bitcoin/pull/35445)
**Author:** [@achow101](https://github.com/achow101) | **[Maintenance & Tech Debt]** *(Activity: 33 review events)*
> This PR simplifies wallet descriptor handling by removing an unused compatibility string type and an unnecessary descriptor identification concept, making descriptor management more robust and easier to maintain.

**Technical Details:** This PR reverts the use of `StringType::COMPAT` for Miniscript expressions within wallet descriptors, as it proved unnecessary and complicated parsing. Concurrently, it removes the concept of a "Descriptor ID that can be validated," simplifying descriptor identification by relying solely on the descriptor string itself. This change streamlines descriptor parsing, reduces potential inconsistencies, and removes a layer of complexity from wallet descriptor management.

#### [#34931: validation: abort on DB unreadable coins instead of treating them as missing](https://github.com/bitcoin/bitcoin/pull/34931)
**Author:** [@furszy](https://github.com/furszy) | **[Security & Consensus]** *(Activity: 24 review events)*
> This PR changes how the validation logic handles unreadable coins found in the database; instead of silently treating them as missing, the node will now abort. This ensures critical database corruption is immediately detected, preventing potentially dangerous continued operation on an inconsistent state.

**Technical Details:** The `CCoinsView` interface and its implementations (e.g., `CCoinsViewDB`) are modified. When an entry in the UTXO database (chainstate) is encountered that cannot be correctly deserialized or read, the validation process will no longer simply assume the coin is missing. Instead, it will trigger an immediate assertion failure or controlled shutdown. This forces an operator to address the underlying database corruption, preventing the node from building on an invalid chainstate and potentially leading to consensus issues or further data corruption.

#### [#36174: http: throttle send buffer when client stops draining](https://github.com/bitcoin/bitcoin/pull/36174)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 22 review events)*
> This PR improves the HTTP RPC server's resilience by throttling its send buffer when a client stops receiving data. This prevents excessive memory usage and potential denial-of-service issues caused by slow or unresponsive clients.

**Technical Details:** The HTTP RPC server's `send` buffer previously lacked a mechanism to prevent unbounded growth if a connected client ceased draining data from the socket. This change implements a throttling mechanism that monitors the client's read progress. When a client's receive rate significantly drops or stops, the server will pause or slow down writing to the buffer, preventing memory exhaustion on the Bitcoin Core node.

#### [#35303: policy: fix negative CFeeRate::ToString() formatting](https://github.com/bitcoin/bitcoin/pull/35303)
**Author:** [@joaonevess](https://github.com/joaonevess) | **[Wallet & User Tools]** *(Activity: 21 review events)*
> This PR fixes an issue where negative fee rates were formatted incorrectly when converted to a string. This ensures accurate and consistent display of fee rate information, improving user experience and data reliability.

**Technical Details:** The bug was in the `CFeeRate::ToString()` method, which did not correctly handle the sign of a negative fee rate, potentially displaying `-0.000` instead of the correct negative value or a specific error representation. The fix likely involves adjusting the string formatting logic to correctly propagate and display the negative sign when the underlying fee rate value is less than zero, ensuring mathematical correctness in its string representation.

#### [#1931: tests: cover rejection of invalid plain seckey alongside a valid one](https://github.com/bitcoin/bitcoin/pull/1931)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[Maintenance & Tech Debt]** *(Activity: 17 review events)*
> This pull request updates the icons used in Bitcoin Core to a new set that is explicitly licensed under a non-GPL compatible license. This ensures broader compatibility and legal flexibility for the project's visual assets.

**Technical Details:** The change involves replacing specific icon files (e.g., `.png`, `.svg` assets used by Bitcoin-Qt) with functionally equivalent alternatives that carry a different, more permissive license (e.g., MIT, CC0, or similar, rather than GPL). This may also involve updating any accompanying `COPYING` or attribution files to reflect the new licensing terms for these specific assets, ensuring the project's overall license compliance.

#### [#36150: indexes: set prune lock to genesis before first block](https://github.com/bitcoin/bitcoin/pull/36150)
**Author:** [@andrewtoth](https://github.com/andrewtoth) | **[Maintenance & Tech Debt]** *(Activity: 16 review events)*
> This PR adjusts the pruning logic for indexes to ensure it's correctly set to the genesis block state before processing the first block, preventing potential data corruption or incorrect pruning behavior. This ensures the integrity of pruned index data from the very start.

**Technical Details:** This PR addresses a specific initialization condition within the indexing subsystem, particularly when block pruning is enabled. It ensures that the "prune lock" or equivalent state variable, which dictates the earliest block that can be pruned, is correctly initialized to the genesis block's height before the very first block is processed by the node. This prevents premature pruning of essential data that might occur if the prune lock were implicitly set to a later block or an invalid state during initial sync or reindex.

#### [#35796: depends: fix IPC listeners on macOS dying when accepting a dead socket](https://github.com/bitcoin/bitcoin/pull/35796)
**Author:** [@xyzconstant](https://github.com/xyzconstant) | **[Maintenance & Tech Debt]** *(Activity: 16 review events)*
> This PR fixes a bug on macOS where the `depends` system's Inter-Process Communication (IPC) listeners could crash or fail when attempting to accept connections from a dead or invalid socket. This improves the stability and reliability of the build system's IPC mechanisms, particularly for macOS users.

**Technical Details:** The `depends` system's IPC listener implementation on macOS is modified to robustly handle scenarios where `accept()` is called on a socket that has already been closed or is otherwise invalid. This often occurs due to race conditions or external process termination. The fix likely involves adding error checking around the `accept()` call and properly handling `ECONNRESET` or similar errors, preventing the listener from crashing and allowing it to continue operating reliably.

#### [#36196: contrib, kernel: fixed seeds, chainparams, headerssync params, and assumeutxo updates pre-32.0](https://github.com/bitcoin/bitcoin/pull/36196)
**Author:** [@achow101](https://github.com/achow101) | **[Strategic Initiatives]** *(Activity: 14 review events)*
> This PR updates various core parameters, including fixed seeds, chain parameters, headers synchronization settings, and assumeutxo defaults, in preparation for the 32.0 release. These updates ensure the network bootstrap and synchronization mechanisms are optimized and secure for the new version.

**Technical Details:** This PR is a compilation of updates to critical configuration values across different subsystems. It modifies `chainparams` for network-specific constants, updates hardcoded `fixed seeds` for initial P2P peer discovery, adjusts parameters related to `headers synchronization` (e.g., checkpoints or sync logic), and modifies `assumeutxo` parameters (e.g., the default assumed UTXO set state and activation height). These changes are essential for ensuring new nodes can quickly and securely join the network upon release.

#### [#34914: contrib: replace deprecated --deep codesign flag, fix accidental --verify skip on ci](https://github.com/bitcoin/bitcoin/pull/34914)
**Author:** [@Sjors](https://github.com/Sjors) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*
> This PR updates the macOS code signing script for Bitcoin Core, replacing a deprecated flag and ensuring critical verification steps run during continuous integration. This improves the security and reliability of official macOS releases.

**Technical Details:** The deprecated `--deep` flag for `codesign` is replaced with modern, granular signing options in the `contrib` script. Additionally, it corrects a flaw in the CI pipeline that caused the `--verify` step to be skipped under certain conditions. The change guarantees consistent execution of `codesign --verify`, ensuring signed binaries meet Apple's security requirements before release.

#### [#35164: test: cover P2SH sigop counting in test_witness_sigops](https://github.com/bitcoin/bitcoin/pull/35164)
**Author:** [@musaHaruna](https://github.com/musaHaruna) | **[Maintenance & Tech Debt]** *(Activity: 14 review events)*
> This pull request enhances Bitcoin Core's test suite by adding specific coverage for P2SH (Pay-to-Script-Hash) signature operation counting. It ensures that the network's rules for transaction complexity are accurately and thoroughly validated, improving the overall reliability of the software.

**Technical Details:** The `test_witness_sigops` test suite, which validates the counting of signature operations (sigops) for SegWit transactions, lacked explicit test cases for transactions spending P2SH outputs that wrap SegWit scripts (P2SH-P2WPKH or P2SH-P2WSH). This PR introduces new test vectors and assertions to specifically verify that sigop counting for such P2SH-wrapped SegWit scripts is correctly implemented according to consensus rules, accounting for both the legacy P2SH sigop rules and the SegWit discounted sigop rules.

#### [#36076: psbt: preserve sighash type when merging inputs](https://github.com/bitcoin/bitcoin/pull/36076)
**Author:** [@thomasbuilds](https://github.com/thomasbuilds) | **[Wallet & User Tools]** *(Activity: 12 review events)*
> This PR ensures that the `sighash` type associated with transaction inputs is correctly preserved when merging Partially Signed Bitcoin Transactions (PSBTs). This prevents unintended changes to the signing behavior, maintaining the integrity and correctness of how transactions are finalized and broadcast.

**Technical Details:** When merging multiple PSBTs, the previous implementation might inadvertently drop or incorrectly set the `SIGHASH` type for inputs that have already been signed or partially prepared. This PR modifies the PSBT merging logic to explicitly prioritize and retain the `SIGHASH` type provided by the input's `PSBT_IN_SIGHASH` field. This ensures that the final transaction's signing intent is accurately propagated, even across complex merging operations. This is crucial for wallets that rely on specific `sighash` flags for advanced spending conditions.

#### [#36209: guix: cache GUI depends separately](https://github.com/bitcoin/bitcoin/pull/36209)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 12 review events)*
> This PR improves the build process for the GUI by caching its dependencies separately when using Guix, leading to faster and more efficient reproducible builds. This streamlines development and release cycles.

**Technical Details:** This PR modifies the Guix build manifest or script for Bitcoin Core, specifically targeting the graphical user interface (GUI) dependencies. By configuring Guix to cache GUI-specific dependencies (e.g., Qt libraries, protobuf) in a separate store path or build profile, it prevents unnecessary rebuilds or downloads of these components when only non-GUI parts of Bitcoin Core are changed, or when different GUI build configurations are used, significantly optimizing build times.

#### [#36116: iwyu: Fix warnings in `src/rpc` and treat them as errors](https://github.com/bitcoin/bitcoin/pull/36116)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 12 review events)*
> This PR addresses `Include What You Use` (IWYU) warnings within the RPC source code, effectively treating these warnings as build errors. This enforces stricter code hygiene, ensuring RPC components only include necessary headers, which can improve compilation times and reduce dependencies.

**Technical Details:** The `iwyu` tool, which analyzes header usage, identified superfluous includes in the `src/rpc` directory. This PR modifies the code to remove these unnecessary includes. Additionally, the build configuration is updated to interpret any remaining `iwyu` warnings as fatal errors, integrating `iwyu` into the strict error-checking pipeline and maintaining minimal, explicit header dependencies.

#### [#35935: wallet: Avoid unnecessary wtxvariant rewrites](https://github.com/bitcoin/bitcoin/pull/35935)
**Author:** [@achow101](https://github.com/achow101) | **[Performance & Optimization]** *(Activity: 11 review events)*
> This PR optimizes the wallet's internal operations by reducing unnecessary rewrites of `wtxvariant` objects, which improves performance and resource utilization. This change leads to a more efficient and responsive wallet experience.

**Technical Details:** The `wtxvariant` likely represents a flexible container for different transaction types within the wallet. This PR aims to minimize redundant serialization, deserialization, or copying operations of these objects. It might involve implementing a lazy-loading mechanism, caching, or more intelligent state management to prevent modifying `wtxvariant` if no actual data changes, thereby reducing CPU cycles and potential I/O operations.

#### [#36176: wallet: avoid a crash when creating a wallet with -nosettings](https://github.com/bitcoin/bitcoin/pull/36176)
**Author:** [@Rob1Ham](https://github.com/Rob1Ham) | **[Wallet & User Tools]** *(Activity: 11 review events)*
> This update fixes a critical bug that caused Bitcoin Core to crash when users attempted to create a new wallet while starting with the `-nosettings` flag. It improves the reliability and robustness of wallet management, ensuring a smoother user experience.

**Technical Details:** The crash occurred due to an unhandled exception or null pointer dereference in the wallet initialization logic when the `-nosettings` flag was active, preventing the proper loading or instantiation of certain configuration components. The fix likely involves adding a check for the `-nosettings` flag's state, gracefully handling the missing settings components, or ensuring that necessary defaults are provided even without a settings file, thus preventing an invalid state that led to the crash.

#### [#36127: wallet: remove unused code](https://github.com/bitcoin/bitcoin/pull/36127)
**Author:** [@jeanpablojp](https://github.com/jeanpablojp) | **[Maintenance & Tech Debt]** *(Activity: 11 review events)*
> This PR removes unnecessary or unused code from the wallet module, contributing to a cleaner and more efficient codebase. This helps simplify future development and maintenance for wallet-related features.

**Technical Details:** This change targets the `wallet` subdirectory, identifying and deleting code that is no longer invoked or referenced by any active wallet functions or features. This reduces the overall size and complexity of the wallet module, making it easier for developers to navigate and understand the core logic without being distracted by deprecated or orphaned code.

#### [#1918: refactor: split `ge_parse` into explicit variants (compressed, uncompressed, uncompressed+hybrid)](https://github.com/bitcoin/bitcoin/pull/1918)
**Author:** [@theStack](https://github.com/theStack) | **[🔐 Consensus & Cryptography]** *(Activity: 11 review events)*
> This PR optimizes peer-to-peer (P2P) network synchronization by sending a 'mempool' command at the start of each new session. This allows newly connected nodes to quickly discover unconfirmed transactions, leading to faster mempool consistency across the network.

**Technical Details:** This PR alters the initial communication protocol for new P2P connections. Upon successful establishment of a peer connection, the Bitcoin Core node will now proactively send a `mempool` message. This message signals to the remote peer that it should respond with an `inv` (inventory) message containing the transaction IDs of its unconfirmed transactions, thereby facilitating a more rapid and efficient exchange of mempool contents and accelerating network synchronization.

#### [#35949: miner: Enforce Murch-Zawy rule (BIP54)](https://github.com/bitcoin/bitcoin/pull/35949)
**Author:** [@fjahr](https://github.com/fjahr) | **[Security & Consensus]** *(Activity: 9 review events)*
> This PR implements the Murch-Zawy rule (BIP54) in the miner component, which enforces a policy on `nLockTime` for version 2 transactions. This helps miners produce more robust blocks and contributes to the overall stability and security of the Bitcoin network.

**Technical Details:** This PR integrates the Murch-Zawy rule, as described in BIP54, into Bitcoin Core's mining transaction selection logic. Specifically, it applies a limit to the maximum delta between a version 2 transaction's `nLockTime` and the current block height or timestamp. This miner policy aims to prevent 'fee sniping' and ensure transactions with very large `nLockTime` deltas do not remain in the mempool indefinitely, improving mempool health and block building reliability.

#### [#36203: Update secp256k1 subtree to latest master](https://github.com/bitcoin/bitcoin/pull/36203)
**Author:** [@fanquake](https://github.com/fanquake) | **[Performance & Optimization]** *(Activity: 9 review events)*
> This PR updates the integrated `secp256k1` cryptographic library to its latest master version. This update brings in the latest optimizations and general bug fixes from the upstream project, enhancing the efficiency of core cryptographic operations in Bitcoin Core.

**Technical Details:** The `secp256k1` library is included as a Git subtree within the Bitcoin Core repository. This PR performs a `git subtree pull` operation to synchronize the `src/secp256k1` directory with the upstream `secp256k1` master branch. This effectively upgrades the cryptographic primitives used for signature generation and verification. It integrates the most recent, validated version of the library, potentially yielding performance benefits in cryptographic computations.

#### [#36201: Update embedded asmap to 1788801420](https://github.com/bitcoin/bitcoin/pull/36201)
**Author:** [@fjahr](https://github.com/fjahr) | **[Network & Privacy]** *(Activity: 9 review events)*
> This PR updates the embedded Autonomous System map (asmap) to the latest version. Keeping the asmap current helps nodes maintain accurate network topology information, which improves peer selection and network resilience.

**Technical Details:** The asmap is a dataset mapping IP addresses to Autonomous System (AS) numbers, used for connection management and Sybil resistance. This PR replaces the current embedded `asmap.dat` file with a newer version identified by timestamp 1788801420. This ensures new node deployments and nodes that haven't updated their asmap separately have the most recent AS-to-IP mappings.

#### [#36199: net: treat RFC 9637 new IPv6 documentation range as invalid](https://github.com/bitcoin/bitcoin/pull/36199)
**Author:** [@fjahr](https://github.com/fjahr) | **[Network & Privacy]** *(Activity: 9 review events)*
> This PR updates Bitcoin Core's network logic to correctly identify and reject connection attempts to the newly defined IPv6 documentation range (RFC 9637). This prevents nodes from attempting to connect to non-routable or special-purpose addresses, improving network efficiency.

**Technical Details:** RFC 9637 introduces a new reserved IPv6 address range intended solely for documentation purposes. This PR modifies the network address validation logic to classify addresses within this new `2000::/3` range (specifically `2001:DB8::/32`) as invalid for peer-to-peer connections. This prevents nodes from storing or attempting to connect to these globally unroutable addresses, enhancing network hygiene.

#### [#35468: ci, iwyu: Request IPC file generation explicitly](https://github.com/bitcoin/bitcoin/pull/35468)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 9 review events)*
> This PR modifies the Continuous Integration setup to explicitly request the generation of Intermediate Program Communication (IPC) files for `iwyu`. This ensures the `Include What You Use` tool has the necessary data to accurately analyze header dependencies, thereby improving code quality checks within our automated builds.

**Technical Details:** The CI configuration is updated to include a specific flag or command-line argument that instructs the build system (e.g., CMake) to generate compile_commands.json or similar IPC data files. These files are crucial for `iwyu` to correctly map symbol usage to header includes across the codebase. By explicitly requesting their generation, the reliability and accuracy of the `iwyu` analysis in CI are significantly enhanced.

#### [#36096: rpc: avoid quadratic JSON construction when keys are unique](https://github.com/bitcoin/bitcoin/pull/36096)
**Author:** [@l0rinc](https://github.com/l0rinc) | **[Performance & Optimization]** *(Activity: 8 review events)*
> This optimization improves the performance of Bitcoin Core's RPC interface by preventing a quadratic slowdown when constructing JSON objects with many unique keys. It makes RPC calls significantly faster and more efficient, particularly for operations involving large data structures.

**Technical Details:** Previously, when constructing JSON objects via the RPC framework, the addition of unique keys could lead to an O(N^2) performance bottleneck, likely due to repeated linear scans or inefficient data structure lookups as more keys were added. The fix likely involves switching to a hash-map based approach (e.g., `std::map` or `std::unordered_map` with a custom hasher/comparator) for key management during JSON construction, ensuring that key lookups and insertions are amortized O(1) or O(log N), thus reducing the overall complexity to O(N) or O(N log N).

#### [#36215: asmap: Make version match externally computed hashes](https://github.com/bitcoin/bitcoin/pull/36215)
**Author:** [@hodlinator](https://github.com/hodlinator) | **[Network & Privacy]** *(Activity: 8 review events)*
> This PR ensures the internal version of the asmap data structure matches its externally computed hash. This improves the reliability and verifiability of asmap updates, which helps nodes better understand and connect to the Bitcoin network.

**Technical Details:** The asmap data structure, used for Autonomous System (AS) bucketing, has an embedded version field. This PR ensures that this internal version field is consistent with the cryptographic hash of the asmap data itself, typically used for external verification and update mechanisms. By synchronizing the internal version with the data's hash, it prevents inconsistencies during asmap updates and improves confidence in the integrity of the downloaded AS map.

#### [#1893: test: cover schnorrsig_sign_custom in constant-time tests](https://github.com/bitcoin/bitcoin/pull/1893)
**Author:** [@Yudis-bit](https://github.com/Yudis-bit) | **[Wallet & User Tools]** *(Activity: 8 review events)*
> This important update adds a clear warning message to the `encryptwallet` RPC command, reminding users to back up their newly encrypted wallet. This critical safety measure helps prevent accidental loss of funds by emphasizing the importance of securing the encrypted wallet.

**Technical Details:** The implementation involves modifying the `encryptwallet` RPC handler function. After the wallet encryption process is initiated or completed, an explicit warning string is appended to the RPC response or displayed in the client interface. This warning advises the user to create a backup of the newly encrypted `wallet.dat` file, highlighting the potential for irrecoverable fund loss if the wallet is not backed up post-encryption and the passphrase is forgotten or the file becomes corrupted.

#### [#36113: psbt: fix rendering for invalid long sighash type field](https://github.com/bitcoin/bitcoin/pull/36113)
**Author:** [@Sjors](https://github.com/Sjors) | **[Wallet & User Tools]** *(Activity: 8 review events)*
> This patch corrects a display bug within Partially Signed Bitcoin Transactions (PSBTs) that occurred when an invalid or unusually long sighash type was present. It ensures that PSBTs are always rendered correctly, improving clarity and preventing misinterpretation of transaction details.

**Technical Details:** The rendering logic for PSBT `sighash type` fields was not correctly handling values that exceeded expected lengths or were otherwise malformed. This could lead to incorrect or garbled output when displaying PSBT information, potentially in the GUI or RPC responses. The fix likely involves robust parsing and sanitization of the `sighash type` field before rendering, perhaps truncating excessively long values or displaying a generic "invalid" indicator, to prevent formatting errors and ensure legible output.

#### [#35969: [31.x] More Backports](https://github.com/bitcoin/bitcoin/pull/35969)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR aggregates several fixes and minor improvements from the main development branch into the 31.x release branch. These backported changes enhance the stability and functionality of the upcoming 31.x release.

**Technical Details:** This Pull Request is a meta-PR that bundles multiple individual patches previously merged into the `master` branch and applies them to the `31.x` release branch. The specific changes encompassed are diverse, ranging from bug fixes to minor feature enhancements, ensuring the release branch incorporates critical updates from ongoing development. It primarily involves cherry-picking commits.

#### [#36218: build: avoid `pipe2` on Darwin (for now)](https://github.com/bitcoin/bitcoin/pull/36218)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 8 review events)*
> This PR implements a temporary workaround for a build issue on macOS by avoiding the use of the `pipe2` system call. This allows Bitcoin Core to compile correctly on affected macOS systems.

**Technical Details:** On Darwin platforms, the `pipe2` system call, when used by the build system, was causing compilation failures or unexpected behavior. This PR introduces conditional compilation directives to substitute calls to `pipe2` with its non-`pipe2` equivalent for Darwin targets. This resolves immediate build system compatibility issues, allowing successful compilation on macOS.

#### [#36213: Release: 32.0 translations update](https://github.com/bitcoin/bitcoin/pull/36213)
**Author:** [@hebasto](https://github.com/hebasto) | **[Wallet & User Tools]** *(Activity: 7 review events)*
> This PR incorporates the latest translations for Bitcoin Core in anticipation of the 32.0 release, making the software more accessible to a global user base. It ensures the user interface and messages are available in multiple languages.

**Technical Details:** This PR involves updating the `.po` (Portable Object) or `.ts` (Qt Translation Source) files with the most recent translated strings obtained from the translation platform (e.g., Transifex). It typically includes regenerating the compiled translation files (e.g., `.mo` or `.qm`) that are bundled with the software, ensuring that all user-facing text is localized for the upcoming release.

#### [#36195: test: `get_previous_releases.py` use `PREVIOUS_RELEASES_DIR`](https://github.com/bitcoin/bitcoin/pull/36195)
**Author:** [@davidgumberg](https://github.com/davidgumberg) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR updates a test script to correctly reference the directory for previous releases. It ensures the reliability of our automated testing infrastructure for release-related checks.

**Technical Details:** The `get_previous_releases.py` script, used within the test suite, is modified to utilize the `PREVIOUS_RELEASES_DIR` variable instead of an implicitly derived path. This change standardizes the lookup mechanism for release assets within the testing framework. It prevents potential test failures due to incorrect path resolution, ensuring test stability when release directory structures evolve.

#### [#35778: scripted-diff: Use C.UTF-8 locale in all shell scripts](https://github.com/bitcoin/bitcoin/pull/35778)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR standardizes the locale used across all shell scripts to `C.UTF-8`. This ensures consistent behavior and correct handling of characters in script execution environments, preventing locale-dependent issues across different systems or CI runners.

**Technical Details:** A `scripted-diff` operation is used to prepend `export LC_ALL=C.UTF-8` or a similar directive to shell scripts within the repository. This forces the scripts to run under a consistent UTF-8 compatible locale, which influences string sorting, character classification, and command output. By standardizing the locale, potential inconsistencies or errors arising from varying default system locales are mitigated, improving script reliability across diverse environments.

#### [#36202: doc: add rel note about potential CJDNS removal](https://github.com/bitcoin/bitcoin/pull/36202)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This PR adds a release note to inform users about the potential future removal of CJDNS network support. This pre-announcement helps users anticipate upcoming changes and adjust their configurations if they rely on CJDNS.

**Technical Details:** This Pull Request solely adds a release note entry within the documentation, specifically detailing the consideration for deprecating and potentially removing CJDNS support in a future release. It does not implement any code changes but serves as an advisory to the user base about architectural shifts regarding alternative network protocol support.

#### [#1932: silentpayments: drop "empty key arrays must be NULL" requirement](https://github.com/bitcoin/bitcoin/pull/1932)
**Author:** [@theStack](https://github.com/theStack) | **[Maintenance & Tech Debt]** *(Activity: 7 review events)*
> This update refines various thread-related log messages within Bitcoin Core to improve clarity and accuracy. It aids developers and advanced users in diagnosing issues by providing more precise information in the application logs.

**Technical Details:** The fix involves reviewing and adjusting the formatting, content, or verbosity of logging statements specifically pertaining to thread creation, management, and shutdown events. This might include ensuring consistent naming conventions for threads in logs, providing more specific identifiers, correcting misleading messages, or adding details (e.g., thread IDs, function names) to make log analysis more effective for debugging concurrency issues.

#### [#35513: rpc: help metadata fixes](https://github.com/bitcoin/bitcoin/pull/35513)
**Author:** [@RuslanProgrammer](https://github.com/RuslanProgrammer) | **[Wallet & User Tools]** *(Activity: 6 review events)*
> This PR improves the help output for Bitcoin Core's RPC commands, making it easier for users and developers to understand how to use them correctly. It enhances the discoverability and clarity of RPC interfaces.

**Technical Details:** This PR addresses inconsistencies and inaccuracies within the RPC help strings, specifically focusing on metadata associated with RPC commands. It likely involves updating `CRPCTable` entries or the documentation generation logic to reflect correct parameter types, descriptions, and examples, thereby improving the programmatic and human-readable interfaces for RPC clients.

#### [#36228: ci, iwyu: generate embedded ASMap header explicitly](https://github.com/bitcoin/bitcoin/pull/36228)
**Author:** [@kriss39](https://github.com/kriss39) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR refines the build process by explicitly generating the embedded ASMap header, which helps improve the accuracy of include-what-you-use (IWYU) checks and overall build hygiene. This ensures that dependencies are correctly declared, reducing potential build issues and improving code quality.

**Technical Details:** This PR modifies the CI and build configuration, specifically for the ASMap data structure used in address management. By explicitly generating the header, a script or build step now creates a header file containing ASMap data (e.g., a static array or structure) that can then be included. This explicit generation ensures that IWYU tools correctly identify dependencies and reduces implicit build dependencies, leading to a more robust and maintainable build system.

#### [#36181: ci: Upgrade IWYU to 0.27 compatible with Clang 23](https://github.com/bitcoin/bitcoin/pull/36181)
**Author:** [@hebasto](https://github.com/hebasto) | **[Maintenance & Tech Debt]** *(Activity: 6 review events)*
> This PR upgrades the `Include What You Use` (IWYU) tool within our Continuous Integration environment to version 0.27. This ensures compatibility with the newer Clang 23 compiler, maintaining the functionality of our code quality checks and allowing us to use modern compiler features.

**Technical Details:** The CI pipeline configuration is modified to fetch and utilize `IWYU` version 0.27. This upgrade is necessitated by the introduction of Clang 23, as older `IWYU` versions may not correctly parse or analyze code compiled with the newer compiler's features or diagnostics. This ensures that the `iwyu` checks continue to provide accurate feedback on header dependencies as our compiler toolchain evolves, preserving code quality standards.

#### [#36168: PSBT: Make input/output `Merge()` methods return void](https://github.com/bitcoin/bitcoin/pull/36168)
**Author:** [@nebula-21](https://github.com/nebula-21) | **[Wallet & User Tools]** *(Activity: 4 review events)*
> This change refines how Partially Signed Bitcoin Transactions (PSBTs) are merged by adjusting internal methods. It simplifies the code by removing unnecessary return values from `Merge()` operations, making the internal logic cleaner.

**Technical Details:** The `Merge()` methods for PSBT inputs and outputs are modified to return `void`. Previously, these methods might have returned a boolean or an error status, but the caller's logic has been adjusted to handle merge failures or successes differently, perhaps through exceptions or by pre-validating states. This refactoring implies that the success/failure of merging is now determined by other means or that the previous return value was redundant.

#### [#36221: [31.x] More Backports](https://github.com/bitcoin/bitcoin/pull/36221)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR aggregates several fixes and minor improvements from the main development branch into the 31.x release branch. These backported changes enhance the stability and functionality of the upcoming 31.x release.

**Technical Details:** This Pull Request is a meta-PR that bundles multiple individual patches previously merged into the `master` branch and applies them to the `31.x` release branch. The specific changes encompassed are diverse, ranging from bug fixes to minor feature enhancements, ensuring the release branch incorporates critical updates from ongoing development. It primarily involves cherry-picking commits.

#### [#36222: [30.x] More Backports](https://github.com/bitcoin/bitcoin/pull/36222)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR aggregates several fixes and minor improvements from the main development branch into the 30.x release branch. These backported changes enhance the stability and functionality of the upcoming 30.x release.

**Technical Details:** This Pull Request is a meta-PR that bundles multiple individual patches previously merged into the `master` branch and applies them to the `30.x` release branch. The specific changes encompassed are diverse, ranging from bug fixes to minor feature enhancements, ensuring the release branch incorporates critical updates from ongoing development. It primarily involves cherry-picking commits.

#### [#35899: [29.x] More backports](https://github.com/bitcoin/bitcoin/pull/35899)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This PR incorporates a set of additional backported changes into the `29.x` release branch. These backports bring important bug fixes, minor improvements, or security updates from the development branch into the upcoming stable release.

**Technical Details:** This Pull Request involves cherry-picking multiple commits from the `master` branch onto the `29.x` release branch. The specific commits being backported are typically non-breaking bug fixes, performance enhancements, or critical patches. This process ensures the release branch benefits from recent improvements and stability fixes deemed necessary for the targeted release, without diverging significantly from the main development line.

#### [#36136: rpc: remove stale "canonical form" claim from getdescriptorinfo help](https://github.com/bitcoin/bitcoin/pull/36136)
**Author:** [@craigraw](https://github.com/craigraw) | **[Maintenance & Tech Debt]** *(Activity: 4 review events)*
> This minor update cleans up the documentation for the `getdescriptorinfo` RPC command by removing an outdated and inaccurate claim about "canonical form." It ensures that Bitcoin Core's RPC help messages are accurate and up-to-date for developers.

**Technical Details:** The help text for the `getdescriptorinfo` RPC command contained a statement regarding "canonical form" that is no longer relevant or was never fully accurate for its output. The change simply removes this specific sentence or phrase from the internal help string associated with the RPC. This is purely a documentation correction, not a change to the RPC's functionality or output.

#### [#36227: Pre 32.x branching updates](https://github.com/bitcoin/bitcoin/pull/36227)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR prepares the codebase for the upcoming 32.x release branch by applying necessary updates and configurations. It's a critical step in the release management process, ensuring a smooth transition to the new development cycle.

**Technical Details:** This PR likely encompasses a collection of minor adjustments and configuration changes that are standard practice before a new major release branch is created. This could include updating version numbers, setting flags for new features, adjusting build scripts for the new release cycle, or making small cleanups that are specific to the branching point, but without introducing new features itself.

#### [#36223: [29.x] More Backports](https://github.com/bitcoin/bitcoin/pull/36223)
**Author:** [@fanquake](https://github.com/fanquake) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR aggregates several fixes and minor improvements from the main development branch into the 29.x release branch. These backported changes enhance the stability and functionality of the upcoming 29.x release.

**Technical Details:** This Pull Request is a meta-PR that bundles multiple individual patches previously merged into the `master` branch and applies them to the `29.x` release branch. The specific changes encompassed are diverse, ranging from bug fixes to minor feature enhancements, ensuring the release branch incorporates critical updates from ongoing development. It primarily involves cherry-picking commits.

#### [#36226: doc: Move release notes to wiki ahead of branch-off](https://github.com/bitcoin/bitcoin/pull/36226)
**Author:** [@sedited](https://github.com/sedited) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR moves the release notes documentation from the repository to the project wiki, streamlining the documentation process for the upcoming branch-off. This change improves accessibility and maintainability of release information.

**Technical Details:** This PR specifically targets the documentation process for release notes. It involves removing the release notes file(s) from the `doc/` directory within the main repository and updating any pointers or build steps that previously referenced them. The intent is to manage this type of documentation externally on the project's wiki, potentially leveraging a more collaborative and web-based editing environment.

#### [#36211: doc: Fix PR reference in productivity guide](https://github.com/bitcoin/bitcoin/pull/36211)
**Author:** [@LittleYier](https://github.com/LittleYier) | **[Maintenance & Tech Debt]** *(Activity: 3 review events)*
> This PR corrects an incorrect Pull Request reference within the project's productivity guide documentation. It ensures the accuracy of our developer resources, making it easier for contributors to find relevant information.

**Technical Details:** The change specifically targets a documentation file, likely `doc/developer-notes/productivity.md`, where an existing PR number or link was pointing to an incorrect target. The reference is updated to point to the correct Pull Request. This is a purely textual correction within the documentation, requiring no code changes to the Bitcoin Core application itself.

#### [#1928: tests: add coverage for exact-size DER signature serialization](https://github.com/bitcoin/bitcoin/pull/1928)
**Author:** [@brunoerg](https://github.com/brunoerg) | **[🏗️ Build, CI & Testing]** *(Activity: 2 review events)*

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

#### [#36182: fees: return `block_policy` fee rate estimate when `mempool_policy` is not ready](https://github.com/bitcoin/bitcoin/pull/36182)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq) | **[Wallet & User Tools]** *(Activity: 22 review events this week)*
> This PR improves Bitcoin Core's fee estimation service by providing a `block_policy` estimate when the preferred `mempool_policy` estimate is unavailable. This ensures users consistently receive a reliable fee recommendation, even during startup or network congestion.

#### [#36174: http: throttle send buffer when client stops draining](https://github.com/bitcoin/bitcoin/pull/36174)
**Author:** [@pinheadmz](https://github.com/pinheadmz) | **[Security & Consensus]** *(Activity: 18 review events this week)*
> This PR improves the HTTP RPC server's resilience by throttling its send buffer when a client stops receiving data. This prevents excessive memory usage and potential denial-of-service issues caused by slow or unresponsive clients.

#### [#36196: contrib, kernel: fixed seeds, chainparams, headerssync params, and assumeutxo updates pre-32.0](https://github.com/bitcoin/bitcoin/pull/36196)
**Author:** [@achow101](https://github.com/achow101) | **[Strategic Initiatives]** *(Activity: 14 review events this week)*
> This PR updates various core parameters, including fixed seeds, chain parameters, headers synchronization settings, and assumeutxo defaults, in preparation for the 32.0 release. These updates ensure the network bootstrap and synchronization mechanisms are optimized and secure for the new version.

#### [#36209: guix: cache GUI depends separately](https://github.com/bitcoin/bitcoin/pull/36209)
**Author:** [@willcl-ark](https://github.com/willcl-ark) | **[Maintenance & Tech Debt]** *(Activity: 12 review events this week)*
> This PR improves the build process for the GUI by caching its dependencies separately when using Guix, leading to faster and more efficient reproducible builds. This streamlines development and release cycles.

#### [#35975: wallet: Fix `CWalletTx` malleated transaction metadata sync](https://github.com/bitcoin/bitcoin/pull/35975)
**Author:** [@achow101](https://github.com/achow101) | **[Wallet & User Tools]** *(Activity: 11 review events this week)*
> This PR fixes an issue where the wallet might incorrectly handle metadata for transactions that have been malleated, ensuring the wallet accurately reflects the true state of such transactions. This prevents discrepancies and potential user confusion regarding transaction status.

## 🗓️ Dev Meeting
Summary of the core dev IRC meeting on 2026-09-10 with 28 participants.

- QML GUI Working Group progress, including initial merges and future development plans.
- Kernel Working Group update and a call for comments/reviews on PR #34374.
- Status of the upcoming 32.x release branch-off, noting a few remaining items in the milestone.
- Extensive discussion on the 'private broadcast' feature's privacy guarantees, potential leaks, and managing user expectations for the 32.x release.
- Debate on whether to rename 'private broadcast' or clarify its limitations through documentation to address user expectations.

**Action Items:**
- johnny9dev to create a tracking issue 'Upgrading Gui to Qml' with the list of development chunks.
- Pseudoramdom to continue updating QML designs to be more desktop-friendly and consistent.
- Epicleafies to continue addressing QML transaction and activity issues.
- Reviewers are encouraged to provide comments on PR #34374.
- The security team is to facilitate a broader discussion about the threat model for the private broadcast feature.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: PQC output type discussion](https://delvingbitcoin.org/t/pqc-output-type-discussion/2749/26)
**Source:** Delving | **Started By:** {'username': 'Pieter Wuille', 'uuid': 'can_pieter_wuille'} | **Messages:** 9
> Bitcoin developers are actively discussing how to integrate quantum-resistant transaction types to future-proof the network against potential quantum computer threats. This crucial work aims to secure Bitcoin transactions for decades to come by adopting advanced cryptography.

**Technical Details:** The discussion revolves around the architectural approach for introducing Post-Quantum Cryptography (PQC) transaction output types within Bitcoin. Key debate points include whether new PQC constructions should be considered entirely separate output types or deployed as 'segwit subversions' sharing an existing witness version, such as within a potential P2TRv2. While sharing a witness version, such additions would functionally act as distinct output types, highlighting the core decision on how to modularly extend Bitcoin's transaction capabilities.

### [Re: PSBT/MuSig2 coordination over Nostr relays: transport invariants for nonce safety under at-least-once delivery](https://delvingbitcoin.org/t/psbt-musig2-coordination-over-nostr-relays-transport-invariants-for-nonce-safety-under-at-least-once-delivery/2852/8)
**Source:** Delving | **Started By:** {'username': 'Rafael Turon', 'uuid': 'auto_rafael_turon'} | **Messages:** 4
> Developers are focusing on making multi-party Bitcoin custody solutions more robust and secure. The goal is to ensure users can reliably recover their funds without relying solely on simple warnings, especially after device loss.

**Technical Details:** The discussion centers on the inherent coordinator reliance in most multi-party custody schemes (e.g., multisig, MuSig2, vaults) for PSBT exchange. Rafael Turon highlights the inadequacy of a simple "warning" as a security layer, particularly during critical restore operations on new or wiped devices, where such warnings might be absent or ineffective. This implies a need for stronger architectural solutions and multi-layered defenses that ensure robust recovery independent of volatile user interface warnings.

### [Re: Universal opt-in replay protection?](https://delvingbitcoin.org/t/universal-opt-in-replay-protection/2792/14)
**Source:** Delving | **Started By:** {'username': 'Loki', 'uuid': 'can_loki'} | **Messages:** 4
> Developers are exploring optional replay protection for Bitcoin to help users safeguard their funds during potential future 'fork wars' or contentious chain splits. This feature would allow transactions to only apply to the user's intended chain, offering peace of mind and financial security.

**Technical Details:** The discussion revolves around implementing opt-in replay protection, primarily debating the architectural approach. Anthony Towns suggests a new public key encoding for Taproot CHECKSIG, similar to BIP 118 APO, avoiding a new SegWit version. Sjors Provoost finds this 'hacky' and implies preference for a more fundamental mechanism, while also acknowledging the prototype complexity. Further implementation details include a proposed 100-block committed height requirement to ensure reorg safety, mitigate relay issues, and prevent short-reorg bribe risks.

### [Re: Block-wide Signature Aggregation via SNARKs](https://delvingbitcoin.org/t/block-wide-signature-aggregation-via-snarks/2875/6)
**Source:** Delving | **Started By:** {'username': 'conduition', 'uuid': 'auto_conduition'} | **Messages:** 3
> Developers are exploring ways to handle larger next-generation cryptographic signatures to keep Bitcoin fast and affordable to run, ensuring its long-term resilience.

**Technical Details:** Discussions on integrating Post-Quantum (PQ) signatures address their large size impacting block propagation and archival costs. The focus has shifted to designing these implementations to be DoS-resistant, specifically by using simpler, narrowly focused 'prover' mechanisms for transaction sets. A key architectural debate centers on whether to integrate this prover functionality directly into Bitcoin Core—rather than relying on external miner software—to leverage Bitcoin Core's extensive review for enhanced security and vulnerability mitigation. This approach is seen as crucial for preventing DoS vectors.

### [Re: Addressing the Diminishing Block Subsidy](https://delvingbitcoin.org/t/addressing-the-diminishing-block-subsidy/2640/29)
**Source:** Delving | **Started By:** {'username': 'Sho', 'uuid': 'auto_sho'} | **Messages:** 2
> Discussions are underway regarding Bitcoin's long-term security and how to sustain miner incentives as the block reward diminishes, balancing its fundamental monetary policy with innovative approaches. The community is exploring options to ensure the network remains robust and decentralized for centuries to come.

**Technical Details:** The discussion centers on ensuring long-term miner incentives as the block subsidy diminishes. One perspective, articulated by gmaxwell, stresses Bitcoin's immutable monetary policy, advocating against radical changes and highlighting that alternative cryptocurrencies exist for different rule sets. Conversely, a hypothetical non-hard-fork method is proposed by ArmchairCryptologist to extend the subsidy for centuries, potentially leveraging an external 'CRQC'. This architectural debate explores the tension between preserving Bitcoin's foundational monetary rules and implementing innovative solutions to adapt its economic model without altering core protocol mechanics, posing a complex challenge for network sustainability.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@LittleYier](https://github.com/LittleYier), [@Rob1Ham](https://github.com/Rob1Ham), [@Yudis-bit](https://github.com/Yudis-bit), [@craigraw](https://github.com/craigraw), [@kriss39](https://github.com/kriss39)

### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@hebasto](https://github.com/hebasto), [@fjahr](https://github.com/fjahr), [@achow101](https://github.com/achow101), [@sedited](https://github.com/sedited)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@fanquake](https://github.com/fanquake), [@hebasto](https://github.com/hebasto), [@willcl-ark](https://github.com/willcl-ark), [@achow101](https://github.com/achow101)
