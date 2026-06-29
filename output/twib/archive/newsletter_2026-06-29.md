# 📰 This Week in Bitcoin (2026-06-22 to 2026-06-28)

## 📌 The TL;DR
- The complete removal of Libevent marks a significant architectural cleanup, simplifying the networking stack, reducing external dependencies, and improving long-term maintainability and security.
- Active discussion around the "Segregated Data" BIP draft highlights ongoing efforts to explore new block regions for prunable, script-isolated data carriage, addressing efficient block space utilization and potential future use cases for on-chain data.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core.

### 📡 RPC, APIs & ZMQ
#### [#35182: Replace libevent with our own HTTP and socket-handling implementation](https://github.com/bitcoin/bitcoin/pull/35182)
**Author:** [@pinheadmz](https://github.com/pinheadmz)

### 👛 Wallet & Keys
#### [#35266: rpc, wallet: add an option to not load the wallet after migrating](https://github.com/bitcoin/bitcoin/pull/35266)
**Author:** [@polespinasa](https://github.com/polespinasa)
> Users now have more control over when their wallet is loaded after an upgrade, allowing for smoother and more flexible system management. This helps prevent unwanted automatic wallet loading, especially in automated setups.

**Technical Details:** This PR introduces a new `load_on_startup` option for the RPC `migratetodescriptor` command and general wallet handling. Previously, wallets would automatically load post-migration, which could be problematic for specific configurations or automated scripts. The change allows users to explicitly disable immediate loading, improving operational flexibility and system resource management. This refines the wallet lifecycle management within the RPC and wallet subsystems by providing explicit control.

#### [#35601: wallet: remove experimental warning from send and sendall](https://github.com/bitcoin/bitcoin/pull/35601)
**Author:** [@Sjors](https://github.com/Sjors)
> The `send` and `sendall` wallet RPC commands are now considered stable and reliable, removing the "experimental" warning. This means users can confidently use these features without concerns about their maturity.

**Technical Details:** This PR removes the `RPC_METHOD_EXPERIMENTAL` flag from the `send` and `sendall` RPC methods within the wallet component. These methods have undergone sufficient testing and usage, demonstrating their stability and robustness in production environments. Removing the experimental tag signals their readiness for general use, improving the user experience by eliminating unnecessary warnings for mature functionality and streamlining RPC command documentation.

### 📝 Documentation
#### [#35424: doc, wallet: align external signer documentation, reject sendtoaddress/sendmany](https://github.com/bitcoin/bitcoin/pull/35424)
**Author:** [@w0xlt](https://github.com/w0xlt)
> Documentation for external signers has been improved and specific wallet commands are now correctly prevented from being used with them. This enhances security and clarifies proper usage for hardware wallets and other external signing devices.

**Technical Details:** This PR brings two key changes: first, it updates and aligns the documentation for external signers (e.g., hardware wallets) to provide clearer guidance on their setup and usage. Second, it implements a check within the wallet to explicitly reject `sendtoaddress` and `sendmany` RPC calls when an external signer is active. This prevents potentially insecure or unsupported operations with external signing devices, reinforcing the security model by requiring users to use appropriate `descriptors` or `walletprocesspsbt` workflows for external signer interaction.

#### [#35602: doc: Clarify build docs about `pkgconf` / `pkg-config` requirements](https://github.com/bitcoin/bitcoin/pull/35602)
**Author:** [@hebasto](https://github.com/hebasto)
> The build instructions have been made clearer regarding a technical tool called `pkg-config`, making it easier for new users and developers to set up and compile Bitcoin Core.

**Technical Details:** This PR updates the build documentation to provide explicit clarification on the requirements and usage of `pkgconf` or `pkg-config`. These tools are essential for locating and linking against external libraries during the compilation process. The documentation enhancement specifies which packages or development headers are necessary for `pkg-config` to function correctly across different Linux distributions, reducing common build environment setup issues for developers and contributors, thereby lowering the barrier to entry.

### 🛠️ Build, CI & Testing
#### [#35543: test: introduce ExtendedPrivateKey and ExtendedPublicKey classes](https://github.com/bitcoin/bitcoin/pull/35543)
**Author:** [@rkrux](https://github.com/rkrux)
> New internal tools have been created for testing advanced wallet features like hierarchical deterministic (HD) keys, which will help ensure the correctness and security of these complex cryptographic functions.

**Technical Details:** This PR introduces new `ExtendedPrivateKey` and `ExtendedPublicKey` classes specifically for use within the test framework. These classes encapsulate the logic for handling Hierarchical Deterministic (HD) keys, including parent fingerprinting, derivation paths, and serialization. Their introduction allows for more robust and isolated testing of HD-related functionality without relying directly on the full wallet implementation, improving test coverage and maintainability for cryptographic primitives by separating concerns within the testing suite.

#### [#35506: test: ensure group data cluster pointers are live](https://github.com/bitcoin/bitcoin/pull/35506)
**Author:** [@instagibbs](https://github.com/instagibbs)
> An internal test has been enhanced to confirm that certain data structures crucial for transaction memory pool management are correctly handled, improving the reliability of how transactions are processed.

**Technical Details:** This PR adds a test to ensure that 'group data cluster pointers' within the mempool are correctly managed and remain 'live' (i.e., valid and accessible). This is critical for the `mempool`'s internal data structures, particularly when grouping related transactions or managing dependencies to prevent transaction rule violations. The test validates the integrity of these pointers, preventing potential crashes or incorrect transaction evaluation by confirming that memory is correctly referenced throughout the mempool's lifecycle, enhancing robustness.

#### [#35576: test: raise `feature_reindex` RPC timeout](https://github.com/bitcoin/bitcoin/pull/35576)
**Author:** [@l0rinc](https://github.com/l0rinc)
> The timeout for a specific internal test involving re-indexing the blockchain has been increased. This accounts for variations in system performance and prevents false failures during testing.

**Technical Details:** This PR increases the RPC timeout specifically for the `feature_reindex` test case. The re-indexing process, which rebuilds the blockchain state from scratch, can be computationally intensive and vary significantly in duration depending on hardware and system load. Extending the timeout prevents premature test failures due to transient performance bottlenecks, ensuring the test accurately reflects the re-index functionality's correctness rather than system-specific timing issues, improving CI reliability.

#### [#35595: ci: remove some packages from Chimera job](https://github.com/bitcoin/bitcoin/pull/35595)
**Author:** [@fanquake](https://github.com/fanquake)
> The automated testing system has been optimized by removing unnecessary software packages from a specific test environment, making the tests run faster and more efficiently.

**Technical Details:** This PR modifies the Continuous Integration (CI) configuration for the 'Chimera' job, specifically by removing several extraneous packages from its build and test environment. This cleanup reduces the container image size, accelerates setup times for the CI job, and minimizes the attack surface by reducing unnecessary dependencies. It streamlines the CI pipeline, making the development workflow more efficient and cost-effective by focusing resources on essential components.

#### [#35571: ci: use warp docker buildkit cache](https://github.com/bitcoin/bitcoin/pull/35571)
**Author:** [@willcl-ark](https://github.com/willcl-ark)
> Our automated build system now uses an improved caching mechanism for Docker, making it much faster to build and test Bitcoin Core, which speeds up development.

**Technical Details:** This PR integrates 'warp docker buildkit cache' into the Continuous Integration (CI) pipeline. Buildkit is a next-generation builder toolkit for Docker, offering advanced caching capabilities. By leveraging warp cache with Buildkit, the CI system can dramatically speed up image builds by reusing layers from previous builds more effectively, especially across different branches or during iterative development. This significantly reduces CI run times, improves developer productivity, and optimizes resource utilization for continuous integration.

#### [#35220: fuzz: connman: strengthen assertions and extend coverage](https://github.com/bitcoin/bitcoin/pull/35220)
**Author:** [@brunoerg](https://github.com/brunoerg)
> Bitcoin Core's internal testing tools have been improved to better detect potential issues in the network connection management. This makes the software more robust and resistant to unexpected behavior.

**Technical Details:** This PR enhances the fuzzer coverage for the `CConnman` (Connection Manager) component by adding stronger assertions and extending test cases. It involves introducing more rigorous checks within the fuzzed environment to validate the internal state and behavior of `CConnman` under various, often malformed, inputs. This strengthens the robustness of the P2P networking layer by identifying edge cases and potential vulnerabilities earlier in the development cycle, contributing to overall system stability.

#### [#35536: fuzz: share a single mocked steady clock across FuzzedSock instances](https://github.com/bitcoin/bitcoin/pull/35536)
**Author:** [@HowHsu](https://github.com/HowHsu)
> Improvements to the internal testing tools now ensure a consistent "time" for all parts of a simulated network environment. This leads to more accurate and reliable testing of network interactions.

**Technical Details:** This PR refactors the fuzzer framework to use a single, shared mocked `steady_clock` instance across all `FuzzedSock` objects. Previously, each `FuzzedSock` might have had its own independent mocked clock, leading to inconsistencies and non-deterministic behavior in fuzzer runs involving multiple simulated network connections. Centralizing the mocked clock ensures a synchronized time source for all network components under test, improving the determinism and reliability of fuzzing network-related code by eliminating temporal discrepancies.

#### [#35452: [30.x] 30.3rc1](https://github.com/bitcoin/bitcoin/pull/35452)
**Author:** [@fanquake](https://github.com/fanquake)
> The first release candidate for Bitcoin Core version 30.3 is now available, marking a significant step towards a new stable release with various improvements and bug fixes.

**Technical Details:** This PR merges the tag for `30.3rc1`, signifying the creation of the first release candidate for the 30.x branch. This release candidate incorporates a collection of bug fixes, minor enhancements, and backported changes deemed stable enough for broader testing before a final stable release. The purpose is to gather community feedback and identify any regressions or critical issues in a pre-release environment, ensuring a robust and reliable final 30.3 release with high quality assurance.

#### [#35594: fuzz: cover async chainstate compaction](https://github.com/bitcoin/bitcoin/pull/35594)
**Author:** [@l0rinc](https://github.com/l0rinc)
> The internal testing tools now cover the background processes that optimize blockchain storage, making sure this critical maintenance work runs smoothly and reliably.

**Technical Details:** This PR extends the fuzzer coverage to include the asynchronous chainstate compaction logic. The chainstate compaction process, which optimizes the on-disk storage of blockchain data, involves complex concurrent operations. By fuzzing this area, developers can test its robustness against various timing issues, memory allocations, and unexpected states, ensuring that background chainstate management remains stable and free of data corruption or race conditions, thereby improving data integrity and node reliability.

#### [#35450: [29.x] 29.4rc1](https://github.com/bitcoin/bitcoin/pull/35450)
**Author:** [@fanquake](https://github.com/fanquake)
> The first release candidate for Bitcoin Core version 29.4 is now available, offering an opportunity to test an updated stable release with important fixes and improvements.

**Technical Details:** This PR merges the tag for `29.4rc1`, indicating the creation of the first release candidate for the 29.x branch. Similar to other release candidates, this version bundles critical bug fixes, security updates, and other improvements backported from the master branch that are suitable for an incremental stable release. This step invites broader community testing to validate the changes and ensure the stability and reliability of the upcoming 29.4 stable release, maintaining the established release cadence.

### 🔄 Misc / Other
#### [#35465: coins: compact chainstate regularly](https://github.com/bitcoin/bitcoin/pull/35465)
**Author:** [@l0rinc](https://github.com/l0rinc)

#### [#35331: [31.x] Backports](https://github.com/bitcoin/bitcoin/pull/35331)
**Author:** [@fanquake](https://github.com/fanquake)

#### [#35521: fuzz: Speed up `dbwrapper_concurrent_reads` harness](https://github.com/bitcoin/bitcoin/pull/35521)
**Author:** [@marcofleon](https://github.com/marcofleon)

#### [#35403: mining: pr 33966 followups (disentangle miner startup defaults)](https://github.com/bitcoin/bitcoin/pull/35403)
**Author:** [@Sjors](https://github.com/Sjors)
> This update refines how the built-in mining feature starts up, making it clearer and more predictable for users who choose to run a solo miner.

**Technical Details:** This PR implements follow-up changes to consolidate and clarify the startup defaults for the internal CPU miner, building upon previous work (PR #33966). It aims to disentangle default mining parameters and startup logic, making the behavior more explicit and less prone to configuration errors. This refactoring improves the maintainability of the mining component and provides clearer control for users who interact with the RPC `setgenerate` command or other mining-related configurations, ensuring predictable behavior.

#### [#35559: scripted-diff: Rename SteadyClockContext to FakeSteadyClock](https://github.com/bitcoin/bitcoin/pull/35559)
**Author:** [@HowHsu](https://github.com/HowHsu)

### ⚡ P2P & Network
#### [#34411: Full Libevent removal](https://github.com/bitcoin/bitcoin/pull/34411)
**Author:** [@fanquake](https://github.com/fanquake)
> Developers are working to remove an old dependency called Libevent, making Bitcoin Core simpler, more modern, and easier to maintain. This will improve the overall code quality and future development speed.

**Technical Details:** This ongoing PR aims to fully decouple Bitcoin Core from the Libevent library, which has historically been used for asynchronous networking. The effort involves refactoring remaining event-driven code paths, particularly in areas like RPC and P2P networking, to use C++ standard library features or custom event loop implementations. This architectural cleanup reduces external dependencies, improves build times, and facilitates easier integration of modern C++ paradigms, enhancing maintainability and reducing the attack surface.

#### [#35588: scripted-diff: Rename `Sock::{RECV,SEND,ERR}`](https://github.com/bitcoin/bitcoin/pull/35588)
**Author:** [@hebasto](https://github.com/hebasto)
> This change is part of an ongoing effort to improve the internal code readability and consistency, making it easier for developers to understand and work with the networking code. It's a foundational improvement for future development.

**Technical Details:** This PR proposes a scripted-diff to rename `Sock` member variables `RECV`, `SEND`, and `ERR` to more descriptive names (e.g., `m_recv_buffer`, `m_send_buffer`, `m_error_status`). This refactoring is a mechanical cleanup, ensuring consistent naming conventions across the codebase for internal socket state management. It improves code clarity and maintainability within the network I/O abstraction layer by adhering to modern C++ style guides.

#### [#35550: net_processing: fix BIP152 first integer interpretation](https://github.com/bitcoin/bitcoin/pull/35550)
**Author:** [@brunoerg](https://github.com/brunoerg)
> A minor correction has been made to how Bitcoin Core interprets certain data for faster block relay (BIP152). This ensures more efficient and correct communication between nodes, improving network performance.

**Technical Details:** This PR fixes an incorrect interpretation of the first integer in BIP152 compact block messages within the `net_processing` component. The `cmpctblock` message structure includes an integer indicating the short transaction IDs, and a misinterpretation could lead to inefficient or incorrect compact block processing. The fix ensures that the integer is correctly parsed and utilized, aligning with the BIP152 specification and improving the efficiency and reliability of compact block relay for faster block propagation and reduced bandwidth usage.

### 🛡️ Consensus & Cryptography
#### [#35070: validation: prevent FindMostWorkChain from causing UB](https://github.com/bitcoin/bitcoin/pull/35070)
**Author:** [@stratospher](https://github.com/stratospher)
> A subtle bug that could lead to unpredictable behavior in rare circumstances during blockchain synchronization has been fixed, making the network's understanding of the strongest chain more reliable.

**Technical Details:** This PR addresses a potential Undefined Behavior (UB) in the `FindMostWorkChain` function within the validation component. Specifically, it prevents issues that could arise from dereferencing null pointers or accessing invalid memory locations when evaluating the chain with the most accumulated proof-of-work, especially during blockchain reorganization events or initial sync. The fix involves adding explicit checks or restructuring the logic to guarantee valid memory access, enhancing the stability and correctness of chain selection critical for network consensus.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

### 🔄 Misc / Other
#### [#35588: scripted-diff: Rename `Sock::{RECV,SEND,ERR}`](https://github.com/bitcoin/bitcoin/pull/35588)
**Author:** [@hebasto](https://github.com/hebasto)
*(Activity: 17 review events this week)*
> This change is part of an ongoing effort to improve the internal code readability and consistency, making it easier for developers to understand and work with the networking code. It's a foundational improvement for future development.

#### [#35587: Remove boost as a unit test runner](https://github.com/bitcoin/bitcoin/pull/35587)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob)
*(Activity: 17 review events this week)*
> Developers are working to simplify the testing framework by removing Boost, an external library. This will make our tests easier to manage and update in the future.

#### [#35569: Encapsulation for CTransaction](https://github.com/bitcoin/bitcoin/pull/35569)
**Author:** [@purpleKarrot](https://github.com/purpleKarrot)
*(Activity: 16 review events this week)*

#### [#34411: Full Libevent removal](https://github.com/bitcoin/bitcoin/pull/34411)
**Author:** [@fanquake](https://github.com/fanquake)
*(Activity: 16 review events this week)*
> Developers are working to remove an old dependency called Libevent, making Bitcoin Core simpler, more modern, and easier to maintain. This will improve the overall code quality and future development speed.

### 👛 Wallet & Keys
#### [#35436: wallet: Add addHDkey interface](https://github.com/bitcoin/bitcoin/pull/35436)
**Author:** [@pseudoramdom](https://github.com/pseudoramdom)
*(Activity: 14 review events this week)*
> A new feature is being developed to allow easier integration and management of hierarchical deterministic (HD) keys within the wallet. This will enable more flexible and powerful ways to handle Bitcoin addresses.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [BIP Draft] Segregated Data: a prunable, script-isolated block region for data carriage](https://delvingbitcoin.org/t/bip-draft-segregated-data-a-prunable-script-isolated-block-region-for-data-carriage/2641/2)
**Source:** Delving | **Started By:** Antoine Poinsot | **Messages:** 24
> Developers are discussing a new proposal to add a dedicated space in Bitcoin blocks for storing certain types of data, which could be pruned later. This could allow for more flexible uses of the blockchain while keeping the network efficient.

**Technical Details:** This discussion thread concerns a Bitcoin Improvement Proposal (BIP) draft for "Segregated Data," aiming to introduce a new, distinct region within Bitcoin blocks. This region would be specifically designated for data carriage, characterized by being prunable (allowing nodes to discard the data after a certain period to save disk space) and script-isolated (meaning the data cannot be directly interpreted as script or affect transaction validity). The architectural goal is to enable new applications requiring on-chain data storage without burdening full nodes with permanent, non-essential data, while carefully considering implications for consensus rules and network resource usage and potential soft fork activation.

### [Re: State of the transaction privacy work in Bitcoin](https://delvingbitcoin.org/t/state-of-the-transaction-privacy-work-in-bitcoin/2622/10)
**Source:** Delving | **Started By:** Adam Gibson | **Messages:** 16
> Developers are actively discussing the current status and future directions of work to improve transaction privacy on the Bitcoin network. This continuous effort aims to make Bitcoin transactions more confidential for users.

**Technical Details:** This discussion thread provides an overview and ongoing conversation regarding the current state of transaction privacy efforts within Bitcoin. It covers various research and development initiatives, including topics like CoinJoin implementations, PayJoin, Schnorr/Taproot improvements, potential future technologies like Confidential Transactions (CTs) or Zero-Knowledge Proofs (ZKPs) on sidechains, and challenges related to practical deployment and adoption. The thread serves as a forum for developers to share updates, assess progress, and strategize on enhancing user privacy for on-chain Bitcoin transactions, considering trade-offs with scalability and decentralization.

## 🏆 Contributor Shoutouts
### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@l0rinc](https://github.com/l0rinc), [@HowHsu](https://github.com/HowHsu), [@Sjors](https://github.com/Sjors), [@brunoerg](https://github.com/brunoerg)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@maflcko](https://github.com/maflcko), [@l0rinc](https://github.com/l0rinc), [@hebasto](https://github.com/hebasto), [@Sjors](https://github.com/Sjors)
