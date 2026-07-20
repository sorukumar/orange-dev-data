# 📰 This Week in Bitcoin (2026-07-13 to 2026-07-19)

## 📌 The TL;DR
- Proactive Quantum Security Discussions:** There's an active and growing community focus on preparing Bitcoin for potential quantum computing threats, with discussions spanning post-quantum witness data commitments and strategies for protecting existing hashed address coins from future quantum attacks.
- Long-Term Economic and Security Model Evolution:** Significant research and discussion are underway regarding the Bitcoin network's economic and security model in the distant future, specifically addressing how to ensure miner incentives and prevent deviation once block rewards diminish to zero and transaction fees become the sole remuneration.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core.

### ⚡ P2P & Network
#### [#34538: net: advertise -externalip addresses](https://github.com/bitcoin/bitcoin/pull/34538)
**Author:** [@willcl-ark](https://github.com/willcl-ark)
> Your Bitcoin node can now better announce its public address to the network, making it easier for other nodes to connect to you.

**Technical Details:** This PR enables Bitcoin Core nodes to explicitly advertise their configured `-externalip` addresses using the `ADDRv2` message format. By doing so, nodes can proactively inform peers of their public reachability, rather than relying solely on passive address observation. This enhancement improves peer discovery and network connectivity for nodes behind NAT or with specific public IP configurations, contributing to a more robust and well-connected Bitcoin network topology.

### 🛠️ Build, CI & Testing
#### [#35590: test: wallet: BnB incomplete result on attempt-limit success](https://github.com/bitcoin/bitcoin/pull/35590)
**Author:** [@brunoerg](https://github.com/brunoerg)
> This test ensures the wallet's coin selection algorithm always provides the most optimal results for your transactions.

**Technical Details:** This PR introduces a new test case for the wallet's Branch and Bound (BnB) coin selection algorithm. It specifically addresses a scenario where the algorithm might successfully find an optimal solution within its `attempt_limit` but incorrectly report an 'incomplete result'. The test verifies that such successful outcomes are correctly recognized and not discarded, ensuring the wallet's coin selection logic is robust and accurately reflects its ability to find efficient solutions for transaction construction.

#### [#35427: depends: Build `qt` and `qrencode` packages on OpenBSD](https://github.com/bitcoin/bitcoin/pull/35427)
**Author:** [@hebasto](https://github.com/hebasto)
> It's now easier to build Bitcoin Core, especially its graphical interface, on the OpenBSD operating system.

**Technical Details:** This PR updates the `depends` system to correctly build the `qt` GUI framework and the `qrencode` library on OpenBSD. This involves adjusting build configurations, patches, and dependencies within the `depends` scripts to accommodate OpenBSD's specific environment and toolchain. This enhancement ensures that users and developers on OpenBSD can easily compile and run Bitcoin Core with its full graphical user interface, expanding platform support and accessibility.

#### [#35708: depends: capnp 1.5.0](https://github.com/bitcoin/bitcoin/pull/35708)
**Author:** [@fanquake](https://github.com/fanquake)
> We've updated an internal tool, Cap'n Proto, to its latest version. This keeps Bitcoin Core secure and performant by using the most up-to-date underlying technologies.

**Technical Details:** This PR updates the Cap'n Proto dependency to version 1.5.0 in the `depends` system. This ensures that when Bitcoin Core is built from source, it utilizes the latest stable release of Cap'n Proto, potentially bringing performance improvements, bug fixes, and security enhancements to components that rely on its serialization capabilities. It simplifies future maintenance by staying current with upstream libraries.

#### [#35718: ci: Update lint container dependencies](https://github.com/bitcoin/bitcoin/pull/35718)
**Author:** [@fanquake](https://github.com/fanquake)
> We've updated the tools used to automatically check our code for errors. This makes our development process more efficient and reliable, leading to higher quality software.

**Technical Details:** This PR updates the dependencies within the Continuous Integration (CI) linting container. This involves refreshing the versions of various linters and static analysis tools used to enforce coding standards and detect potential issues during the automated build process. Keeping these dependencies current ensures that the CI environment is robust, accurate, and leverages the latest rule sets and bug fixes from the linting tools, improving code quality and maintainability.

#### [#35705: bench: replace CreateMockableWalletDatabase with MakeInMemoryWalletDatabase](https://github.com/bitcoin/bitcoin/pull/35705)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
> We've improved our internal testing tools, which helps developers write more reliable and efficient code for your wallet.

**Technical Details:** This PR refactors the wallet benchmarking infrastructure by replacing `CreateMockableWalletDatabase` with `MakeInMemoryWalletDatabase`. This change streamlines the creation of isolated, in-memory wallet databases for performance testing, removing unnecessary mocking complexity. The architectural benefit is a cleaner, more direct approach to benchmarking wallet operations, ensuring more accurate and reproducible performance measurements without disk I/O overhead or external dependencies during tests.

#### [#35719: ci: disable Qt build in OpenBSD cross job](https://github.com/bitcoin/bitcoin/pull/35719)
**Author:** [@fanquake](https://github.com/fanquake)
> We've streamlined our automated testing process for certain operating systems. This allows us to focus development resources more effectively on core components.

**Technical Details:** This PR disables the Qt build specifically within the OpenBSD cross-compilation Continuous Integration (CI) job. This adjustment addresses historical difficulties and known limitations in successfully cross-compiling the Qt GUI on OpenBSD, which often leads to build failures or excessive resource consumption. By explicitly disabling it, the CI pipeline becomes more robust and efficient for OpenBSD, allowing the daemon and other non-GUI components to be tested reliably without being blocked by GUI build issues.

#### [#35715: cmake: Fix WITH_EXTERNAL_LIBMULTIPROCESS + BUILD_FUZZ_BINARY](https://github.com/bitcoin/bitcoin/pull/35715)
**Author:** [@ryanofsky](https://github.com/ryanofsky)
> We've fixed an issue that helps developers build and test certain advanced features. This ensures they work correctly and robustly.

**Technical Details:** This PR resolves a build configuration conflict in the CMake system, specifically when `WITH_EXTERNAL_LIBMULTIPROCESS` and `BUILD_FUZZ_BINARY` are simultaneously enabled. The fix ensures that the build system correctly links dependencies for the fuzzer binaries when an external multiprocessing library is used, preventing compilation errors. This improves the reliability of the build process for developers leveraging advanced testing tools with specific external library configurations, facilitating robust fuzz testing.

#### [#35679: fuzz: Remove unused `DeserializeFromFuzzingInput` params overload](https://github.com/bitcoin/bitcoin/pull/35679)
**Author:** [@hebasto](https://github.com/hebasto)
> This is a minor cleanup of the code used for automated security testing, making it more streamlined.

**Technical Details:** This PR removes an unused overloaded version of the `DeserializeFromFuzzingInput` function from the fuzzing test framework. Removing dead code simplifies the codebase, reduces potential for developer confusion, and slightly improves compile times. This maintains the health and efficiency of the fuzzing harnesses, ensuring that testing infrastructure remains focused and easy to manage for future security enhancements.

#### [#35720: ipc: Update libmultiprocess subtree and drop fuzz test workaround](https://github.com/bitcoin/bitcoin/pull/35720)
**Author:** [@maflcko](https://github.com/maflcko)
> An important internal library for inter-process communication has been updated, improving stability and preparing for future enhancements.

**Technical Details:** This PR updates the `libmultiprocess` external subtree to a newer version. This library provides crucial inter-process communication (IPC) capabilities, which are fundamental for enabling future architectural changes to Bitcoin Core, such as running different components in separate processes. The update also allows for the removal of a specific workaround in fuzz tests, indicating resolved issues in the newer library version and further streamlining the testing framework while reducing technical debt.

#### [#35723: fuzz: Drop unnecessary mutexes](https://github.com/bitcoin/bitcoin/pull/35723)
**Author:** [@marcofleon](https://github.com/marcofleon)
> The automated security tests now run more efficiently by removing unnecessary synchronization steps.

**Technical Details:** This change optimizes the fuzzing infrastructure by identifying and removing `std::mutex` locks that were no longer necessary. In many fuzzing contexts, data access might be single-threaded or the data in question is not shared concurrently during a specific fuzzing run, making mutexes redundant. Removing these unnecessary locks improves fuzz test execution speed, reduces overhead, and simplifies the code, allowing the fuzzers to cover more attack surface more quickly.

### 👛 Wallet & Keys
#### [#32763: wallet: Replace CWalletTx::mapValue and vOrderForm with explicit class members](https://github.com/bitcoin/bitcoin/pull/32763)
**Author:** [@achow101](https://github.com/achow101)
> We've made internal wallet data storage more organized. This helps keep your wallet robust and easier for developers to maintain and improve.

**Technical Details:** This PR refactors the `CWalletTx` class by replacing the generic `mapValue` and `vOrderForm` members with explicit, strongly-typed class members. This change improves type safety, reduces the risk of runtime errors from incorrect key lookups, and enhances code readability and maintainability within the wallet subsystem. Architecturally, it moves towards a more explicit data model, making the intent of stored transaction metadata clearer and simplifying future modifications or extensions to wallet transaction handling.

#### [#35690: wallet: Introduce WalletError with machine-readable error code](https://github.com/bitcoin/bitcoin/pull/35690)
**Author:** [@pseudoramdom](https://github.com/pseudoramdom)
> Future updates will provide clearer, more understandable error messages from the wallet, making it easier for users and applications to troubleshoot issues. This improves the experience for anyone building on Bitcoin Core.

**Technical Details:** This 'hot PR' introduces a structured `WalletError` type within the wallet component, intended to replace generic exceptions or string-based error messages. The new error type will encapsulate specific, machine-readable error codes and potentially more detailed context, enabling programmatic handling of wallet failures. This standardizes wallet error reporting, facilitates better API design, and allows external applications to robustly interpret and react to wallet-related issues without parsing human-readable strings.

#### [#35579: wallet: reserve walletrescan before checking wallet is at the tip](https://github.com/bitcoin/bitcoin/pull/35579)
**Author:** [@polespinasa](https://github.com/polespinasa)
> We've made a small but important improvement to how your wallet syncs. This prevents potential issues and ensures it updates correctly with all your transactions.

**Technical Details:** This PR modifies the wallet rescan logic by reserving the `walletrescan` before checking if the wallet is already synchronized to the chain tip. Previously, a race condition could occur where the wallet might appear to be at the tip, only for a rescan to start later, potentially missing transactions. This change ensures that the rescan flag is atomically set, preventing missed rescans and guaranteeing consistency when a wallet needs to catch up, improving reliability of transaction discovery.

#### [#35639: external_signer: validate fingerprint from enumerate response](https://github.com/bitcoin/bitcoin/pull/35639)
**Author:** [@kwsantiago](https://github.com/kwsantiago)
> We've added an extra security check when connecting to external hardware wallets. This makes it safer to use them with Bitcoin Core by validating the device's identity.

**Technical Details:** This PR implements validation of the hardware wallet's fingerprint obtained from the `enumerate` response during external signer interaction. This security enhancement ensures that the connected external device presents a consistent and expected fingerprint, preventing potential spoofing or connection to an unintended device. It hardens the integration with external signers by adding an additional layer of verification, reducing the risk of signing transactions with a compromised or incorrect hardware wallet.

#### [#35633: wallet: avoid call bumpfeediscount with negative values](https://github.com/bitcoin/bitcoin/pull/35633)
**Author:** [@polespinasa](https://github.com/polespinasa)
> We've refined how the wallet handles transaction fees. This ensures it only applies valid adjustments and prevents unexpected errors or incorrect fee calculations.

**Technical Details:** This PR introduces a check to prevent the `bumpfeediscount` function from being called with negative values. The `bumpfeediscount` mechanism is intended to adjust transaction fees upwards or downwards for specific scenarios. Allowing negative inputs could lead to undefined behavior or incorrect fee calculations. This fix improves the robustness and correctness of the wallet's fee management logic, ensuring that fee adjustments operate within their intended parameters and avoiding potential underpayments or unexpected behavior.

### 📝 Documentation
#### [#35659: Clarify supported *BSD releases and drop outdated workarounds](https://github.com/bitcoin/bitcoin/pull/35659)
**Author:** [@hebasto](https://github.com/hebasto)
> We've updated our system requirements and removed outdated code. This ensures better compatibility and a smoother experience for users on *BSD operating systems.

**Technical Details:** This PR clarifies the officially supported *BSD releases within the project documentation and build system. Concurrently, it removes outdated workarounds and conditional compilation flags that were necessary for older, no longer supported *BSD versions. This streamlines the codebase, reduces maintenance burden, and improves the overall build experience for current *BSD users by focusing resources on actively maintained platforms and leveraging their modern features.

#### [#35698: doc: Update enum class constant naming style guide](https://github.com/bitcoin/bitcoin/pull/35698)
**Author:** [@maflcko](https://github.com/maflcko)
> We're continually refining our coding style guides to make Bitcoin Core's codebase clearer and easier to understand for everyone. This helps new developers contribute more efficiently and speeds up development.

**Technical Details:** PR #35698 merges an update to the `enum class` constant naming style guide. This change standardizes `enum class` constant identifiers to `UPPER_SNAKE_CASE`, promoting consistency and readability across the codebase. While not a functional change, it significantly enhances maintainability and reduces cognitive load for developers, streamlining code reviews and improving overall code quality documentation for future contributions.

### 🔄 Misc / Other
#### [#34514: refactor: remove unnecessary `std::move` for a few trivially copyable types](https://github.com/bitcoin/bitcoin/pull/34514)
**Author:** [@l0rinc](https://github.com/l0rinc)
> This is a minor internal code cleanup to make Bitcoin Core slightly more efficient and easier for developers to maintain.

**Technical Details:** This refactor removes unnecessary `std::move` calls for trivially copyable types. For fundamental types like integers or pointers, `std::move` offers no performance benefit and can sometimes hinder compiler optimizations or obscure intent. Removing these redundant calls simplifies the C++ codebase, improves readability, and brings the code closer to modern C++ best practices, contributing to overall code quality without altering functionality.

#### [#35200: node: smooth oversized `dbcache` warnings](https://github.com/bitcoin/bitcoin/pull/35200)
**Author:** [@l0rinc](https://github.com/l0rinc)
> The node's warnings about excessive memory usage for the database cache are now smarter and less frequent.

**Technical Details:** This change refines the mechanism for warning about an oversized `dbcache`. Previously, these warnings could be too aggressive or frequent, especially for users with large memory configurations. The update likely introduces a more sophisticated triggering logic, such as debouncing or higher thresholds, to smooth out these notifications. This ensures that users only receive warnings when the oversized cache is genuinely problematic and persistent, improving the operator experience and reducing alert fatigue.

### ⚙️ Consensus & Cryptography
#### [#35380: kernel: expose witness stack and scriptSig for btck_TransactionInput](https://github.com/bitcoin/bitcoin/pull/35380)
**Author:** [@pzafonte](https://github.com/pzafonte)
> More transaction details are now exposed for internal components, which can enable new features and improve how the node validates transactions.

**Technical Details:** This PR exposes the witness stack and `scriptSig` fields within the `btck_TransactionInput` struct, which is part of the transaction kernel interface. By making these essential components directly accessible, it enables more complete and efficient transaction validation and introspection within the kernel without requiring re-parsing from raw transaction data. This improves modularity, reduces boilerplate, and is a foundational step for advanced transaction processing and validation logic.

#### [#35572: coins: make cursor iteration DB-only](https://github.com/bitcoin/bitcoin/pull/35572)
**Author:** [@l0rinc](https://github.com/l0rinc)
> This update makes the way Bitcoin Core accesses its transaction database more efficient and robust.

**Technical Details:** This PR refactors the `CoinsDB` (UTXO set database) cursor iteration to be strictly database-driven. By making cursor operations 'DB-only,' it ensures that iteration logic directly interacts with the underlying database interface, avoiding potential inconsistencies or performance overheads from in-memory caching or mixed logic. This change improves the clarity, efficiency, and reliability of large-scale UTXO set traversal operations, which are crucial for blockchain synchronization and validation.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

### 🛠️ Build, CI & Testing
#### [#35590: test: wallet: BnB incomplete result on attempt-limit success](https://github.com/bitcoin/bitcoin/pull/35590)
**Author:** [@brunoerg](https://github.com/brunoerg)
*(Activity: 17 review events this week)*
> This test ensures the wallet's coin selection algorithm always provides the most optimal results for your transactions.

### 🔄 Misc / Other
#### [#35215: coins: use SipHash-1-3-UJ for CCoinsMap keys](https://github.com/bitcoin/bitcoin/pull/35215)
**Author:** [@l0rinc](https://github.com/l0rinc)
*(Activity: 16 review events this week)*
> We're enhancing the core technology that tracks unspent transaction outputs, making your Bitcoin node more efficient and secure. This improvement helps protect the network against potential denial-of-service attacks.

#### [#35729: refactor: test: Unroll `&&` conditions in macros](https://github.com/bitcoin/bitcoin/pull/35729)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob)
*(Activity: 11 review events this week)*
> We're refining our internal testing framework to make tests clearer and more reliable. This helps us catch potential issues faster and maintain the high quality and stability of Bitcoin Core.

### 📡 RPC, APIs & ZMQ
#### [#35730: http: limit connected HTTPRemoteClients](https://github.com/bitcoin/bitcoin/pull/35730)
**Author:** [@pinheadmz](https://github.com/pinheadmz)
*(Activity: 13 review events this week)*
> We're adding safeguards to prevent excessive connections to your Bitcoin node's administrative interface. This protects it from potential overload, ensuring its stability and responsiveness.

### 👛 Wallet & Keys
#### [#35501: wallet: store all witness variants of a transaction](https://github.com/bitcoin/bitcoin/pull/35501)
**Author:** [@achow101](https://github.com/achow101)
*(Activity: 9 review events this week)*
> We're upgrading the Bitcoin wallet to better handle and reconstruct advanced transaction types. This makes your wallet more robust and improves its compatibility with future Bitcoin features.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [Research] Bitcoin After Block Rewards : preventing miner's deviation when the Bitcoin rewards is zero](https://delvingbitcoin.org/t/research-bitcoin-after-block-rewards-preventing-miners-deviation-when-the-bitcoin-rewards-is-zero/2626/4)
**Source:** Delving | **Started By:** Cameron | **Messages:** 9
> Developers are actively researching long-term strategies to maintain Bitcoin's robust security. This ensures the network remains strong and decentralized even far into the future when transaction fees will entirely fund miners.

**Technical Details:** This discussion thread delves into critical research concerning Bitcoin's long-term security model, specifically addressing miner incentives once the block subsidy diminishes to zero. Participants are exploring mechanisms to prevent miner deviation and ensure the network's security remains robust solely through transaction fees. This involves analyzing economic game theory, potential protocol adjustments, and worst-case scenarios to guarantee continued decentralization and censorship resistance, a foundational challenge for Bitcoin's future sustainability and architectural integrity.

### [Re: Segwit commitment to post-quantum witness data?](https://delvingbitcoin.org/t/segwit-commitment-to-post-quantum-witness-data/2702/4)
**Source:** Delving | **Started By:** Anthony Towns | **Messages:** 6
> Developers are discussing ways to future-proof Bitcoin against potential threats from quantum computers. This research explores how to ensure the long-term security of your transactions for decades to come.

**Technical Details:** This discussion centers on the complex topic of adapting SegWit commitments to account for post-quantum cryptographic considerations, specifically for witness data. Participants are examining how to integrate quantum-resistant signatures or proofs into the existing SegWit structure without requiring a hard fork, evaluating the implications for transaction malleability, data size, and consensus rules. This critical research aims to ensure Bitcoin's long-term cryptographic security against future quantum computing advancements, preserving the architectural integrity of its commitment schemes.

## 🏆 Contributor Shoutouts
### 🎉 First-Time Merges
Welcome to the codebase: [@kwsantiago](https://github.com/kwsantiago), [@pseudoramdom](https://github.com/pseudoramdom)

### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@hebasto](https://github.com/hebasto), [@l0rinc](https://github.com/l0rinc), [@maflcko](https://github.com/maflcko), [@polespinasa](https://github.com/polespinasa)

### 🕵️ Top Reviewers
Providing critical review and testing: [@maflcko](https://github.com/maflcko), [@l0rinc](https://github.com/l0rinc), [@hebasto](https://github.com/hebasto), [@brunoerg](https://github.com/brunoerg), [@ryanofsky](https://github.com/ryanofsky)
