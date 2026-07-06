# 📰 This Week in Bitcoin (2026-06-29 to 2026-07-05)

## 📌 The TL;DR
- Ongoing efforts are strongly focused on enhancing the robustness, security, and portability of Bitcoin Core, seen through expanded CI (e.g., RISC-V, OpenBSD), extensive fuzzing additions, and architectural improvements enabling greater wallet modularity and watch-only functionality.
- The developer discussions indicate significant exploration into potential fundamental protocol changes, including new block data structures (Segregated Data), transaction validity rules (input-triggered expiry), advanced scripting mechanisms, and long-term economic sustainability challenges like the diminishing block subsidy.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core.

### 👛 Wallet & Keys
#### [#32489: wallet: Add `exportwatchonlywallet` RPC to export a watchonly version of a wallet](https://github.com/bitcoin/bitcoin/pull/32489)
**Author:** [@achow101](https://github.com/achow101)
> This new feature will allow you to easily create a secure, "watch-only" version of your wallet, making it safer to monitor your funds without exposing your private keys. You'll be able to check your balance and transaction history on less secure devices without risk.

**Technical Details:** This PR introduces a new RPC, `exportwatchonlywallet`, which generates a new wallet file containing only the public keys and transaction data from an existing wallet. This new wallet will be marked as "watch-only" and will not contain any private keys, enhancing security by allowing users to monitor their funds on potentially less secure systems without the risk of spending. The implementation involves serializing relevant public key and script data into a new wallet database, ensuring proper flag propagation and preventing the inclusion of sensitive spending keys.

#### [#34959: wallet: Enforce BDB btree levels and overflow item sizes](https://github.com/bitcoin/bitcoin/pull/34959)
**Author:** [@achow101](https://github.com/achow101)
> We've improved the reliability and performance of your Bitcoin wallet by ensuring its internal database structure is more robust. This change helps prevent data corruption and keeps your wallet running smoothly.

**Technical Details:** This PR enforces specific constraints on Berkeley DB (BDB) btree levels and overflow item sizes within the wallet database. By setting minimum and maximum btree levels and ensuring overflow items are appropriately sized, it prevents BDB from creating inefficient or unstable database structures. This enhances wallet robustness, reduces the likelihood of database corruption due to suboptimal BDB configurations, and improves overall read/write performance for wallet operations, particularly as wallets grow in size.

### 🛠️ Build, CI & Testing
#### [#31425: CI: Add Riscv bare metal job](https://github.com/bitcoin/bitcoin/pull/31425)
**Author:** [@sedited](https://github.com/sedited)
> We're expanding our automated testing to support the open-source RISC-V hardware architecture, ensuring Bitcoin Core runs reliably on an even wider range of devices.

**Technical Details:** This PR integrates a new Continuous Integration (CI) job that compiles and tests Bitcoin Core on RISC-V architecture in a 'bare metal' environment. This means the tests run directly on the hardware without a full operating system abstraction. This low-level testing ensures architectural compatibility and identifies potential issues specific to RISC-V instruction sets and memory models, crucial for supporting diverse hardware platforms and expanding Bitcoin Core's reach.

#### [#35397: ci: add OpenBSD Clang cross job](https://github.com/bitcoin/bitcoin/pull/35397)
**Author:** [@fanquake](https://github.com/fanquake)
> We're now automatically testing Bitcoin Core with the Clang compiler on OpenBSD, improving security and compatibility for users on this robust operating system.

**Technical Details:** This PR introduces a new CI job that cross-compiles Bitcoin Core for OpenBSD using the Clang compiler. This addition ensures that the codebase correctly builds and functions on OpenBSD, a security-focused operating system, and verifies compatibility with alternative compiler toolchains. Regular testing against diverse platforms and compilers like this helps catch platform-specific bugs and ensures broader software portability and resilience.

#### [#35510: test: SOCKS5 proxy: expect that connection may be reset during SOCKS5 handshake or data forwarding](https://github.com/bitcoin/bitcoin/pull/35510)
**Author:** [@vasild](https://github.com/vasild)
> Our software is now smarter about handling temporary network issues when using SOCKS5 proxies, making your connections more resilient and reliable.

**Technical Details:** This PR updates SOCKS5 proxy integration tests to explicitly account for and tolerate `ECONNRESET` errors (connection resets) that can occur during both the SOCKS5 handshake phase and subsequent data forwarding. By expecting and handling these transient network conditions in tests, the software better reflects real-world network instability. This improvement significantly enhances the robustness and resilience of Bitcoin Core's privacy-enhancing proxy connections.

#### [#35147: depends: Boost 1.91.0-1](https://github.com/bitcoin/bitcoin/pull/35147)
**Author:** [@fanquake](https://github.com/fanquake)
> We've updated a core software component, Boost, to its latest version, bringing performance improvements, security fixes, and greater stability to Bitcoin Core.

**Technical Details:** This PR updates the Boost library dependency to version 1.91.0-1. Boost is a collection of peer-reviewed portable C++ source libraries used extensively within Bitcoin Core. Upgrading this dependency ensures that the project benefits from the latest upstream bug fixes, security patches, performance enhancements, and new functionalities. This is a crucial maintenance task that contributes to the overall stability, security, and long-term maintainability of the codebase.

#### [#35129: test: add fuzz test for private broadcast](https://github.com/bitcoin/bitcoin/pull/35129)
**Author:** [@vasild](https://github.com/vasild)
> We're expanding our sophisticated bug detection methods to enhance the reliability and security of private transaction broadcasting features.

**Technical Details:** This PR introduces a new fuzz test specifically targeting the 'private broadcast' mechanism, likely related to transaction relay privacy protocols such as Dandelion++. Fuzzing randomly injects malformed or unexpected data inputs to uncover edge cases, vulnerabilities, or crashes in the implementation. This test aims to bolster the robustness and security of sensitive privacy-related network interactions, making the privacy features more resilient against attack or malfunction.

#### [#35603: build: QRencode cleanups](https://github.com/bitcoin/bitcoin/pull/35603)
**Author:** [@hebasto](https://github.com/hebasto)
> Minor improvements have been made to the QR code generation component, making it more efficient and tidier for developers.

**Technical Details:** This PR performs various cleanups and minor refactorings within the QRencode integration in the Bitcoin Core build system. This includes removing dead code, improving code style, and potentially updating build flags or dependencies related to QR code generation. The change aims to reduce technical debt, enhance maintainability, and ensure the QR code functionality remains efficient and well-integrated without introducing new features or altering user-facing behavior.

#### [#35438: test: introduce NodeSigner, run feature_taproot.py without wallet compiled](https://github.com/bitcoin/bitcoin/pull/35438)
**Author:** [@theStack](https://github.com/theStack)
> We've made our advanced Taproot feature testing more efficient and versatile, allowing quicker and more focused validation of complex upgrades.

**Technical Details:** This PR introduces a `NodeSigner` interface, abstracting away the wallet's signing capabilities for integration tests. This allows the `feature_taproot.py` test suite to execute and validate Taproot-specific functionality without requiring the full wallet component to be compiled. Decoupling the signing logic from the wallet reduces test compilation times, simplifies test setups, and enables more focused, efficient testing of protocol features independent of wallet-specific dependencies. This improves our testing infrastructure significantly.

#### [#35609: ci: Bump tsan config to ubuntu:26.04 with -U_FORTIFY_SOURCE](https://github.com/bitcoin/bitcoin/pull/35609)
**Author:** [@maflcko](https://github.com/maflcko)
> Our automated tests for finding memory and threading issues are now running on a newer, more robust system. This helps us catch subtle bugs sooner.

**Technical Details:** This PR updates the ThreadSanitizer (TSan) configuration within the Continuous Integration (CI) pipeline. It specifically bumps the base Docker image for the TSan job to `ubuntu:26.04` and introduces the `-U_FORTIFY_SOURCE` flag. This upgrade provides a more modern and potentially more effective environment for detecting concurrency bugs and memory errors, while the flag ensures `_FORTIFY_SOURCE` optimizations are not enabled, which could interfere with TSan's diagnostic capabilities, leading to more accurate bug detection.

#### [#35640: ci: use a 8x instance over 16x for riscv job](https://github.com/bitcoin/bitcoin/pull/35640)
**Author:** [@fanquake](https://github.com/fanquake)
> We've optimized the resources used for our RISC-V testing, making our development process more efficient and cost-effective without compromising quality.

**Technical Details:** This PR adjusts the Continuous Integration (CI) configuration for the RISC-V bare metal job, switching from a 16x compute instance to a smaller, more efficient 8x instance. This optimization aims to reduce CI resource consumption and associated costs by allocating hardware proportional to the actual workload requirements for the RISC-V compilation and testing. This change improves overall CI pipeline efficiency and resource management without impacting test coverage.

#### [#35118: fuzz: add ipc round-trip fuzz target](https://github.com/bitcoin/bitcoin/pull/35118)
**Author:** [@enirox001](https://github.com/enirox001)
> We've added new testing methods to make inter-process communication (IPC) within Bitcoin Core more robust and secure. This helps ensure different parts of the software communicate reliably, reducing the risk of bugs and vulnerabilities.

**Technical Details:** This PR introduces a new fuzz target specifically designed to test the inter-process communication (IPC) mechanisms within Bitcoin Core. The "IPC round-trip" fuzz target generates arbitrary data and simulates sending and receiving it between different process components, exercising the serialization, deserialization, and message handling logic. This improves the robustness and security of our IPC implementation by identifying potential crashes, memory errors, or logic bugs that could arise from malformed or unexpected messages during inter-process data exchange.

#### [#35653: fuzz: Remove `ConsumeUniValue`](https://github.com/bitcoin/bitcoin/pull/35653)
**Author:** [@marcofleon](https://github.com/marcofleon)
> We've refined our automated testing tools by removing an unnecessary function, making our testing framework cleaner and more focused. This helps us write better tests to find more bugs.

**Technical Details:** This PR removes the `ConsumeUniValue` function from Bitcoin Core's fuzzing framework. `ConsumeUniValue` was likely a utility function used to extract and consume `UniValue` objects during fuzzing, but its functionality might have become redundant, been refactored, or deemed unnecessary for current fuzz targets. Removing it simplifies the fuzzing codebase, reduces potential points of failure or confusion within the testing harness, and streamlines the process of writing new fuzz targets, leading to a more maintainable and efficient testing infrastructure for identifying vulnerabilities and bugs.

#### [#35607: nanobench: fix performance counter buffer initialization](https://github.com/bitcoin/bitcoin/pull/35607)
**Author:** [@l0rinc](https://github.com/l0rinc)
> We've improved the accuracy of our internal performance benchmarking tool, ensuring that developers get more precise measurements when optimizing Bitcoin Core. This helps in making informed decisions about future performance enhancements.

**Technical Details:** This PR fixes an issue related to the initialization of performance counter buffers within `nanobench`, Bitcoin Core's micro-benchmarking tool. An incorrect or incomplete buffer initialization could lead to inaccurate or corrupted performance metrics during benchmarking runs, hindering proper analysis of code changes. The fix ensures that these buffers are consistently and correctly initialized before each benchmark, guaranteeing reliable and precise performance measurements across different environments and configurations. This improved accuracy is vital for effective optimization efforts and comparing the performance impact of code modifications.

#### [#35615: fuzz: restore CreateSock in PCP targets](https://github.com/bitcoin/bitcoin/pull/35615)
**Author:** [@HowHsu](https://github.com/HowHsu)
> We're enhancing our automated bug-finding tools to better test network communication, making Bitcoin Core more secure and reliable against potential vulnerabilities.

**Technical Details:** This PR restores the `CreateSock` functionality within fuzzing targets for Peer-to-Peer Communication (PCP), specifically addressing issues where socket creation was unintentionally mocked or removed. Re-enabling actual socket creation allows fuzz tests to effectively exercise the network layer, uncovering subtle bugs and edge cases related to connection handling and protocol parsing that would otherwise be missed. This improves the robustness of our network stack.

### 📝 Documentation
#### [#35599: doc: Add release notes for #33671 (getbalances nonmempool field)](https://github.com/bitcoin/bitcoin/pull/35599)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
> We've updated our documentation to clearly explain a new field in `getbalances` that helps users better track their funds.

**Technical Details:** This PR adds comprehensive release notes documenting the `nonmempool` field introduced in PR #33671 for the `getbalances` RPC call. This field provides a detailed breakdown of amounts held by the wallet that are not currently in the mempool (e.g., watch-only inputs, internal change outputs that are already confirmed). Documenting this explicitly helps users understand new RPC outputs and accurately interpret their wallet's balance state for better accounting.

### 🔄 Misc / Other
#### [#35604: log: expose `-logratelimit` in normal help](https://github.com/bitcoin/bitcoin/pull/35604)
**Author:** [@l0rinc](https://github.com/l0rinc)
> It's now easier to find and use the logging rate limit setting, giving you better control over how much information Bitcoin Core writes to its log files. This helps keep log files manageable without missing critical alerts.

**Technical Details:** This PR updates the help output for `bitcoind` and `bitcoin-qt` to explicitly include the `-logratelimit` configuration option in the normal help section. Previously, this important option was only visible in the advanced debug help (`-help-debug`). Exposing it in the standard help makes the log rate limiting feature more discoverable and accessible to a broader user base. This improves user experience by allowing easier configuration of log verbosity and preventing excessive log file growth, without compromising the ability to capture important events.

#### [#35610: bitcoin-util: Add netmagic command](https://github.com/bitcoin/bitcoin/pull/35610)
**Author:** [@ekzyis](https://github.com/ekzyis)
> We've added a new utility command that helps developers and advanced users quickly identify the correct network for various Bitcoin operations. This makes development and troubleshooting across different networks like mainnet or testnet much easier.

**Technical Details:** This PR introduces a new `netmagic` command to the `bitcoin-util` tool. This command allows users to retrieve the "network magic" bytes for a specified Bitcoin network (e.g., mainnet, testnet, signet, regtest). These magic bytes are crucial for identifying network messages and ensuring compatibility across different network instances. The implementation involves a simple lookup and output of the predefined network magic values, providing a convenient and reliable way for developers and power users to quickly obtain this essential network-specific constant without manually looking up source code.

#### [#35597: logging: More fully remove libevent log category](https://github.com/bitcoin/bitcoin/pull/35597)
**Author:** [@ryanofsky](https://github.com/ryanofsky)
> We've streamlined our internal logging system by removing an outdated component, making it cleaner and more efficient. This improves overall code quality and maintainability.

**Technical Details:** This PR completes the removal of the `libevent` log category from Bitcoin Core's logging system. While `libevent` itself is still utilized for networking tasks, its specific log category was no longer relevant or actively used for internal log statements. This change involves reviewing and ensuring no lingering references to the `libevent` category exist in log macros or configuration, thus simplifying the logging infrastructure. This improves code hygiene, reduces potential confusion for developers, and slightly decreases the logging system's overhead.

### 🛡️ Consensus & Cryptography
#### [#35634: txospenderindex: use zero-byte entry values](https://github.com/bitcoin/bitcoin/pull/35634)
**Author:** [@l0rinc](https://github.com/l0rinc)
> We've made an internal database index more efficient by optimizing how it stores certain data. This improvement helps Bitcoin Core process transaction data more quickly and with less storage.

**Technical Details:** This PR optimizes the `txospenderindex` by changing its database entry values to zero-byte entries. This index maps outpoints to the transactions that spend them. Since the value itself only needs to exist to indicate the presence of a spender (the key provides the necessary information), storing a zero-byte value is sufficient and significantly more space-efficient than storing a full transaction ID or other data. This reduces the disk footprint of the UTXO set and potentially improves read/write performance for this specific index, contributing to overall database efficiency for node operators.

### 📡 RPC, APIs & ZMQ
#### [#35614: HTTPServer: Prevent race condition between worker thread and I/O thread](https://github.com/bitcoin/bitcoin/pull/35614)
**Author:** [@pinheadmz](https://github.com/pinheadmz)
> We've fixed an issue that could cause rare crashes or instability in the built-in HTTP server, making it more reliable when interacting with external services. This ensures smoother communication and a more stable experience.

**Technical Details:** This PR addresses a critical race condition within the `HTTPServer` between the worker thread handling requests and the I/O thread managing connection events. The race could occur when a worker thread attempts to access a connection that the I/O thread might be simultaneously closing or deallocating, leading to use-after-free bugs or crashes. The fix involves implementing proper synchronization mechanisms, likely through mutexes or careful state management of shared `HTTPConnection` objects, to ensure safe access to connection resources across thread boundaries, guaranteeing their validity throughout their lifecycle and significantly improving the server's stability.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

### 🔄 Misc / Other
#### [#35621: validation: Ignore eventual error message from flushing in AcceptBlock](https://github.com/bitcoin/bitcoin/pull/35621)
**Author:** [@optout21](https://github.com/optout21)
*(Activity: 18 review events this week)*
> This upcoming change will make block validation more resilient, ensuring that Bitcoin Core handles minor, non-critical issues during block processing without unnecessarily rejecting valid blocks.

#### [#35569: Encapsulation for CTransaction](https://github.com/bitcoin/bitcoin/pull/35569)
**Author:** [@purpleKarrot](https://github.com/purpleKarrot)
*(Activity: 15 review events this week)*
> We're making foundational improvements to how Bitcoin transactions are handled internally. This work makes the code more robust and easier to maintain, leading to a more reliable Bitcoin.

#### [#35295: validation: fetch block input prevouts in parallel during ConnectBlock](https://github.com/bitcoin/bitcoin/pull/35295)
**Author:** [@andrewtoth](https://github.com/andrewtoth)
*(Activity: 13 review events this week)*
> Developers are working on a significant performance improvement that will make nodes process new blocks much faster by fetching transaction data in parallel.

### 👛 Wallet & Keys
#### [#32489: wallet: Add `exportwatchonlywallet` RPC to export a watchonly version of a wallet](https://github.com/bitcoin/bitcoin/pull/32489)
**Author:** [@achow101](https://github.com/achow101)
*(Activity: 12 review events this week)*
> This new feature will allow you to easily create a secure, "watch-only" version of your wallet, making it safer to monitor your funds without exposing your private keys. You'll be able to check your balance and transaction history on less secure devices without risk.

### 🛠️ Build, CI & Testing
#### [#35619: test:  ExtendedPrivateKey follow-ups](https://github.com/bitcoin/bitcoin/pull/35619)
**Author:** [@rkrux](https://github.com/rkrux)
*(Activity: 10 review events this week)*
> We're continuously improving and verifying the security of how your Bitcoin keys are managed. This ensures your funds remain safe and accessible through hierarchical deterministic wallets.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: [BIP Draft] Segregated Data: a prunable, script-isolated block region for data carriage](https://delvingbitcoin.org/t/bip-draft-segregated-data-a-prunable-script-isolated-block-region-for-data-carriage/2641/26)
**Source:** Delving | **Started By:** Murch | **Messages:** 13
> Developers are discussing a new proposal to store non-financial data on the Bitcoin blockchain more efficiently. This could help keep the network lean by allowing certain data to be discarded later, reducing long-term storage needs.

**Technical Details:** This thread delves into a BIP draft for 'Segregated Data,' proposing a new, distinct region within Bitcoin blocks. This region would be specifically designed for arbitrary data carriage, isolated from existing script validation, and crucially, designated as 'prunable' data. The architectural value lies in providing a dedicated, consensus-safe mechanism for embedding data without permanently burdening the full node's storage requirements, thus mitigating blockchain bloat. The discussion covers implementation specifics, potential impacts on node operation, and the incentives for data inclusion and pruning.

### [Re: Input-triggered transaction expiry](https://delvingbitcoin.org/t/input-triggered-transaction-expiry/2667/2)
**Source:** Delving | **Started By:** josh | **Messages:** 12
> Developers are discussing new ways for Bitcoin transactions to automatically expire under certain conditions. This could enable novel applications and more efficient use of the network.

**Technical Details:** The discussion thread 'Re: Input-triggered transaction expiry' explores a conceptual feature where the validity or expiry of a transaction is determined by specific conditions on its inputs. This entails discussing potential consensus changes to introduce opcodes or script capabilities that allow for time-based or event-based expiry tied to the UTXOs being spent. The conversation likely covers use cases like payment channels, covenants, security implications, and the complexity of implementing such a feature without introducing undue burden or attack vectors on the network.

## 🏆 Contributor Shoutouts
### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@l0rinc](https://github.com/l0rinc), [@achow101](https://github.com/achow101), [@vasild](https://github.com/vasild), [@HowHsu](https://github.com/HowHsu)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@l0rinc](https://github.com/l0rinc), [@maflcko](https://github.com/maflcko), [@polespinasa](https://github.com/polespinasa), [@hebasto](https://github.com/hebasto)
