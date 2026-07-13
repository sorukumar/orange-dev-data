# 📰 This Week in Bitcoin (2026-07-06 to 2026-07-12)

## 📌 The TL;DR
- Significant performance improvements landed in block validation, most notably the parallel fetching of prevouts during `ConnectBlock`, drastically reducing the time required for initial sync and catch-up.
- New IPC methods were added to empower external mining operations with more flexible block template construction, alongside ongoing active discussions on advanced protocol enhancements like P2MR, privacy-focused techniques (e.g., zkPoH), and the economic implications of new script types for fee selection.

## 🚢 Core Code (Merged This Week)
The most critical pull requests merged into Bitcoin Core.

### ⚡ P2P & Network
#### [#32606: p2p: Drop unsolicited CMPCTBLOCK from non-HB peer and when blocksonly](https://github.com/bitcoin/bitcoin/pull/32606)
**Author:** [@davidgumberg](https://github.com/davidgumberg)
> Bitcoin Core is becoming more efficient in how it receives block data, ignoring unnecessary messages to improve network performance and security.

**Technical Details:** This P2P protocol enhancement modifies node behavior to drop unsolicited `CMPCTBLOCK` messages from peers that are not designated as high-bandwidth (HB) peers. Additionally, such messages are now ignored when the node is operating in `blocksonly` mode. This change reduces unnecessary network traffic, conserves resources, and mitigates potential denial-of-service vectors by preventing peers from sending unrequested compact block data.

#### [#34997: p2p: Don't participate in addr relay with feelers](https://github.com/bitcoin/bitcoin/pull/34997)
**Author:** [@danielabrozzoni](https://github.com/danielabrozzoni)
> To improve privacy and network efficiency, Bitcoin Core nodes will no longer share address information with temporary, short-lived connections.

**Technical Details:** This P2P protocol change prevents 'feeler' connections from participating in address relay. Feeler connections are short-lived, exploratory connections made to test network reachability. By not exchanging address information with these temporary peers, we enhance the privacy of our node's address book, reduce unnecessary network traffic, and prevent feeler connections from polluting the network's address discovery mechanisms.

#### [#35670: net: optimize compact block extra tx iteration](https://github.com/bitcoin/bitcoin/pull/35670)
**Author:** [@l0rinc](https://github.com/l0rinc)
> We've made Bitcoin Core faster and more efficient at synchronizing with the network. This means your node can process blocks more quickly and use less power.

**Technical Details:** This PR optimizes the iteration over extra transactions during compact block reconstruction. By improving the search and processing logic for transactions not initially present in a compact block, it reduces CPU overhead and improves the overall efficiency of block propagation and synchronization, particularly benefiting nodes with slower connections or during initial sync. The change targets `ProcessCompactBlockTxns()` by optimizing the loop structure and access patterns when matching transactions.

#### [#35406: private broadcast: limit outstanding txs to count of 10,000](https://github.com/bitcoin/bitcoin/pull/35406)
**Author:** [@instagibbs](https://github.com/instagibbs)
> We've added a safeguard to an experimental privacy feature to prevent it from consuming too many resources.
This ensures the network remains stable and efficient for everyone, even with new features under development.

**Technical Details:** This PR implements a hard limit of 10,000 outstanding transactions for the private transaction broadcast mechanism. This is a critical resource management improvement for the experimental 'assumeutxo' private broadcast feature. The limit prevents excessive memory usage and potential DoS vectors by ensuring that the node doesn't hold an unbounded number of unconfirmed private transactions, thus stabilizing the system while this feature is under development and undergoing testing for scalability.

#### [#35691: chainparams: delete my DNS seed](https://github.com/bitcoin/bitcoin/pull/35691)
**Author:** [@sipa](https://github.com/sipa)
> A volunteer-provided address used to help new Bitcoin nodes find peers is being removed.
This reflects a natural evolution of network bootstrapping resources and decentralization.

**Technical Details:** This PR involves the removal of a specific DNS seed from the `chainparams` configuration. DNS seeds are hardcoded entry points used by new nodes to discover initial peers on the network when starting up for the first time. The removal of a seed typically occurs when it is no longer actively maintained, or if the network has matured sufficiently to reduce reliance on specific centralized bootstrapping points, contributing to a more robust and decentralized peer discovery mechanism.

#### [#35667: refactor: Use `NetworkErrorString` for macOS code in `netif.cpp`](https://github.com/bitcoin/bitcoin/pull/35667)
**Author:** [@hebasto](https://github.com/hebasto)
> We're making our code more consistent across different operating systems, which helps us maintain and improve Bitcoin Core more easily.

**Technical Details:** This refactoring modifies `netif.cpp` to use the standardized `NetworkErrorString` utility function for error reporting specifically within macOS-related network interface code. This change improves code consistency by consolidating error message generation, reducing duplication, and making error handling more uniform across different platforms and network-related functions.

### 📝 Documentation
#### [#35386: doc: add an AI contribution policy](https://github.com/bitcoin/bitcoin/pull/35386)
**Author:** [@willcl-ark](https://github.com/willcl-ark)
> We've added a new policy on how contributors should use AI tools when working on Bitcoin Core. This ensures that our code quality and open-source principles are maintained.

**Technical Details:** This PR introduces a formal policy document outlining guidelines for AI tool usage within the Bitcoin Core project. It details acceptable practices, such as using AI for grammar checks or code explanations, while prohibiting direct AI-generated code submission without rigorous human review, testing, and explicit attribution. This policy is crucial for maintaining code integrity, security standards, and adhering to open-source licensing principles in an evolving development landscape.

#### [#35669: doc: archive release notes for v31.1](https://github.com/bitcoin/bitcoin/pull/35669)
**Author:** [@fanquake](https://github.com/fanquake)
> Our documentation has been updated to archive older release notes for version 31.1, keeping our active notes clear and current.

**Technical Details:** This PR moves the release notes for Bitcoin Core version 31.1 from the active `doc/release-notes` directory into the `doc/release-notes/archive` directory. This is a routine documentation maintenance task that helps keep the main release notes directory focused on current and upcoming versions, while ensuring historical release information remains accessible and organized.

#### [#35650: doc: Add release notes for 32489 (exportwatchonlywallet RPC)](https://github.com/bitcoin/bitcoin/pull/35650)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
> We've added release notes for a new feature that allows users to export watch-only wallet data, making it easier to manage funds with enhanced privacy.

**Technical Details:** This PR adds specific release notes detailing the `exportwatchonlywallet` RPC, which was previously merged. The notes describe its functionality, parameters, and use cases, allowing users to securely export extended public keys (xpubs) and other watch-only data into a new, separate wallet. This improves usability and discoverability of the RPC, enhancing user workflows for privacy and cold storage setups.

#### [#35700: doc: archive release notes for v29.4](https://github.com/bitcoin/bitcoin/pull/35700)
**Author:** [@fanquake](https://github.com/fanquake)
> We've organized our documentation by archiving the release notes for version 29.4, ensuring our current release information is easy to find.

**Technical Details:** Similar to other archiving PRs, this change moves the release notes documentation for Bitcoin Core version 29.4 from the primary `doc/release-notes` directory to the `doc/release-notes/archive` folder. This is part of ongoing documentation hygiene, ensuring that older, stable release information is preserved but doesn't clutter the active documentation set for developers and users.

#### [#35685: doc: Archive 30.3 release notes](https://github.com/bitcoin/bitcoin/pull/35685)
**Author:** [@achow101](https://github.com/achow101)
> Our documentation now has older release notes for version 30.3 archived, helping to keep everything neatly organized and up-to-date.

**Technical Details:** This PR performs the routine task of archiving the release notes for Bitcoin Core version 30.3. The relevant markdown file is moved from the active release notes directory into the designated archive location within `doc/release-notes/archive`. This practice helps maintain a clean and manageable documentation structure, providing clear separation between current and historical releases.

#### [#35651: doc: Improve offline-signing-tutorial after 32489](https://github.com/bitcoin/bitcoin/pull/35651)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
> We've improved our tutorial on offline transaction signing, making it simpler and more secure to manage your Bitcoin without exposing your private keys online.

**Technical Details:** This PR enhances the existing `doc/offline-signing-tutorial.md` by incorporating and explaining the usage of the recently added `exportwatchonlywallet` RPC. The tutorial is updated to leverage this RPC for creating watch-only wallets, streamlining the setup process for offline signing. This provides clearer, more secure instructions for users, improving the overall understanding and practicality of cold storage and offline transaction workflows.

### 👛 Wallet & Keys
#### [#35655: wallet: Use in-memory SQLite for temporary wallet in exportwatchonlywallet](https://github.com/bitcoin/bitcoin/pull/35655)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
> We are reviewing a change to improve the security and privacy of exporting watch-only wallet data, by ensuring temporary sensitive information never touches your computer's disk.

**Technical Details:** This 'Hot PR' proposes to refactor the `exportwatchonlywallet` RPC implementation to utilize an in-memory SQLite database for temporary wallet creation. Currently, temporary wallet data might briefly touch disk before deletion. This change aims to entirely prevent sensitive intermediate data from being written to persistent storage by keeping it exclusively in RAM, significantly enhancing the security and privacy posture of the `exportwatchonlywallet` RPC and reducing the risk of data leakage or forensic recovery.

### 🛠️ Build, CI & Testing
#### [#35412: ci: add NetBSD Clang cross job](https://github.com/bitcoin/bitcoin/pull/35412)
**Author:** [@fanquake](https://github.com/fanquake)
> We've expanded our automated testing to include NetBSD using Clang, ensuring Bitcoin Core works reliably on even more operating systems.

**Technical Details:** This PR integrates a new Continuous Integration (CI) job for NetBSD using the Clang compiler into the Bitcoin Core build pipeline. This new cross-compilation and testing environment will detect platform-specific issues earlier, improving portability and ensuring the codebase is robust across a wider range of operating systems. It enhances our quality assurance process by adding another layer of automated verification for builds and basic tests.

#### [#35701: test: Remove `mock_process.cpp`](https://github.com/bitcoin/bitcoin/pull/35701)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob)
> We've cleaned up our internal testing tools by removing an obsolete component, streamlining our development process.

**Technical Details:** This PR removes the `mock_process.cpp` utility file from the test suite. This file likely provided mocking capabilities for process-related operations during testing, but its functionality is either no longer needed, has been refactored, or replaced by more modern testing frameworks. Deleting this unused component simplifies the test codebase, reduces technical debt, and improves the overall maintainability and clarity of the testing infrastructure.

#### [#35649: depends: move FreeBSD SDK handling to CI](https://github.com/bitcoin/bitcoin/pull/35649)
**Author:** [@fanquake](https://github.com/fanquake)
> Building Bitcoin Core for FreeBSD is now more streamlined and reliable. This makes it easier for developers to contribute and ensures consistent operation across various systems.

**Technical Details:** This PR relocates the FreeBSD SDK setup logic from the `depends` build system to the continuous integration (CI) environment. This refactoring simplifies the `depends` framework, making it solely responsible for fetching and building external dependencies. The change improves build reproducibility and reduces boilerplate, allowing for more efficient testing and platform-specific environment management within the CI pipelines for FreeBSD.

#### [#35689: test: Inline incorrect check in `util_tests`](https://github.com/bitcoin/bitcoin/pull/35689)
**Author:** [@rustaceanrob](https://github.com/rustaceanrob)
> A small but important internal cleanup improves the reliability of Bitcoin Core's automated tests. This helps us ensure the software remains robust and error-free.

**Technical Details:** This PR inlined an incorrect and redundant assertion within `util_tests`, specifically addressing a helper function that was not correctly validating its input or intent. The previous check was either misplaced or performing an irrelevant assertion within the test fixture setup. By directly integrating the correct validation logic, the test's intent becomes clearer, improving maintainability and ensuring the test accurately reflects the utility function's expected behavior.

#### [#35616: refactor: Use u64 over size_t for all cache sizes to fix a 32-bit overflow](https://github.com/bitcoin/bitcoin/pull/35616)
**Author:** [@maflcko](https://github.com/maflcko)
> This important update prevents a potential issue where Bitcoin Core could miscalculate memory usage on older computer systems.
It improves stability and reliability across all supported platforms.

**Technical Details:** This refactoring effort standardizes the use of `uint64_t` (or `u64`) for all internal variables representing cache sizes throughout the Bitcoin Core codebase, replacing `size_t`. This change specifically addresses a potential 32-bit integer overflow bug that could occur on systems where `size_t` is 32-bit wide, leading to incorrect cache size calculations and memory allocation issues, which could result in crashes or inefficient resource utilization. Ensuring `u64` is used guarantees correct handling of large memory quantities across all architectures.

#### [#35652: init: fix reindex deadlock by waking cv after interrupt](https://github.com/bitcoin/bitcoin/pull/35652)
**Author:** [@ismaelsadeeq](https://github.com/ismaelsadeeq)
> This fix resolves an issue that could cause the software to freeze when rebuilding its transaction history.
It makes the reindexing process more robust and prevents deadlocks, improving node reliability.

**Technical Details:** This PR addresses a deadlock scenario that could occur during the reindexing process. The fix involves explicitly waking a condition variable (`cv`) after an interrupt signal is received during reindexing. Previously, if an interrupt occurred while a thread was waiting on the `cv` (e.g., during the flush of a block), it might remain indefinitely blocked, leading to a deadlock and a frozen node. By ensuring the `cv` is always notified, even on interruption, the system can correctly terminate or restart the reindex operation without blocking.

#### [#35684: Update libmultiprocess subtree to add `max_connections` option](https://github.com/bitcoin/bitcoin/pull/35684)
**Author:** [@ryanofsky](https://github.com/ryanofsky)
> Our internal testing tools are getting an upgrade, making them more robust and efficient for developing new features.

**Technical Details:** This PR updates the `libmultiprocess` Git subtree, a critical component of our testing infrastructure, by adding a `max_connections` option. This new parameter allows for explicit control over the maximum number of concurrent connections established by the subprocess, improving resource management and test stability, particularly in complex integration tests.

#### [#35658: refactor: Drop unneeded `<sys/types.h>` include before `<ifaddrs.h>`](https://github.com/bitcoin/bitcoin/pull/35658)
**Author:** [@hebasto](https://github.com/hebasto)
> We're continually refining our codebase to keep it clean and efficient, making it easier for developers to work on Bitcoin Core.

**Technical Details:** This refactoring removes an unneeded `#include <sys/types.h>` header file that was previously placed before `#include <ifaddrs.h>`. Modern compilers and POSIX standards ensure that `<ifaddrs.h>` implicitly includes necessary types, making the explicit inclusion redundant. This change slightly improves build times and reduces potential header dependency conflicts.

#### [#35661: Update libmultiprocess subtree to add `ThreadMap.makePool` method](https://github.com/bitcoin/bitcoin/pull/35661)
**Author:** [@ryanofsky](https://github.com/ryanofsky)
> Our testing framework has been enhanced with new tools, allowing for more flexible and powerful test scenarios.

**Technical Details:** This PR updates the `libmultiprocess` Git subtree to introduce a new `ThreadMap.makePool` method. This enhancement provides a more structured and efficient way to manage pools of threads or processes within our testing environment, enabling better parallelism and resource handling during complex test executions, thereby increasing test reliability and speed.

### 🛡️ Consensus & Cryptography
#### [#35295: validation: fetch block input prevouts in parallel during ConnectBlock](https://github.com/bitcoin/bitcoin/pull/35295)
**Author:** [@andrewtoth](https://github.com/andrewtoth)
> Developers are working on a significant performance improvement that will make nodes process new blocks much faster by fetching transaction data in parallel.

**Technical Details:** This 'Hot PR' is under review and proposes a significant performance enhancement for the `ConnectBlock` function within the validation component. It introduces parallel fetching of previous transaction outputs (prevouts) for block inputs. Instead of fetching each prevout sequentially, this change would use multiple threads or asynchronous operations to retrieve the necessary data concurrently from the UTXO set. This optimization is expected to drastically reduce block processing times, especially for large blocks, improving node synchronization speed and overall network performance.

#### [#35464: kernel: Add function for creating chainparams with a signet challenge](https://github.com/bitcoin/bitcoin/pull/35464)
**Author:** [@sedited](https://github.com/sedited)
> Developers can now more easily set up custom test networks called Signets.
This simplifies testing new features in a secure and controlled environment before wider release.

**Technical Details:** This PR introduces a new helper function within the `kernel` module to facilitate the creation of `CChainParams` objects specifically configured for Signet networks. This function takes a Signet challenge script as input, abstracting the complex setup of Signet-specific parameters. This simplifies the process for developers and testers to spin up custom Signet instances, fostering more efficient and standardized testing of new Bitcoin features and protocols without needing to manually configure all parameters.

#### [#35621: validation: Ignore eventual error message from flushing in AcceptBlock](https://github.com/bitcoin/bitcoin/pull/35621)
**Author:** [@optout21](https://github.com/optout21)
> This upcoming change will make block validation more resilient, ensuring that Bitcoin Core handles minor, non-critical issues during block processing without unnecessarily rejecting valid blocks.

**Technical Details:** This PR proposes to adjust the critical `AcceptBlock` logic to ignore specific, non-critical error messages that may occasionally arise from internal flushing operations (e.g., database writes or cache synchronization). These errors do not indicate a fundamental block validation failure but rather transient system-level issues. By treating such eventual errors as non-fatal, the node can avoid rejecting otherwise valid blocks, improving network stability and preventing unnecessary re-downloading without compromising security.

### 📡 RPC, APIs & ZMQ
#### [#34020: mining: add getTransactions(ByWitnessID) IPC methods](https://github.com/bitcoin/bitcoin/pull/34020)
**Author:** [@Sjors](https://github.com/Sjors)
> This update helps miners create blocks more efficiently by giving them better tools to select transactions for inclusion.
It allows for more optimized block construction, improving network throughput.

**Technical Details:** This PR introduces new IPC (Inter-Process Communication) methods, `getTransactions` and `getTransactionsByWitnessID`, to the `mining` module. These methods enable external block construction tools (e.g., Stratum V2 pool implementations) to query mempool transactions by their transaction ID or witness ID. This enhances block template generation by providing a direct and efficient way to retrieve specific transaction data for inclusion, improving integration for mining software and custom block builders.

#### [#35678: private broadcast: define and use new RPC_LIMIT_EXCEEDED error code ( + other follow-ups)](https://github.com/bitcoin/bitcoin/pull/35678)
**Author:** [@stickies-v](https://github.com/stickies-v)
> We've introduced clearer error messages for a new privacy feature.
This makes it easier for users and developers to understand when certain limits are reached, improving user experience.

**Technical Details:** This PR defines and integrates a new `RPC_LIMIT_EXCEEDED` error code for the experimental private broadcast RPCs. This new error specifically signals when the previously implemented limit on outstanding private transactions (from #35406) has been hit, providing a distinct and actionable error. This improves API clarity and allows RPC clients to programmatically identify and handle cases where resource limits prevent further transaction submissions, providing more granular feedback than a generic error code.

### 🔄 Misc / Other
#### [#34897: indexes: Don't commit ahead of the flushed chainstate](https://github.com/bitcoin/bitcoin/pull/34897)
**Author:** [@mzumsande](https://github.com/mzumsande)
> This update improves the reliability and consistency of transaction and address indexes.
It ensures they always reflect the most up-to-date and confirmed state of the blockchain, even after unexpected shutdowns.

**Technical Details:** This change ensures that various optional indexes (e.g., `txindex`, `blockfilterindex`) do not commit their progress to disk until the main chainstate has also been fully flushed and committed. Previously, indexes could commit data that was not yet durably reflected in the persistent chainstate, leading to potential inconsistencies or state rollbacks on unexpected shutdowns. This synchronization guarantees atomic updates across related data stores, significantly improving data integrity and recovery robustness.

#### [#35568: txospenderindex: disable bloom filters to optimize disk usage](https://github.com/bitcoin/bitcoin/pull/35568)
**Author:** [@andrewtoth](https://github.com/andrewtoth)
> We've optimized a specialized transaction index to use less disk space.
This makes it more efficient and practical for users who choose to enable this optional feature.

**Technical Details:** This PR disables the use of Bloom filters within the `txospenderindex`. While Bloom filters can offer probabilistic performance gains for some queries, in the context of `txospenderindex`, they were found to consume significant disk space (multiple gigabytes) without providing a commensurate benefit for its typical usage patterns. Removing them significantly reduces the disk footprint of this optional index, making it more space-efficient and practical for users who opt-in to enable it.

#### [#35673: refactor: Move LoadGenesisBlock to ChainstateManager](https://github.com/bitcoin/bitcoin/pull/35673)
**Author:** [@maflcko](https://github.com/maflcko)
> We're making internal code improvements to better organize how the software handles the very first block of the blockchain.
This leads to a cleaner, more maintainable codebase and improves overall system architecture.

**Technical Details:** This PR proposes moving the `LoadGenesisBlock` function into the `ChainstateManager` class. This refactoring aligns with the ongoing effort to encapsulate chainstate-related logic within `ChainstateManager`, centralizing responsibilities and improving modularity. By integrating genesis block loading here, it streamlines the initialization process, makes the dependencies clearer, and enhances overall architectural consistency within the `kernel` and `validation` components, improving long-term maintainability and readability.

#### [#35695: Remove myself as security contact](https://github.com/bitcoin/bitcoin/pull/35695)
**Author:** [@sipa](https://github.com/sipa)
> We're keeping our security processes fresh and accurate by updating our list of core team security contacts.

**Technical Details:** This PR updates the `doc/security.md` file to remove a specific individual from the list of security contacts. This ensures the contact information for reporting security vulnerabilities remains accurate and reflects current team responsibilities, streamlining the incident response process by directing reports to active maintainers.

#### [#35674: [29.x] Finalise 29.4](https://github.com/bitcoin/bitcoin/pull/35674)
**Author:** [@fanquake](https://github.com/fanquake)
> A new maintenance release, 29.4, is now finalized, ensuring continued stability and minor updates for users running this version.

**Technical Details:** This PR finalizes the 29.4 release branch, preparing it for its official release. This typically involves updating the release notes, bumping the version string to indicate the new maintenance release, and ensuring all necessary changes from the 29.x series are properly integrated and documented for users.

#### [#35668: [30.x] Finalise 30.3](https://github.com/bitcoin/bitcoin/pull/35668)
**Author:** [@fanquake](https://github.com/fanquake)
> The 30.3 maintenance release is now finalized, bringing stability and minor improvements to users on this version branch.

**Technical Details:** This PR finalizes the 30.3 release branch, marking the completion of preparations for its official distribution. This process typically involves updating the changelog, adjusting version numbers, and integrating all confirmed fixes and minor enhancements for users following the 30.x release series.

#### [#35666: [31.x] Finalise 31.1](https://github.com/bitcoin/bitcoin/pull/35666)
**Author:** [@fanquake](https://github.com/fanquake)
> We've finalized the 31.1 release, delivering important stability and improvement updates for the latest version of Bitcoin Core.

**Technical Details:** This PR finalizes the 31.1 release branch, completing the necessary steps for its official release. This includes updating release documentation, incrementing version numbers, and ensuring all included changes from the 31.x development cycle are ready for widespread adoption by users.

## 🔍 Under Review (Hot PRs)
The most actively discussed and reviewed open pull requests right now.

### 👛 Wallet & Keys
#### [#35655: wallet: Use in-memory SQLite for temporary wallet in exportwatchonlywallet](https://github.com/bitcoin/bitcoin/pull/35655)
**Author:** [@pablomartin4btc](https://github.com/pablomartin4btc)
*(Activity: 22 review events this week)*
> We are reviewing a change to improve the security and privacy of exporting watch-only wallet data, by ensuring temporary sensitive information never touches your computer's disk.

#### [#35690: wallet: Introduce WalletError with machine-readable error code](https://github.com/bitcoin/bitcoin/pull/35690)
**Author:** [@pseudoramdom](https://github.com/pseudoramdom)
*(Activity: 18 review events this week)*
> Future updates will provide clearer, more understandable error messages from the wallet, making it easier for users and applications to troubleshoot issues. This improves the experience for anyone building on Bitcoin Core.

### 🔄 Misc / Other
#### [#35676: util: Abort in CheckDiskSpace/FlatFileSeq::Open on rare exceptions](https://github.com/bitcoin/bitcoin/pull/35676)
**Author:** [@maflcko](https://github.com/maflcko)
*(Activity: 16 review events this week)*
> This change aims to make Bitcoin Core more resilient, preventing rare issues that could lead to crashes or data problems when checking disk space or accessing files. It safeguards your Bitcoin data even under unusual system conditions.

#### [#35673: refactor: Move LoadGenesisBlock to ChainstateManager](https://github.com/bitcoin/bitcoin/pull/35673)
**Author:** [@maflcko](https://github.com/maflcko)
*(Activity: 12 review events this week)*
> We're making internal code improvements to better organize how the software handles the very first block of the blockchain.
This leads to a cleaner, more maintainable codebase and improves overall system architecture.

#### [#35691: chainparams: delete my DNS seed](https://github.com/bitcoin/bitcoin/pull/35691)
**Author:** [@sipa](https://github.com/sipa)
*(Activity: 11 review events this week)*
> A volunteer-provided address used to help new Bitcoin nodes find peers is being removed.
This reflects a natural evolution of network bootstrapping resources and decentralization.

## 🗣️ Research & Governance
Top active threads across mailing lists and research forums.

### [Re: Public key recovery for EC leaves in P2MR (BIP-360)](https://delvingbitcoin.org/t/public-key-recovery-for-ec-leaves-in-p2mr-bip-360/2603/24)
**Source:** Delving | **Started By:** conduition | **Messages:** 9
> Explores technical optimizations in the proposed Pay-to-Merkle-Root protocol to allow smaller transaction sizes by recovering public keys directly from cryptographic leaves.

### [Re: Addressing the Diminishing Block Subsidy](https://delvingbitcoin.org/t/addressing-the-diminishing-block-subsidy/2640/19)
**Source:** Delving | **Started By:** Sho | **Messages:** 6
> Bitcoin developers are engaging in a critical discussion about the network's long-term security as the reward for miners (the 'block subsidy') continues to decrease over time. The goal is to ensure Bitcoin remains robust and secure well into the future.

**Technical Details:** This discussion addresses the fundamental long-term economic challenge of Bitcoin's diminishing block subsidy. As the block reward halves over time and eventually approaches zero, the debate centers on how transaction fees will need to fully compensate miners to maintain network security and a robust hash rate against potential 51% attacks. Topics include modeling future fee markets, analyzing the economic sustainability of a fee-only miner revenue model, and exploring potential (though controversial) protocol adjustments to incentives. The architectural value is focused on ensuring the protocol's continued security budget and the stability of its economic foundation for decades to come.

## 🏆 Contributor Shoutouts
### ✍️ Top Authors
The most active PR authors this week: [@fanquake](https://github.com/fanquake), [@pablomartin4btc](https://github.com/pablomartin4btc), [@maflcko](https://github.com/maflcko), [@andrewtoth](https://github.com/andrewtoth), [@sipa](https://github.com/sipa)

### 🕵️ Top Reviewers
Providing critical review and testing: [@sedited](https://github.com/sedited), [@maflcko](https://github.com/maflcko), [@Sjors](https://github.com/Sjors), [@achow101](https://github.com/achow101), [@l0rinc](https://github.com/l0rinc)
