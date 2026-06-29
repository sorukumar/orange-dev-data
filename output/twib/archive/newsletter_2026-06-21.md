# This Week in Bitcoin (2026-06-15 to 2026-06-21)

> 📈 **This Week's Pulse:** **16** PRs Merged | **218** Review Events | **1** First-time Contributors | **21** Active Discussions

## The TL;DR
Intense research and discussion around **novel blockchain scalability techniques**, particularly Fountain Codes, aiming to fundamentally reduce storage requirements and improve overall network efficiency.
Deep technical dives into **advanced transaction privacy schemes**, specifically Pay-to-Mutually-Recoverable (P2MR) outputs and associated public key recovery mechanisms, indicating significant future directions for on-chain privacy enhancements.

## Core Code: Merged This Week
### 🛠️ Build, CI & Testing
*   [#1860](https://github.com/bitcoin/bitcoin/pull/1860) **cmake: Emulate Libtool's behavior on NetBSD and OpenBSD** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * Improves compatibility for building Bitcoin Core on NetBSD and OpenBSD systems. This ensures a smoother setup process for developers and users on these platforms.
*   [#35441](https://github.com/bitcoin/bitcoin/pull/35441) **ci: inline runner selection** (by [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark))
    * Streamlines how our automated testing system selects specific machines to run tests. This makes our continuous integration process more efficient and reliable.
*   [#35503](https://github.com/bitcoin/bitcoin/pull/35503) **guix: CMake-related improvements** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * Enhances the Guix build system for Bitcoin Core, making it more robust and reliable. This helps ensure reproducible builds for greater security and trust.
*   [#35520](https://github.com/bitcoin/bitcoin/pull/35520) **lint: remove redundant test suite uniqueness check** (by [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap))
    * Optimizes our development tools by removing an unnecessary check, making our code validation process slightly faster.
*   [#35396](https://github.com/bitcoin/bitcoin/pull/35396) **ci: Rewrite broken wrap-valgrind.sh to .py** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
    * Upgrades our Valgrind memory-checking tool from a shell script to Python. This improves the reliability of our automated memory error detection, helping us find and fix bugs faster.
*   [#35547](https://github.com/bitcoin/bitcoin/pull/35547) **lint: Require scripted-diff script to succeed** (by [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator))
    * Strengthens our code quality checks by ensuring automated code formatting scripts are applied correctly. This helps keep our codebase consistent and clean.
*   [#35526](https://github.com/bitcoin/bitcoin/pull/35526) **ci: bump MSan fuzz timeout from 150 to 180 minutes** (by [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost))
    * Increases the time allowed for our memory safety tests (MSan fuzzing) to run. This gives our testing more opportunity to find subtle memory bugs, making Bitcoin Core more secure.
*   [#35540](https://github.com/bitcoin/bitcoin/pull/35540) **test: descriptor: bare multisig at TOP level with exactly 3 pubkeys is allowed** (by [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia))
    * Improves the flexibility and correctness of our descriptor system by allowing a specific type of multi-signature setup directly. This makes it easier to define certain wallet configurations.
*   [#35463](https://github.com/bitcoin/bitcoin/pull/35463) **depends: Drop trailing slash from `CMAKE_INSTALL_LIBDIR`** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * Fixes a minor detail in our build process related to how libraries are installed. This resolves potential issues and ensures cleaner installation paths.
*   [#35546](https://github.com/bitcoin/bitcoin/pull/35546) **ci: Use GCC consistently in i686 task** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
    * Ensures our automated tests for older 32-bit systems consistently use the GCC compiler. This helps maintain stable and reliable builds for these specific environments.
*   [#35538](https://github.com/bitcoin/bitcoin/pull/35538) **test: make TestChain100Setup's m_clock timestamp more readable** (by [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu))
    * Improves the readability of timestamps in our testing environment. This makes it easier for developers to understand test results and debug issues related to time.
*   [#1865](https://github.com/bitcoin/bitcoin/pull/1865) **test: enable -Wunused-function in test suite (Fix #1831)** (by [@kallal79](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_kallal79))
    * Activates a compiler warning to catch unused functions in our test code. This helps developers identify and remove dead code, making our test suite cleaner and more maintainable.
*   [#1867](https://github.com/bitcoin/bitcoin/pull/1867) **test: musig: fix dead "aggnonce encodes two points at infinity" check** (by [@theStack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sebastian_falbesoner))
    * Corrects a specific check in our MuSig signature implementation tests. This ensures our cryptographic tests are fully accurate and reliable, contributing to the security of advanced Bitcoin features.

### 🛡️ Consensus & Cryptography
*   [#1859](https://github.com/bitcoin/bitcoin/pull/1859) **field: force-inline 5x52 mul and sqr** (by [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap))
    * Optimizes critical cryptographic calculations by making them run faster. This improves the overall performance of Bitcoin's cryptographic operations.

### 🔄 Misc / Other
*   [#35173](https://github.com/bitcoin/bitcoin/pull/35173) **util: shorten thread names to avoid Linux truncation** (by [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap))
    * Improves how system tools display information about Bitcoin Core's internal operations. This makes it easier for developers to monitor and debug the software.
*   [#35470](https://github.com/bitcoin/bitcoin/pull/35470) **argsman: Prevent duplicate option registration across categories** (by [@pablomartin4btc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pablo_martin))
    * Prevents potential conflicts in how Bitcoin Core processes command-line arguments and configuration settings. This makes the software more robust and reliable.

## Core Code: Under Review (Hot PRs)
### 🔄 Misc / Other
*   [#1859](https://github.com/bitcoin/bitcoin/pull/1859) **add LOCK() for proxy related data-structures** (13 review events)
*   [#1859](https://github.com/bitcoin/bitcoin/pull/1859) **field: force-inline 5x52 mul and sqr** (13 review events)
*   [#35173](https://github.com/bitcoin/bitcoin/pull/35173) **util: shorten thread names to avoid Linux truncation** (11 review events)
*   [#35496](https://github.com/bitcoin/bitcoin/pull/35496) **kernel: add `btck_set_mock_time` for testing time-dependent paths** (8 review events)
*   [#35511](https://github.com/bitcoin/bitcoin/pull/35511) **RFC: consensus: Make `CAmount` a class** (7 review events)
*   [#35531](https://github.com/bitcoin/bitcoin/pull/35531) **txindex: use 5-byte siphash keys to optimize disk usage** (7 review events)

## Research & Governance
*   **[Delving]** [Re: Fountain Codes: a way to reduce blockchain storage costs](https://delvingbitcoin.org/t/fountain-codes-a-way-to-reduce-blockchain-storage-costs/2624/2) (8 messages)
    * Developers are discussing a new technology called "Fountain Codes" that could significantly reduce the amount of storage needed for the Bitcoin blockchain. This research aims to make running a full node more accessible and cost-effective in the future.
*   **[Delving]** [Re: State of the transaction privacy work in Bitcoin](https://delvingbitcoin.org/t/state-of-the-transaction-privacy-work-in-bitcoin/2622/2) (8 messages)
    * Developers are actively discussing the current status and future directions of work to improve transaction privacy on the Bitcoin network. This continuous effort aims to make Bitcoin transactions more confidential for users.

## Contributor Shoutouts
A huge thanks to everyone who contributed code or reviewed PRs this week!

🎉 **Welcome First-Time Contributors!**
* [@kallal79](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_kallal79)

🥇 **Top Authors (Merged PRs)**
* [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov) (3), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap) (3), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke) (2), [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu) (1), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost) (1)

🏆 **Top Reviewers**
* [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke) (21), [@ryanofsky](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_russell_yanofsky) (15), [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford) (15), [@real-or-random](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_tim_ruffing) (14), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov) (14)

**All Active Contributors:**
* [@ajtowns](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_anthony_towns), [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth), [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia), [@danielabrozzoni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniela_brozzoni), [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot)
* [@edilmedeiros](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jos_edil_guimar_es_de_medeiros), [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford), [@furszy](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matias_furszyfer), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov), [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator)
* [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu), [@instagibbs](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_greg_sanders), [@ismaelsadeeq](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_abubakar_sadiq_ismail), [@janb84](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jan_b), [@Jay-1409](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jay_1409)
* [@josibake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_oghenovo_usiwoma), [@kallal79](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_kallal79), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap), [@m3dwards](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_max_edwards), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke)
* [@mzumsande](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_martin_zumsande), [@nervana21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_nervana21), [@optout21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_optout21), [@pablomartin4btc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pablo_martin), [@pseudoramdom](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pseudoramdom)
* [@real-or-random](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_tim_ruffing), [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux), [@ryanofsky](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_russell_yanofsky), [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung), [@ShauryaaSharma](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_shauryaasharma)
* [@sipa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_pieter_wuille), [@siv2r](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sivaram_dhakshinamoorthy), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost), [@stickies-v](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stickies_v), [@stringintech](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stringintech)
* [@theStack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sebastian_falbesoner), [@theuni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_cory_fields), [@tobtoht](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_tobtoht), [@vasild](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov), [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx)
* [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark), [@winterrdog](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_winterrdog)