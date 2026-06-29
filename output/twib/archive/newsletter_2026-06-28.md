# This Week in Bitcoin (2026-06-22 to 2026-06-28)

> 📈 **This Week's Pulse:** **20** PRs Merged | **458** Review Events | **0** First-time Contributors | **33** Active Discussions

## The TL;DR
The complete removal of Libevent marks a major architectural shift to a modern asynchronous I/O model for the networking stack, improving performance, scalability, and maintainability.
Significant ongoing discussion around a "Segregated Data" block region suggests potential future protocol changes impacting block structure and data carriage, alongside continued research into advanced transaction types and privacy-enhancing techniques (e.g., P2MR).

## Core Code: Merged This Week
### 👛 Wallet & Keys
*   [#35266](https://github.com/bitcoin/bitcoin/pull/35266) **rpc, wallet: add an option to not load the wallet after migrating** (by [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa))
    * Users can now choose not to load a wallet immediately after an upgrade, offering more control over node startup. This helps manage resource usage and ensures wallets are only loaded when explicitly needed.
*   [#35601](https://github.com/bitcoin/bitcoin/pull/35601) **wallet: remove experimental warning from send and sendall** (by [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost))
    * The wallet's 'send' and 'sendall' features are now considered stable and reliable. You can use them with full confidence for your transactions.

### 📝 Documentation
*   [#35424](https://github.com/bitcoin/bitcoin/pull/35424) **doc, wallet: align external signer documentation, reject sendtoaddress/sendmany** (by [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx))
    * External signer functionality is now better documented, and certain wallet commands are blocked when using external signers. This improves security and clarity for users managing funds with hardware wallets.
*   [#35602](https://github.com/bitcoin/bitcoin/pull/35602) **doc: Clarify build docs about `pkgconf` / `pkg-config` requirements** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * The build instructions are now clearer about which tools are needed to compile Bitcoin Core. This makes it easier for new developers to get started.

### 🛠️ Build, CI & Testing
*   [#35543](https://github.com/bitcoin/bitcoin/pull/35543) **test: introduce ExtendedPrivateKey and ExtendedPublicKey classes** (by [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux))
    * We've added new internal tools to improve how we test features related to HD wallets and advanced key management. This helps ensure the security and correctness of future wallet developments.
*   [#35603](https://github.com/bitcoin/bitcoin/pull/35603) **build: QRencode cleanups** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * Minor improvements have been made to the QR code generation component, making it more efficient and tidier for developers.
*   [#35506](https://github.com/bitcoin/bitcoin/pull/35506) **test: ensure group data cluster pointers are live** (by [@instagibbs](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_greg_sanders))
    * This ensures that internal data structures used for transaction organization are correctly managed and don't lead to errors, making the system more stable.
*   [#35576](https://github.com/bitcoin/bitcoin/pull/35576) **test: raise `feature_reindex` RPC timeout** (by [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap))
    * The timeout for reindexing the blockchain in our automated tests has been increased. This makes our tests more reliable, especially on slower systems.
*   [#35595](https://github.com/bitcoin/bitcoin/pull/35595) **ci: remove some packages from Chimera job** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * We've streamlined our continuous integration testing environment by removing unnecessary packages. This makes our tests faster and more efficient.
*   [#35609](https://github.com/bitcoin/bitcoin/pull/35609) **ci: Bump tsan config to ubuntu:26.04 with -U_FORTIFY_SOURCE** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
    * Our automated tests for finding memory and threading issues are now running on a newer, more robust system. This helps us catch subtle bugs sooner.
*   [#35220](https://github.com/bitcoin/bitcoin/pull/35220) **fuzz: connman: strengthen assertions and extend coverage** (by [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia))
    * We've improved our automated testing for network connection management, making it even better at finding potential issues before they affect users.
*   [#35536](https://github.com/bitcoin/bitcoin/pull/35536) **fuzz: share a single mocked steady clock across FuzzedSock instances** (by [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu))
    * Our advanced automated tests now use a consistent time source when simulating network interactions. This helps us find time-sensitive bugs more accurately.
*   [#35452](https://github.com/bitcoin/bitcoin/pull/35452) **[30.x] 30.3rc1** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * This is a release candidate for Bitcoin Core version 30.3, preparing for an upcoming maintenance update.
*   [#35594](https://github.com/bitcoin/bitcoin/pull/35594) **fuzz: cover async chainstate compaction** (by [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap))
    * We've expanded our automated tests to cover background processes that optimize how blockchain data is stored, helping to prevent issues with disk usage.
*   [#35450](https://github.com/bitcoin/bitcoin/pull/35450) **[29.x] 29.4rc1** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * This is a release candidate for Bitcoin Core version 29.4, gearing up for an upcoming maintenance update.

### ⚡ P2P & Network
*   [#34411](https://github.com/bitcoin/bitcoin/pull/34411) **Full Libevent removal** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * Developers are actively working to completely remove the Libevent library, simplifying Bitcoin Core's code and reducing its external dependencies.
*   [#35588](https://github.com/bitcoin/bitcoin/pull/35588) **scripted-diff: Rename `Sock::{RECV,SEND,ERR}`** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
    * This is a code cleanup effort to rename internal network functions, making the codebase more consistent and easier for developers to understand.
*   [#35550](https://github.com/bitcoin/bitcoin/pull/35550) **net_processing: fix BIP152 first integer interpretation** (by [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia))
    * A subtle bug affecting how nodes communicate about new blocks (BIP152) has been fixed, improving network efficiency and reliability.

### 🛡️ Consensus & Cryptography
*   [#35070](https://github.com/bitcoin/bitcoin/pull/35070) **validation: prevent FindMostWorkChain from causing UB** (by [@stratospher](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stratospher))
    * A potential issue that could cause crashes or unexpected behavior in the blockchain validation logic has been fixed, making the node more robust.

### 🔄 Misc / Other
*   [#35403](https://github.com/bitcoin/bitcoin/pull/35403) **mining: pr 33966 followups (disentangle miner startup defaults)** (by [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost))
    * Improvements have been made to how the built-in miner starts up, making its configuration clearer and less prone to conflicts.

## Core Code: Under Review (Hot PRs)
### 🔄 Misc / Other
*   [#35587](https://github.com/bitcoin/bitcoin/pull/35587) **Remove boost as a unit test runner** (18 review events)
*   [#35588](https://github.com/bitcoin/bitcoin/pull/35588) **scripted-diff: Rename `Sock::{RECV,SEND,ERR}`** (17 review events)
*   [#34411](https://github.com/bitcoin/bitcoin/pull/34411) **Full Libevent removal** (16 review events)
*   [#35295](https://github.com/bitcoin/bitcoin/pull/35295) **validation: fetch block input prevouts in parallel during ConnectBlock** (14 review events)

### 👛 Wallet & Keys
*   [#35436](https://github.com/bitcoin/bitcoin/pull/35436) **wallet: Add addHDkey interface** (14 review events)

## Research & Governance
*   **[Delving]** [Re: [BIP Draft] Segregated Data: a prunable, script-isolated block region for data carriage](https://delvingbitcoin.org/t/bip-draft-segregated-data-a-prunable-script-isolated-block-region-for-data-carriage/2641/2) (24 messages)
    * Developers are discussing a new proposal to store non-financial data on the Bitcoin blockchain more efficiently. This could help keep the network lean by allowing certain data to be discarded later, reducing long-term storage needs.
*   **[Delving]** [Re: Addressing the Diminishing Block Subsidy](https://delvingbitcoin.org/t/addressing-the-diminishing-block-subsidy/2640/2) (16 messages)
    * Bitcoin developers are engaging in a critical discussion about the network's long-term security as the reward for miners (the 'block subsidy') continues to decrease over time. The goal is to ensure Bitcoin remains robust and secure well into the future.

## Contributor Shoutouts
A huge thanks to everyone who contributed code or reviewed PRs this week!

🥇 **Top Authors (Merged PRs)**
* [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford) (4), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov) (3), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost) (2), [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia) (2), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap) (2)

🏆 **Top Reviewers**
* [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung) (29), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke) (26), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap) (23), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov) (23), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost) (20)

**All Active Contributors:**
* [@0xB10C](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_0xb10c), [@8144225309](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_8144225309), [@achow101](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ava_chow), [@ajtowns](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_anthony_towns), [@alexanderwiederin](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_alexander_wiederin)
* [@alhudz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_alhudz), [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth), [@arejula27](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_igo_ar_jula_a_sa), [@b-l-u-e](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_b_l_u_e), [@BrandonOdiwuor](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_brandon_odiwuor)
* [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia), [@chriszeng1010](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_chris_z), [@Crypt-iQ](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_eugene_siegel), [@danielabrozzoni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniela_brozzoni), [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot)
* [@davidgumberg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov), [@dergoegge](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_niklas_g_gge), [@ekzyis](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_ekzyis), [@enirox001](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_enoch_azariah), [@Eunovo](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_oghenovo_usiwoma)
* [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford), [@fernandguil](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_guillermo_fernandes), [@fjahr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_fabian_jahr), [@frankomosh](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_frankomosh), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov)
* [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator), [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu), [@instagibbs](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_greg_sanders), [@janb84](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jan_b), [@Jewellorraine](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jewellorraine)
* [@johnnyasantoss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_johnnyasantoss), [@josibake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_oghenovo_usiwoma), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke), [@marcofleon](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_crackercracked)
* [@musaHaruna](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_musa_haruna), [@mzumsande](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_martin_zumsande), [@naiyoma](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_naiyoma), [@nervana21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_nervana21), [@optout21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_optout21)
* [@pablomartin4btc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pablo_martin), [@pinheadmz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matthew_zipkin), [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa), [@pseudoramdom](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pseudoramdom), [@ptrinh](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_ptrinh)
* [@purpleKarrot](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniel_pfeifer), [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux), [@rustaceanrob](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_robert_netzke), [@ryanofsky](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_russell_yanofsky), [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung)
* [@seduless](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_seduless), [@sipa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_pieter_wuille), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost), [@sr-gi](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sergi_delgado_segura), [@stickies-v](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stickies_v)
* [@stratospher](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stratospher), [@stringintech](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stringintech), [@stutxo](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stutxo), [@theStack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sebastian_falbesoner), [@vasild](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov)
* [@ViniciusCestarii](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_vinicius_cestari), [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx), [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark)