# This Week in Bitcoin (2026-06-02 to 2026-06-09)

> 📈 **This Week's Pulse:** **13** PRs Merged | **725** Review Events | **0** First-time Contributors | **36** Active Discussions

## The TL;DR
*   The protocol has been extended with BIP 323, reserving transaction version bits 5-28 for extra nonce space, which is critical for future block template construction and mining protocol evolution.
*   Significant discussions are ongoing regarding network-layer privacy improvements (P2MR) and refining the robustness of Signet, our primary test network, to better support complex feature development and testing.

## Core Code: Merged This Week
### 🛠️ Build, CI & Testing
*   [#35466](https://github.com/bitcoin/bitcoin/pull/35466) **ci: run ipc functional tests in arm job** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * This change expands our test suite to ensure inter-process communication (IPC) features function correctly on ARM-based systems.
*   [#35431](https://github.com/bitcoin/bitcoin/pull/35431) **test: pass -datadir to bitcoin-cli -ipcconnect check** (by [@mjdietzx](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_michael_dietz))
    * This improves the reliability of our IPC functional tests by correctly specifying the data directory for `bitcoin-cli`.
*   [#35446](https://github.com/bitcoin/bitcoin/pull/35446) **ci: use pyzmq over zmq** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * We've updated our continuous integration to use a more robust ZMQ library, improving build stability for developers.
*   [#35447](https://github.com/bitcoin/bitcoin/pull/35447) **ci: use warpbuild cache for docker buildkit cache** (by [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark))
    * This update optimizes our CI build times by leveraging a more efficient caching system, speeding up development workflows.
*   [#35335](https://github.com/bitcoin/bitcoin/pull/35335) **Make deployment configuration available outside of regtest in unit tests** (by [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot))
    * This change enhances testing flexibility by making deployment configurations accessible for unit tests beyond regtest, aiding soft fork development.
*   [#34866](https://github.com/bitcoin/bitcoin/pull/34866) **fuzz: target concurrent leveldb reads** (by [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth))
    * We've enhanced our fuzz testing to target concurrent LevelDB reads, which helps uncover potential database stability issues.

### ⚡ P2P & Network
*   [#35410](https://github.com/bitcoin/bitcoin/pull/35410) **net: use the proxy if overriden when doing v2->v1 reconnections** (by [@vasild](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov))
    * This fix ensures that overridden proxy settings are correctly applied during specific network reconnections, improving privacy and network configuration for operators.

### 🖥️ GUI
*   [#34767](https://github.com/bitcoin/bitcoin/pull/34767) **Bugfix: GUI/Intro: Handle errors from SelectParams the same as if during InitConfig** (by [@luke-jr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_luke_dashjr))
    * This bugfix improves the GUI's robustness by consistently handling network parameter selection errors during startup.

### 👛 Wallet & Keys
*   [#32150](https://github.com/bitcoin/bitcoin/pull/32150) **coinselection: Optimize BnB exploration** (by [@murchandamus](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_mark_erhardt))
    * This optimization to coin selection improves wallet efficiency, potentially leading to better fee estimations or UTXO management for users.

### 🛡️ Consensus & Cryptography
*   [#34779](https://github.com/bitcoin/bitcoin/pull/34779) **BIP 323: reserve version bits 5-28 as extra nonce space** (by [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot))
    * This update reserves additional version bits as extra nonce space, a foundational step for future advanced scripting capabilities like covenants.
*   [#35269](https://github.com/bitcoin/bitcoin/pull/35269) **musig: Include pubnonce in session id** (by [@achow101](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ava_chow))
    * This change strengthens MuSig security and determinism by including the public nonce in the session ID for multi-signature schemes.

### 🔄 Misc / Other
*   [#35232](https://github.com/bitcoin/bitcoin/pull/35232) **[30.x] Backports** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * This pulls a set of important bug fixes and minor improvements into the upcoming 30.x release branch.
*   [#35234](https://github.com/bitcoin/bitcoin/pull/35234) **[29.x] Backports** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
    * This integrates essential bug fixes and minor improvements into the current 29.x stable release branch.

## Core Code: Under Review (Hot PRs)
### 🔄 Misc / Other
*   [#32427](https://github.com/bitcoin/bitcoin/pull/32427) **kernel: Replace leveldb-based BlockTreeDB with flat-file based store** (27 review events)
*   [#33540](https://github.com/bitcoin/bitcoin/pull/33540) **argsman, cli: GNU-style command-line option parsing (allows options after non-option arguments)** (25 review events)
*   [#35465](https://github.com/bitcoin/bitcoin/pull/35465) **validation: compact chainstate after IBD and post-IBD flushes** (23 review events)
*   [#35295](https://github.com/bitcoin/bitcoin/pull/35295) **validation: fetch block input prevouts in parallel during ConnectBlock** (23 review events)

### 📡 RPC, APIs & ZMQ
*   [#34683](https://github.com/bitcoin/bitcoin/pull/34683) **rpc: support a formal description of our JSON-RPC interface** (27 review events)

## Research & Governance
*   **[Delving]** [Re: Default signet reorg history — was the fork at height 287766 (depth 19) deliberate?](https://delvingbitcoin.org/t/default-signet-reorg-history-was-the-fork-at-height-287766-depth-19-deliberate/2591/7) (6 messages)
    * This thread discusses a specific reorg event on the default Signet at height 287766, questioning whether this depth 19 reorg was a deliberate action taken by the maintainers or an unexpected network occurrence.
*   **[Mailing List]** [Re: [bitcoindev] Aligning privacy incentives in P2MR](https://gnusha.org/pi/bitcoindev/26684d8a-cdb3-43dd-a5b4-fc607ed95e8bn@googlegroups.com) (6 messages)
    * This discussion focuses on strategies to better align privacy incentives for nodes participating in P2MR (likely Peer-to-Peer Mempool Relay) to improve overall privacy guarantees for users within the Bitcoin network.

## Contributor Shoutouts
A huge thanks to everyone who contributed code or reviewed PRs this week!

🥇 **Top Authors (Merged PRs)**
* [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford) (4), [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot) (2), [@achow101](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ava_chow) (1), [@luke-jr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_luke_dashjr) (1), [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth) (1)

🏆 **Top Reviewers**
* [@sedited](https://github.com/sedited) (71), [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford) (50), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap) (35), [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_w0xlt) (34), [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark) (32)

**All Active Contributors:**
* [@151henry151](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_henry_romp), [@8144225309](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_8144225309), [@achow101](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ava_chow), [@ajtowns](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_anthony_towns), [@alhudz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_alhudz)
* [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth), [@Ap4sh](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_ap4sh), [@b-l-u-e](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_b_l_u_e), [@Bicaru20](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_bicaru20), [@bitcoin](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_bitcoin)
* [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia), [@codeabysss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_codeabysss), [@Copilot](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_copilot), [@Crypt-iQ](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_eugene_siegel), [@D33r-Gee](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_d33r_gee)
* [@danielabrozzoni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniela_brozzoni), [@darosior](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_antoine_poinsot), [@dergoegge](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_niklas_g_gge), [@digin1](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_digin1), [@edilmedeiros](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jos_edil_guimar_es_de_medeiros)
* [@ekzyis](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_ekzyis), [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford), [@fjahr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_fabian_jahr), [@frankomosh](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_frankomosh), [@furszy](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matias_furszyfer)
* [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov), [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator), [@hulxv](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hulxv), [@instagibbs](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_greg_sanders), [@ismaelsadeeq](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_abubakar_sadiq_ismail)
* [@janb84](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jan_b), [@johnnyasantoss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_johnnyasantoss), [@josibake](https://github.com/josibake), [@kevkevinpal](https://github.com/kevkevinpal), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap)
* [@luke-jr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_luke_dashjr), [@m3dwards](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_max_edwards), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke), [@marcofleon](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_marcofleon), [@mjdietzx](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_michael_dietz)
* [@murchandamus](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_mark_erhardt), [@mzumsande](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_martin_zumsande), [@naiyoma](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_naiyoma), [@nervana21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_nervana21), [@oleonardolima](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_oleonardolima)
* [@optout21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_optout21), [@pablomartin4btc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pablomartin4btc), [@pinheadmz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matthew_zipkin), [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa), [@pseudoramdom](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pseudoramdom)
* [@purpleKarrot](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniel_pfeifer), [@pzafonte](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_peter_zafonte), [@quadcpu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_quadcpu), [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux), [@rustaceanrob](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_robert_netzke)
* [@ryanofsky](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ryan_ofsky), [@Sanjana2906](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sanjana2906), [@satsfy](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_renato_britto), [@sedited](https://github.com/sedited), [@seduless](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_seduless)
* [@sipa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_pieter_wuille), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost), [@stickies-v](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stickies_v), [@theStack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sebastian_falbesoner), [@vasild](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov)
* [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_w0xlt), [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark), [@winterrdog](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_winterrdog), [@yancyribbens](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_yancy_ribbens)