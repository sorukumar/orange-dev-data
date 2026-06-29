# This Week in Bitcoin (2026-06-08 to 2026-06-15)

> 📈 **This Week's Pulse:** **30** PRs Merged | **532** Review Events | **4** First-time Contributors | **21** Active Discussions

## The TL;DR
Advancements in P2P feature negotiation and ongoing discussions for protocol extensions:** The integration of BIP 434 marks a crucial step for flexible peer-to-peer feature adoption, while active discourse around novel covenant designs (like BIP-360's P2MR) and proposals to tighten Merkle tree preimage rules signals a forward-looking approach to Bitcoin's scripting capabilities and core consensus security.
Significant internal hardening, efficiency gains, and concurrency improvements:** Key efforts focused on enhancing the core daemon's reliability, including critical fixes for P2P integer overflows and RPC assertion crashes, crypto buffer cleansing for better security, and refactoring of block storage mutexes and index cache allocation for improved concurrency and resource management.

## Core Code: Merged This Week
### 🛠️ Build, CI & Testing
*   [#35179](https://github.com/bitcoin/bitcoin/pull/35179) **test: Add importdescriptors rpc error coverage** (by [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa))
*   [#35418](https://github.com/bitcoin/bitcoin/pull/35418) **build: exclude mptest target from compile commands** (by [@Sanjana2906](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sanjana2906))
*   [#35114](https://github.com/bitcoin/bitcoin/pull/35114) **test: NodeClockContext follow-ups** (by [@seduless](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_seduless))
*   [#35451](https://github.com/bitcoin/bitcoin/pull/35451) **lint: Grep for `AUTO` test suites in file names** (by [@rustaceanrob](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_robert_netzke))
*   [#35459](https://github.com/bitcoin/bitcoin/pull/35459) **guix: add setup.sh** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
*   [#35288](https://github.com/bitcoin/bitcoin/pull/35288) **ci: Bump toward Ubuntu 26.04** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
*   [#35458](https://github.com/bitcoin/bitcoin/pull/35458) **qa: Avoid extra tracebacks when exception is raised** (by [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator))
*   [#35497](https://github.com/bitcoin/bitcoin/pull/35497) **test: FakeNodeClock follow-ups in unit tests** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
*   [#35487](https://github.com/bitcoin/bitcoin/pull/35487) **scripted-diff: Rename UNIQUE_NAME to BITCOIN_UNIQUE_NAME** (by [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov))
*   [#35462](https://github.com/bitcoin/bitcoin/pull/35462) **test: remove unnecessary nodes from wallet_multisig_descriptor_psbt** (by [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux))
*   [#35456](https://github.com/bitcoin/bitcoin/pull/35456) **test: Perform full reset of CoinsResult in order to avoid passing 21M BTC** (by [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator))
*   [#35514](https://github.com/bitcoin/bitcoin/pull/35514) **ci: Alpine 3.24** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))
*   [#35448](https://github.com/bitcoin/bitcoin/pull/35448) **ci: don't build libunwind in msan** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))

### ⚡ P2P & Network
*   [#34028](https://github.com/bitcoin/bitcoin/pull/34028) **p2p: Prevent integer overflow in LocalServiceInfo::nScore** (by [@codeabysss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_codeabysss))
*   [#35297](https://github.com/bitcoin/bitcoin/pull/35297) **p2p: Release m_peer_mutex early in InitiateTxBroadcastToAll** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
*   [#35498](https://github.com/bitcoin/bitcoin/pull/35498) **net: move cs_main up in FetchBlock to fix rpc assert crash** (by [@Crypt-iQ](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_eugene_siegel))

### 📡 RPC, APIs & ZMQ
*   [#35267](https://github.com/bitcoin/bitcoin/pull/35267) **rpc: make getprivatebroadcastinfo and abortprivatebroadcast fail if privatebroadcast is not enabled** (by [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa))
*   [#35519](https://github.com/bitcoin/bitcoin/pull/35519) **rpc: tighten setmocktime upper bound to UINT32_MAX** (by [@stringintech](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stringintech))

### 🔄 Misc / Other
*   [#35101](https://github.com/bitcoin/bitcoin/pull/35101) **refactor: disable default std::hash for CTransactionRef** (by [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost))
*   [#35221](https://github.com/bitcoin/bitcoin/pull/35221) **BIP 434 Support: Peer feature negotiation** (by [@ajtowns](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_anthony_towns))
*   [#35254](https://github.com/bitcoin/bitcoin/pull/35254) **crypto: cleanse HMAC stack buffers after use and ChainCode** (by [@thomasbuilds](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_thomasbuilds))
*   [#35043](https://github.com/bitcoin/bitcoin/pull/35043) **refactor: Properly return from ThreadSafeQuestion signal + btcsignals follow-ups** (by [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke))
*   [#34636](https://github.com/bitcoin/bitcoin/pull/34636) **node: allocate index caches proportional to usage patterns** (by [@svanstaa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_van_staa))
*   [#35120](https://github.com/bitcoin/bitcoin/pull/35120) **btcsignals: delete broken scoped_connection move assignment** (by [@thomasbuilds](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_thomasbuilds))
*   [#35455](https://github.com/bitcoin/bitcoin/pull/35455) **fuzz: improve dbwrapper_concurrent_reads performance** (by [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth))
*   [#35168](https://github.com/bitcoin/bitcoin/pull/35168) **validation: Don't add pruned blocks to `m_blocks_unlinked` on startup** (by [@marcofleon](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_marcofleon))
*   [#35359](https://github.com/bitcoin/bitcoin/pull/35359) **blockstorage: Remove cs_LastBlockFile recursive mutex** (by [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung))
*   [#35478](https://github.com/bitcoin/bitcoin/pull/35478) **fuzz: reset the mockable steady clock between iterations** (by [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu))
*   [#35489](https://github.com/bitcoin/bitcoin/pull/35489) **fuzz: test non-max descriptor satisfaction weight** (by [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx))
*   [#35523](https://github.com/bitcoin/bitcoin/pull/35523) **Revert "build: exclude mptest target from compile commands"** (by [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford))

## Core Code: Under Review (Hot PRs)
### 🛠️ Build, CI & Testing
*   [#35179](https://github.com/bitcoin/bitcoin/pull/35179) **test: Add importdescriptors rpc error coverage** (18 review events)

### 🔄 Misc / Other
*   [#35465](https://github.com/bitcoin/bitcoin/pull/35465) **coins: compact chainstate regularly** (17 review events)
*   [#35482](https://github.com/bitcoin/bitcoin/pull/35482) **fuzz: exercise the transaction-handling path in process_message** (16 review events)
*   [#32427](https://github.com/bitcoin/bitcoin/pull/32427) **kernel: Replace leveldb-based BlockTreeDB with WAL and .dat file based store** (14 review events)

### 👛 Wallet & Keys
*   [#35405](https://github.com/bitcoin/bitcoin/pull/35405) **wallet: switch default optinrbf from true to false** (12 review events)

## Research & Governance
*   **[Delving]** [Re: Public key recovery for EC leaves in P2MR (BIP-360)](https://delvingbitcoin.org/t/public-key-recovery-for-ec-leaves-in-p2mr-bip-360/2603/16) (10 messages)
*   **[Delving]** [Re: A Bitcoin-native LLM: dataset, architecture and open questions](https://delvingbitcoin.org/t/a-bitcoin-native-llm-dataset-architecture-and-open-questions/2550/12) (6 messages)

## Contributor Shoutouts
A huge thanks to everyone who contributed code or reviewed PRs this week!

🎉 **Welcome First-Time Contributors!**
* [@Sanjana2906](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sanjana2906), [@codeabysss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_codeabysss), [@seduless](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_seduless), [@svanstaa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_van_staa)

🥇 **Top Authors (Merged PRs)**
* [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford) (4), [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke) (4), [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator) (2), [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa) (2), [@thomasbuilds](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_thomasbuilds) (2)

🏆 **Top Reviewers**
* [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke) (61), [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung) (39), [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx) (33), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov) (28), [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux) (27)

**All Active Contributors:**
* [@0xB10C](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_0xb10c), [@achow101](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_ava_chow), [@ajtowns](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_anthony_towns), [@alexanderwiederin](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_alexander_wiederin), [@alhudz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_alhudz)
* [@andrewtoth](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_andrew_toth), [@b-l-u-e](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_b_l_u_e), [@Bicaru20](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_bicaru20), [@BrandonOdiwuor](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_brandon_odiwuor), [@brunoerg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_bruno_garcia)
* [@carloantinarella](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_carloantinarella), [@codeabysss](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_codeabysss), [@Crypt-iQ](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_eugene_siegel), [@danielabrozzoni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_daniela_brozzoni), [@davidgumberg](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov)
* [@dergoegge](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_niklas_g_gge), [@DoumiLaville](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_doumilaville), [@ekzyis](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_ekzyis), [@enirox001](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_enoch_azariah), [@fanquake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_michael_ford)
* [@fjahr](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_fabian_jahr), [@frankomosh](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_frankomosh), [@furszy](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matias_furszyfer), [@gmaxwell](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_gregory_maxwell), [@hebasto](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hennadii_stepanov)
* [@hodlinator](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_hodlinator), [@HowHsu](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_hao_xu), [@instagibbs](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_greg_sanders), [@janb84](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jan_b), [@Jay-1409](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_jay_1409)
* [@jonatack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_jon_atack), [@josibake](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_oghenovo_usiwoma), [@jsarenik](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_j_n_s_ren_k), [@kwsantiago](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_kyle), [@l0rinc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_lorinc_pap)
* [@maflcko](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_marcofalke), [@marcofleon](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_marcofleon), [@mostafarahimifard](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_mostafarahimifard), [@musaHaruna](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_musa_haruna), [@mzumsande](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_martin_zumsande)
* [@nebula-21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rogersala21), [@optout21](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_optout21), [@opus-lux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_opus_lux), [@OSINTv96](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_osintv96), [@pablomartin4btc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pablomartin4btc)
* [@pinheadmz](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_matthew_zipkin), [@polespinasa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pol_espinasa), [@pseudoramdom](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_pseudoramdom), [@rkrux](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_rkrux), [@rustaceanrob](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_robert_netzke)
* [@ryanofsky](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_russell_yanofsky), [@Sanjana2906](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sanjana2906), [@satsfy](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_renato_britto), [@sedited](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_kung), [@seduless](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_seduless)
* [@sh011](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sh011), [@sipa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_pieter_wuille), [@Sjors](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sjors_provoost), [@stickies-v](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stickies_v), [@stratospher](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stratospher)
* [@stringintech](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_stringintech), [@svanstaa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_sebastian_van_staa), [@takeshikurosawaa](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_takeshi_kurosawa), [@theStack](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_sebastian_falbesoner), [@theuni](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_cory_fields)
* [@thomasbuilds](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_thomasbuilds), [@vasild](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=can_vasil_dimov), [@w0xlt](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_woltx), [@willcl-ark](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_will_clark), [@winterrdog](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_winterrdog)
* [@yuvicc](https://sorukumar.github.io/orange-dev-network/profile.html?uuid=auto_i_am_yuvi)