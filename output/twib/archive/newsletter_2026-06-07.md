# This Week in Bitcoin (2026-05-31 to 2026-06-07)

## The TL;DR
*   The project is strategically preparing for significant future protocol upgrades and potential new consensus rules, highlighted by the reservation of BIP 323 nonce space and active discussions regarding the need for a new Testnet 5.
*   There's a continued focus on core component efficiency, with major coin selection optimizations significantly improving wallet performance, alongside ongoing research and development into critical network-level privacy enhancements like P2MR.

## Core Code: Merged This Week
### 🛠️ Build, CI & Testing
*   [#35466](https://github.com/bitcoin/bitcoin/pull/35466) **ci: run ipc functional tests in arm job** (by fanquake)
    * This PR enables running inter-process communication (IPC) functional tests on ARM architecture jobs in continuous integration.
*   [#35431](https://github.com/bitcoin/bitcoin/pull/35431) **test: pass -datadir to bitcoin-cli -ipcconnect check** (by mjdietzx)
    * This PR modifies tests to correctly pass the data directory argument to `bitcoin-cli` when checking IPC connections.
*   [#35446](https://github.com/bitcoin/bitcoin/pull/35446) **ci: use pyzmq over zmq** (by fanquake)
    * This PR updates the continuous integration system to use `pyzmq` instead of the older `zmq` library.
*   [#35447](https://github.com/bitcoin/bitcoin/pull/35447) **ci: use warpbuild cache for docker buildkit cache** (by willcl-ark)
    * This PR integrates Warpbuild's caching mechanism to improve Docker BuildKit caching within the continuous integration environment.
*   [#35439](https://github.com/bitcoin/bitcoin/pull/35439) **test: Improve loopback address check in `rpc_bind.py`** (by xyzconstant)
    * This PR enhances the loopback address verification within the `rpc_bind.py` test script.

### ⚡ P2P & Network
*   [#35410](https://github.com/bitcoin/bitcoin/pull/35410) **net: use the proxy if overriden when doing v2->v1 reconnections** (by vasild)
    * This PR ensures that the configured proxy is utilized during network reconnections when transitioning from v2 to v1 protocols.

### 🔄 Misc / Other
*   [#32150](https://github.com/bitcoin/bitcoin/pull/32150) **coinselection: Optimize BnB exploration** (by murchandamus)
    * This PR introduces optimizations to the Branch and Bound (BnB) algorithm used for coin selection.
*   [#35335](https://github.com/bitcoin/bitcoin/pull/35335) **Make deployment configuration available outside of regtest in unit tests** (by darosior)
    * This PR makes deployment configurations accessible in unit tests beyond just the regtest environment.
*   [#34779](https://github.com/bitcoin/bitcoin/pull/34779) **BIP 323: reserve version bits 5-28 as extra nonce space** (by darosior)
    * This PR proposes to reserve specific version bits (5-28) as additional nonce space, as outlined in BIP 323.
*   [#34866](https://github.com/bitcoin/bitcoin/pull/34866) **fuzz: target concurrent leveldb reads** (by andrewtoth)
    * This PR adds a fuzzer target to test concurrent read operations on LevelDB.
*   [#35232](https://github.com/bitcoin/bitcoin/pull/35232) **[30.x] Backports** (by fanquake)
    * This PR bundles several backported changes for the 30.x release branch.
*   [#35234](https://github.com/bitcoin/bitcoin/pull/35234) **[29.x] Backports** (by fanquake)
    * This PR bundles several backported changes for the 29.x release branch.
*   [#35269](https://github.com/bitcoin/bitcoin/pull/35269) **musig: Include pubnonce in session id** (by achow101)
    * This PR modifies MuSig to incorporate the public nonce into the session identifier.

### 🖥️ GUI
*   [#34767](https://github.com/bitcoin/bitcoin/pull/34767) **Bugfix: GUI/Intro: Handle errors from SelectParams the same as if during InitConfig** (by luke-jr)
    * This bugfix ensures that errors from `SelectParams` in the GUI's intro screen are handled consistently with `InitConfig` errors.

### 👛 Wallet & Keys
*   [#35192](https://github.com/bitcoin/bitcoin/pull/35192) **wallet: unfriend LegacyDataSPKM and DescriptorScriptPubKeyMan** (by rkrux)
    * This PR aims to reduce dependencies or tightly coupled interactions between `LegacyDataSPKM` and `DescriptorScriptPubKeyMan` in the wallet.

## Core Code: Under Review (Hot PRs)
### 🔄 Misc / Other
*   [#34866](https://github.com/bitcoin/bitcoin/pull/34866) **fuzz: target concurrent leveldb reads** (38 review events)
*   [#32427](https://github.com/bitcoin/bitcoin/pull/32427) **kernel: Replace leveldb-based BlockTreeDB with flat-file based store** (35 review events)
*   [#35295](https://github.com/bitcoin/bitcoin/pull/35295) **validation: fetch block input prevouts in parallel during ConnectBlock** (32 review events)

### 🛠️ Build, CI & Testing
*   [#35418](https://github.com/bitcoin/bitcoin/pull/35418) **build: exclude mptest target from compile commands** (27 review events)

### 📡 RPC, APIs & ZMQ
*   [#34683](https://github.com/bitcoin/bitcoin/pull/34683) **rpc: support a formal description of our JSON-RPC interface** (27 review events)

## Research & Governance
*   **[Mailing List]** [Re: [bitcoindev] [BIP Draft] Testnet 5](https://gnusha.org/pi/bitcoindev/aiKYWu7Osn5hIJFP@erisian.com.au) (8 messages)
    * This discussion revolves around a draft BIP concerning the proposal for a new Testnet 5.
*   **[Mailing List]** [Re: [bitcoindev] Prohibit Merkle Internal Node Preimages That Encode
 Minimal 64-Byte Transactions](https://gnusha.org/pi/bitcoindev/zsUBtB2e_nQ-rup8cdIacq139Y1FznGRLQfq8XQ2lM-6SZNk3Kfucj2pxvX0YQ0QW1G2liAhenj8xYBFGqvzGLvtwZYFE5r1Xo2Y91O_Mz8=@protonmail.com) (7 messages)
    * This discussion addresses the proposal to disallow Merkle internal node preimages that encode minimal 64-byte transactions.
*   **[Delving]** [Re: Default signet reorg history — was the fork at height 287766 (depth 19) deliberate?](https://delvingbitcoin.org/t/default-signet-reorg-history-was-the-fork-at-height-287766-depth-19-deliberate/2591/7) (6 messages)
    * This discussion investigates whether a specific reorg event on Signet at height 287766 was intentional.
*   **[Mailing List]** [Re: [bitcoindev] Aligning privacy incentives in P2MR](https://gnusha.org/pi/bitcoindev/26684d8a-cdb3-43dd-a5b4-fc607ed95e8bn@googlegroups.com) (6 messages)
    * This discussion explores methods to better align privacy incentives within the P2MR (Peer-to-Peer Message Relay) protocol.
*   **[Delving]** [Re: Bird of Prey 2: non-malleable Schnorr + PQ signatures](https://delvingbitcoin.org/t/bird-of-prey-2-non-malleable-schnorr-pq-signatures/2514/8) (5 messages)
    * This discussion is about "Bird of Prey 2," focusing on non-malleable Schnorr and post-quantum (PQ) signatures.

## Contributor Shoutouts
A huge thanks to everyone who contributed code or reviewed PRs this week:
* 151henry151, 8144225309, Ap4sh, Bicaru20, Copilot
* Crypt-iQ, D33r-Gee, Sanjana2906, Sjors, achow101
* ajtowns, alhudz, andrewtoth, average-gary, b-l-u-e
* bitcoin, brunoerg, cobratbq, codeabysss, danielabrozzoni
* darosior, dergoegge, digin1, edilmedeiros, ekzyis
* enirox001, fanquake, fjahr, frankomosh, furszy
* hebasto, hodlinator, hulxv, instagibbs, ismaelsadeeq
* janb84, johnnyasantoss, josibake, kevkevinpal, l0rinc
* luke-jr, m3dwards, maflcko, marcofleon, mjdietzx
* murchandamus, mzumsande, naiyoma, nervana21, oleonardolima
* optout21, pablomartin4btc, pinheadmz, polespinasa, pseudoramdom
* purpleKarrot, pzafonte, quadcpu, real-or-random, rkrux
* rustaceanrob, ryanofsky, satsfy, sedited, seduless
* sh011, sipa, stickies-v, stratospher, sys-dev
* theStack, vasild, w0xlt, willcl-ark, winterrdog
* xyzconstant, yancyribbens