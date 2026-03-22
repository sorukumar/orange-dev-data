#!/usr/bin/env python3
"""
Categorize Threads in Bitcoin Social Data

This script analyzes threads in the combined social data to:
- Identify BIP references (e.g., BIP 141, BIP-340)
- Assign rich, multi-label categories that tell the story of Bitcoin's
  technical evolution from 2011 to the present.

Categories are designed so that someone browsing by category can trace
Bitcoin's history: from early P2SH and payment-protocol debates, through
the block-size wars, SegWit activation, Schnorr/Taproot, the Ordinals
controversy, covenant proposals, quantum resistance, and beyond.

It processes threads by aggregating content, applying keyword/regex
matching with priority scoring, and updates the dataset with new fields:
  - bip_refs        (list[str])  – BIP numbers referenced
  - category        (str)        – single best-fit category
  - categories      (list[str])  – all matching categories
  - category_conf   (float)      – confidence score 0-1 for primary

Usage:
    python scripts/analysis/categorize_threads.py
"""

import pandas as pd
import re
import os
import json

# ── paths ────────────────────────────────────────────────────────────
INPUT_PARQUET = "data/raw/social_combined.parquet"
OUTPUT_PARQUET = "data/raw/social_combined_categorized.parquet"

# =====================================================================
# CATEGORY DEFINITIONS
# =====================================================================
# Each category has:
#   keywords  – plain lowercase substrings (fast first pass)
#   patterns  – compiled regexes for precision (second pass)
#   weight    – tie-breaker when multiple categories match; higher = more
#               specific and therefore preferred as primary category
#   bips      – BIP numbers that auto-assign this category
#   desc      – short human-readable description
# =====================================================================

_CATEGORY_DEFS: dict = {

    # ── Consensus & Soft/Hard Fork Mechanics ─────────────────────────
    "soft-fork-activation": {
        "desc": "Soft fork activation mechanisms (BIP 9, BIP 8, Speedy Trial, UASF, flag day)",
        "keywords": ["soft fork", "softfork", "uasf", "user activated",
                      "speedy trial", "flag day", "lot=true", "lot=false",
                      "version bits", "signaling", "bip148", "bip149",
                      "bip91", "activation"],
        "patterns": [
            r"\b(?:soft[\s\-]?fork)\s+activation\b",
            r"\bspeedy\s+trial\b",
            r"\bflag[\s\-]?day\b",
            r"\buser[\s\-]?activated\b",
            r"\bversion[\s\-]?bits\b",
            r"\bsignaling\b.*(?:threshold|miner|block)",
        ],
        "weight": 70,
        "bips": ["8", "9", "91", "135", "148", "149", "343"],
    },

    "hard-fork-block-size": {
        "desc": "Block size debate & hard fork proposals (2015-2017 era and beyond)",
        "keywords": ["block size", "blocksize", "block weight", "2mb",
                      "8mb", "20mb", "segwit2x", "bitcoin xt",
                      "bitcoin classic", "bitcoin unlimited", "bip100",
                      "bip101", "bip102", "bip103", "bip109",
                      "new york agreement", "hong kong agreement"],
        "patterns": [
            r"\bblock[\s\-]?size\b",
            r"\bblock[\s\-]?weight\b",
            r"\b(?:segwit)?2x\b",
            r"\bbitcoin[\s\-]?(?:xt|classic|unlimited)\b",
            r"\b(?:20|2|8)\s*mb\b",
            r"\bhard[\s\-]?fork\b.*\b(?:size|block|capacity|increase)\b",
        ],
        "weight": 75,
        "bips": ["100", "101", "102", "103", "104", "105", "106", "107", "109"],
    },

    "consensus-cleanup": {
        "desc": "Great Consensus Cleanup & related consensus-level fixes",
        "keywords": ["consensus cleanup", "great consensus cleanup",
                      "timewarp", "64-byte transaction",
                      "duplicate transaction", "merkle tree vulnerability"],
        "patterns": [
            r"\bconsensus\s+cleanup\b",
            r"\btimewarp\b",
            r"\b64[\s\-]?byte\s+transaction\b",
        ],
        "weight": 72,
        "bips": ["30", "53", "54"],
    },

    # ── SegWit ───────────────────────────────────────────────────────
    "segwit": {
        "desc": "Segregated Witness design, deployment, and consequences",
        "keywords": ["segwit", "segregated witness", "witness program",
                      "witness version", "bech32", "malleability",
                      "transaction malleability", "anyone-can-spend"],
        "patterns": [
            r"\bseg[\s\-]?wit\b",
            r"\bsegregated\s+witness\b",
            r"\bbech32(?:m)?\b",
            r"\bwitness\s+(?:program|version|discount)\b",
        ],
        "weight": 65,
        "bips": ["141", "142", "143", "144", "145", "147", "148", "149",
                 "173", "350"],
    },

    # ── Taproot / Schnorr ────────────────────────────────────────────
    "taproot": {
        "desc": "Taproot, Schnorr signatures, Tapscript",
        "keywords": ["taproot", "schnorr", "tapscript", "bip340",
                      "bip341", "bip342", "mast",
                      "merkelized abstract syntax tree",
                      "key path spend", "script path spend",
                      "annex"],
        "patterns": [
            r"\btaproot\b",
            r"\bschnorr\b",
            r"\btapscript\b",
            r"\bmast\b",
            r"\bkey[\s\-]?path\b",
            r"\bscript[\s\-]?path\b",
        ],
        "weight": 68,
        "bips": ["114", "340", "341", "342", "343", "386"],
    },

    # ── Covenants & Introspection Opcodes ────────────────────────────
    "covenants": {
        "desc": "Covenant proposals: CTV, OP_CAT, OP_VAULT, TXHASH, APO, CSFS, CHECKSIGFROMSTACK, OP_CHECKCONTRACTVERIFY",
        "keywords": ["covenant", "op_checktemplateverify", "op_ctv",
                      "checktemplateverify", "bip119",
                      "op_cat", "op_vault", "op_txhash",
                      "anyprevout", "sighash_anyprevout",
                      "checksigfromstack", "csfs", "op_checksigfromstackverify",
                      "op_internalkey", "op_paircommit",
                      "op_checkcontractverify", "op_ccv",
                      "introspection", "lnhance",
                      "graftleaf", "op_expire"],
        "patterns": [
            r"\bcovenant[s]?\b",
            r"\bop[\s_]c(?:tv|at|cv)\b",
            r"\bop[\s_]vault\b",
            r"\bop[\s_]txhash\b",
            r"\bop[\s_]checksigfromstack(?:verify)?\b",
            r"\bop[\s_]checktemplateverify\b",
            r"\bop[\s_]internalkey\b",
            r"\bop[\s_]paircommit\b",
            r"\bop[\s_]checkcontractverify\b",
            r"\banyprevout\b",
            r"\bcsfs\b",
            r"\blnhance\b",
            r"\bgraftleaf\b",
        ],
        "weight": 80,
        "bips": ["118", "119", "345", "346", "347", "348", "349", "443"],
    },

    # ── Script & Opcodes (general) ───────────────────────────────────
    "script-opcodes": {
        "desc": "Bitcoin Script, opcodes, Simplicity, Miniscript",
        "keywords": ["opcode", "op_eval", "p2sh", "pay to script hash",
                      "op_return", "miniscript", "simplicity",
                      "script interpreter", "script validation",
                      "sigop", "op_cltv", "op_csv",
                      "checklocktimeverify", "checksequenceverify",
                      "op_if", "op_success", "script restoration"],
        "patterns": [
            r"\bop_[a-z_]+\b",
            r"\bp2sh\b",
            r"\bminiscript\b",
            r"\bsimplicity\b",
            r"\bscript\s+(?:interpreter|validation|flag|engine|execution)\b",
        ],
        "weight": 40,
        "bips": ["12", "16", "17", "18", "62", "65", "66", "68", "98",
                 "112", "113", "116", "117", "379"],
    },

    # ── Ordinals & Inscriptions ──────────────────────────────────────
    "ordinals-inscriptions": {
        "desc": "Ordinal theory, Inscriptions, BRC-20, digital artifacts on Bitcoin",
        "keywords": ["ordinal", "inscription", "inscribe",
                      "brc-20", "brc20", "digital artifact",
                      "sat number", "ordinal number",
                      "inscriptionless", "rare sat"],
        "patterns": [
            r"\bordinal(?:s|\.)\b",
            r"\binscription[s]?\b",
            r"\bbrc[\s\-]?20\b",
            r"\bdigital\s+artifact\b",
            r"\brare\s+sat[s]?\b",
        ],
        "weight": 82,
        "bips": [],
    },

    # ── Runes & Fungible Tokens on Bitcoin ───────────────────────────
    "tokens-runes": {
        "desc": "Runes protocol, colored coins, Counterparty, Omni, RGB, Taro/Taproot Assets, tokenization on Bitcoin",
        "keywords": ["runes protocol", "rune protocol", "runestone",
                      "colored coin", "counterparty", "mastercoin",
                      "omni layer", "taro asset", "taproot asset",
                      "rgb protocol", "rgb consensus",
                      "lrc-20", "tokenization", "fungible token"],
        "patterns": [
            r"\brunes?\s+protocol\b",
            r"\brunestone\b",
            r"\bcolored\s+coin[s]?\b",
            r"\bcounterparty\b",
            r"\bomni\s+layer\b",
            r"\btaproot\s+asset[s]?\b",
            r"\btaro\b(?!\w)",
            r"\brgb\s+(?:protocol|consensus|contract|yellow)\b",
            r"\blrc[\s\-]?20\b",
        ],
        "weight": 82,
        "bips": [],
    },

    # ── Lightning Network ────────────────────────────────────────────
    "lightning": {
        "desc": "Lightning Network: channels, HTLCs, routing, LN-Symmetry (eltoo), watchtowers, LSPs",
        "keywords": ["lightning", "ln ", "htlc", "payment channel",
                      "channel capacity", "routing", "eltoo",
                      "ln-symmetry", "watchtower",
                      "bolt11", "bolt12", "bolt ",
                      "lsp ", "lightning service provider",
                      "submarine swap", "splicing",
                      "onion message", "onion routing",
                      "trampoline", "blinded path",
                      "channel jamming", "channel depletion"],
        "patterns": [
            r"\blightning\s+(?:network|channel|payment|node|invoice|wallet)\b",
            r"\bhtlc\b",
            r"\bln[\s\-]symmetry\b",
            r"\beltoo\b",
            r"\bbolt[\s\-]?(?:11|12)\b",
            r"\bpayment\s+channel[s]?\b",
            r"\bsplicing\b",
            r"\bchannel\s+(?:jamming|depletion|capacity|open|close|factory)\b",
            r"\bsubmarine\s+swap\b",
            r"\bwatchtower\b",
            r"\bonion\s+(?:message|routing)\b",
        ],
        "weight": 60,
        "bips": [],
    },

    # ── Privacy ──────────────────────────────────────────────────────
    "privacy": {
        "desc": "Privacy techniques: CoinJoin, PayJoin, CoinSwap, confidential transactions, dandelion",
        "keywords": ["coinjoin", "payjoin", "coinswap", "bustapay",
                      "confidential transaction", "mixer", "mixing",
                      "dandelion", "privacy", "fungibility",
                      "stealth address", "reusable payment code",
                      "bip47", "paynym"],
        "patterns": [
            r"\bcoin[\s\-]?join\b",
            r"\bpay[\s\-]?join\b",
            r"\bcoin[\s\-]?swap\b",
            r"\bdandelion\b",
            r"\bstealth\s+address\b",
            r"\breusable\s+payment\s+code\b",
            r"\bprivacy\b",
            r"\bfungib(?:ility|le)\b",
        ],
        "weight": 62,
        "bips": ["47", "78", "79", "126", "156"],
    },

    # ── Silent Payments ──────────────────────────────────────────────
    "silent-payments": {
        "desc": "Silent Payments (BIP-352): static payment addresses without address reuse",
        "keywords": ["silent payment", "bip352", "bip-352", "bip 352"],
        "patterns": [
            r"\bsilent\s+payment[s]?\b",
        ],
        "weight": 78,
        "bips": ["351", "352", "375"],
    },

    # ── Wallet, Keys & Descriptors ───────────────────────────────────
    "wallet-keys": {
        "desc": "Wallet standards: HD wallets, BIP39 mnemonics, descriptors, PSBTs, seed backups, key management",
        "keywords": ["hd wallet", "hierarchical deterministic",
                      "mnemonic", "bip39", "bip32", "bip44", "bip43",
                      "seed phrase", "seed word",
                      "descriptor", "output descriptor",
                      "psbt", "partially signed",
                      "key derivation", "xpub", "zpub", "ypub",
                      "codex32", "shamir",
                      "wallet label", "wallet backup",
                      "wallet policy", "multisig wallet"],
        "patterns": [
            r"\bhd\s+wallet[s]?\b",
            r"\bhierarchical\s+deterministic\b",
            r"\bmnemonic\b",
            r"\bseed\s+(?:phrase|word|backup)\b",
            r"\bdescriptor[s]?\b",
            r"\bpsbt\b",
            r"\bpartially\s+signed\b",
            r"\bcodex32\b",
            r"\bkey\s+derivat(?:ion|e)\b",
            r"\boutput\s+descriptor\b",
        ],
        "weight": 50,
        "bips": ["32", "38", "39", "43", "44", "45", "46", "48", "49",
                 "67", "69", "83", "84", "85", "86", "87", "88", "89",
                 "93", "124", "129", "174", "329", "370", "371", "372",
                 "373", "374", "380", "381", "382", "383", "384", "385",
                 "386", "387", "388", "389", "390"],
    },

    # ── Multisig & Threshold Cryptography ────────────────────────────
    "multisig-threshold": {
        "desc": "Multisig, MuSig2, FROST, threshold signatures, key aggregation",
        "keywords": ["multisig", "multi-sig", "musig", "musig2",
                      "frost signing", "frost key", "chilldkg",
                      "key aggregation", "threshold signature",
                      "schnorr multisig", "n-of-m", "m-of-n"],
        "patterns": [
            r"\bmulti[\s\-]?sig\b",
            r"\bmusig[2]?\b",
            r"\bfrost\b",
            r"\bchilldkg\b",
            r"\bthreshold\s+sign(?:ature|ing)\b",
            r"\bkey\s+aggregation\b",
            r"\b[nmk][\s\-]?of[\s\-]?[nmk]\b",
        ],
        "weight": 64,
        "bips": ["11", "45", "48", "67", "87", "327", "328", "373", "390"],
    },

    # ── Mining ───────────────────────────────────────────────────────
    "mining": {
        "desc": "Mining: PoW, ASICs, pools, block templates, Stratum, selfish mining, fee sniping",
        "keywords": ["mining", "miner", "hashrate", "proof of work",
                      "asicboost", "selfish mining", "block template",
                      "getblocktemplate", "stratum", "mining pool",
                      "block withholding", "fee sniping",
                      "coinbase transaction", "nonce", "share",
                      "braidpool", "radpool", "ocean pool",
                      "pplns", "job declaration"],
        "patterns": [
            r"\bmining\b(?!\s+(?:data|the))",
            r"\bminer[s]?\b",
            r"\bhashrate\b",
            r"\basicboost\b",
            r"\bselfish\s+mining\b",
            r"\bstratum\s+v?[12]?\b",
            r"\bblock\s+template\b",
            r"\bgetblocktemplate\b",
            r"\bmining\s+pool\b",
            r"\bpplns\b",
            r"\bbraidpool\b",
            r"\bradpool\b",
        ],
        "weight": 55,
        "bips": ["22", "23", "34", "42", "52", "310", "320"],
    },

    # ── Mempool & Fee Management ─────────────────────────────────────
    "mempool-fees": {
        "desc": "Mempool policy, RBF, CPFP, fee estimation, package relay, cluster mempool, pinning, V3/TRUC",
        "keywords": ["mempool", "replace by fee", "replace-by-fee",
                      "rbf", "full-rbf", "full rbf", "cpfp",
                      "child pays for parent", "fee estimation",
                      "fee rate", "feerate", "package relay",
                      "cluster mempool", "linearization",
                      "pinning", "tx pinning", "transaction pinning",
                      "v3 transaction", "truc", "ephemeral anchor",
                      "pay to anchor", "p2a",
                      "ancestor package", "descendant limit",
                      "mempool policy", "relay policy",
                      "min relay fee", "dust limit", "dust threshold"],
        "patterns": [
            r"\bmempool\b",
            r"\breplace[\s\-]?by[\s\-]?fee\b",
            r"\b(?:full[\s\-]?)?rbf\b",
            r"\bcpfp\b",
            r"\bfee\s+(?:estimation|estimator|rate|bump|snip)\b",
            r"\bpackage\s+relay\b",
            r"\bcluster\s+mempool\b",
            r"\blinearization\b",
            r"\b(?:tx|transaction)\s+pinning\b",
            r"\bv3\s+transaction[s]?\b",
            r"\btruc\b",
            r"\bephemeral\s+anchor[s]?\b",
            r"\bp2a\b",
            r"\bdust\s+(?:limit|threshold|attack)\b",
        ],
        "weight": 58,
        "bips": ["125", "133", "331", "431", "433"],
    },

    # ── P2P Network ──────────────────────────────────────────────────
    "p2p-network": {
        "desc": "P2P protocol, Erlay, BIP324 encrypted transport, addr relay, compact blocks, DNS seeds",
        "keywords": ["p2p protocol", "peer to peer",
                      "erlay", "reconciliation",
                      "bip324", "v2 transport", "encrypted transport",
                      "compact block", "addrv2", "addr relay",
                      "dns seed", "node discovery",
                      "eclipse attack", "bgp",
                      "sendheaders", "feefilter",
                      "bloom filter", "bip37",
                      "compact filter", "bip157", "bip158",
                      "neutrino"],
        "patterns": [
            r"\bp2p\s+(?:protocol|network|transport|relay)\b",
            r"\berlay\b",
            r"\bbip[\s\-]?324\b",
            r"\bv2\s+(?:p2p|transport)\b",
            r"\bencrypted\s+transport\b",
            r"\bcompact\s+block[s]?\b",
            r"\baddr(?:v2)?\s+(?:relay|message)\b",
            r"\bdns\s+seed\b",
            r"\beclipse\s+attack\b",
            r"\bbloom\s+filter\b",
            r"\bcompact\s+(?:block\s+)?filter[s]?\b",
            r"\bneutrino\b",
        ],
        "weight": 52,
        "bips": ["14", "31", "33", "35", "36", "37", "60", "61",
                 "111", "130", "133", "150", "151", "152", "154",
                 "155", "156", "157", "158", "159", "180",
                 "324", "330", "338", "339", "434"],
    },

    # ── Sighash & Signatures ─────────────────────────────────────────
    "signatures-sighash": {
        "desc": "Signature schemes, sighash types, ECDSA, DER encoding",
        "keywords": ["sighash", "signature hash", "ecdsa",
                      "der encoding", "signature verification",
                      "sighash_single", "sighash_all", "sighash_none",
                      "sighash_noinput", "sighash_anyprevout"],
        "patterns": [
            r"\bsighash\b",
            r"\becdsa\b",
            r"\bder\s+(?:encoding|signature)\b",
            r"\bsignature\s+(?:verification|validation|encoding|scheme)\b",
        ],
        "weight": 45,
        "bips": ["66", "143", "146", "340"],
    },

    # ── Vaults ───────────────────────────────────────────────────────
    "vaults": {
        "desc": "Bitcoin vaults: custody, clawback, time-delayed spending, OP_VAULT",
        "keywords": ["vault", "clawback", "unvaulting", "custody",
                      "cold storage", "time-delayed", "recovery path"],
        "patterns": [
            r"\bvault[s]?\b",
            r"\bclawback\b",
            r"\bunvault(?:ing)?\b",
            r"\bcold\s+storage\b",
        ],
        "weight": 66,
        "bips": ["345"],
    },

    # ── DLCs (Discreet Log Contracts) ────────────────────────────────
    "dlc": {
        "desc": "Discreet Log Contracts, oracle-based contracts on Bitcoin",
        "keywords": ["discreet log contract", "dlc ", "dlc-dev",
                      "oracle contract", "dlc oracle"],
        "patterns": [
            r"\bdiscreet\s+log\s+contract[s]?\b",
            r"\bdlc[s]?\b(?!\w)",
        ],
        "weight": 74,
        "bips": ["374"],
    },

    # ── Sidechains & Drivechain ──────────────────────────────────────
    "sidechains-drivechain": {
        "desc": "Sidechains, Drivechain (BIP 300/301), Liquid, federated pegs, merged mining",
        "keywords": ["sidechain", "drivechain", "hashrate escrow",
                      "blind merged mining", "bip300", "bip301",
                      "federated peg", "two-way peg", "liquid network",
                      "liquid sidechain"],
        "patterns": [
            r"\bsidechain[s]?\b",
            r"\bdrivechain[s]?\b",
            r"\bhashrate\s+escrow\b",
            r"\bblind\s+merged\s+mining\b",
            r"\bfederated\s+peg\b",
            r"\btwo[\s\-]?way\s+peg\b",
            r"\bliquid\s+(?:network|sidechain)\b",
        ],
        "weight": 72,
        "bips": ["300", "301"],
    },

    # ── BitVM & Computation ──────────────────────────────────────────
    "bitvm": {
        "desc": "BitVM, BitVMX, off-chain computation verification, fraud proofs, validity proofs, STARKs on Bitcoin",
        "keywords": ["bitvm", "bitvmx", "fraud proof",
                      "validity proof", "validity rollup",
                      "stark", "snark", "zero knowledge",
                      "zk proof", "zkcp",
                      "garbled circuit"],
        "patterns": [
            r"\bbitvm[x2]?\b",
            r"\bfraud\s+proof[s]?\b",
            r"\bvalidity\s+(?:proof|rollup)[s]?\b",
            r"\bstark[s]?\b",
            r"\bsnark[s]?\b",
            r"\bzero[\s\-]?knowledge\b",
            r"\bzk[\s\-]?(?:proof|verify|gossip|statechain|rollup)\b",
            r"\bgarbled\s+circuit[s]?\b",
        ],
        "weight": 76,
        "bips": [],
    },

    # ── L2, Rollups & Bridges ────────────────────────────────────────
    "l2-bridges": {
        "desc": "L2 protocols, rollups, Ark, statechains, bridges, channel factories, colliderVM",
        "keywords": ["rollup", "statechain", "ark protocol",
                      "channel factory", "collidervm",
                      "bridge covenant", "l2 protocol",
                      "superscalar", "timeout tree",
                      "unilateral exit", "aggregate exit"],
        "patterns": [
            r"\brollup[s]?\b",
            r"\bstatechain[s]?\b",
            r"\bark\s+(?:protocol|case)\b",
            r"\bchannel\s+factor(?:y|ies)\b",
            r"\bcollidervm\b",
            r"\bsuperscalar\b",
            r"\btimeout[\s\-]?tree\b",
            r"\bunilateral\s+exit\b",
            r"\bl2\s+(?:protocol|pool|bridge)\b",
        ],
        "weight": 68,
        "bips": [],
    },

    # ── Quantum Resistance ───────────────────────────────────────────
    "quantum": {
        "desc": "Post-quantum cryptography, quantum resistance, P2QRH, hash-based signatures, Lamport, FALCON, SPHINCS",
        "keywords": ["quantum", "post-quantum", "post quantum",
                      "p2qrh", "lamport signature",
                      "hash-based signature", "hash based signature",
                      "falcon signature", "falcon post-quantum",
                      "sphincs", "winternitz",
                      "grover", "shor algorithm"],
        "patterns": [
            r"\bquantum\b",
            r"\bpost[\s\-]?quantum\b",
            r"\bp2qrh\b",
            r"\blamport\b",
            r"\bfalcon\b",
            r"\bsphincs\b",
            r"\bwinternitz\b",
            r"\bshor(?:'s)?\s+algorithm\b",
        ],
        "weight": 78,
        "bips": ["360"],
    },

    # ── Ecash & Chaumian ─────────────────────────────────────────────
    "ecash": {
        "desc": "Chaumian ecash, Cashu, Fedimint, blind signatures, mints",
        "keywords": ["ecash", "e-cash", "cashu", "fedimint",
                      "chaumian", "blind signature",
                      "mint ecash", "fedi"],
        "patterns": [
            r"\becash\b",
            r"\bcashu\b",
            r"\bfedimint\b",
            r"\bchaumian\b",
            r"\bblind\s+signature[s]?\b",
        ],
        "weight": 74,
        "bips": [],
    },

    # ── Spam, Censorship & OP_RETURN Debate ──────────────────────────
    "spam-filtering": {
        "desc": "Spam, censorship, OP_RETURN limits, standardness rules, inscription controversy",
        "keywords": ["spam", "censor", "censorship", "op_return limit",
                      "standardness", "non-standard",
                      "datacarrier", "data carrier",
                      "bare multisig", "dust attack"],
        "patterns": [
            r"\bspam\b",
            r"\bcensor(?:ship|ing)?\b",
            r"\bop_return\s+(?:limit|size|standard|restrict|relay)\b",
            r"\bstandardness\b",
            r"\bnon[\s\-]?standard\b",
            r"\bdatacarrier\b",
        ],
        "weight": 56,
        "bips": [],
    },

    # ── Payment Protocols & URIs ─────────────────────────────────────
    "payment-protocol": {
        "desc": "Payment protocol (BIP 70-75), bitcoin: URIs, BIP21, BOLT11/12 invoices, DNS payment instructions",
        "keywords": ["payment protocol", "payment request",
                      "bip70", "bip 70", "bip21", "bip 21",
                      "bitcoin uri", "bitcoin:", "x509",
                      "dns payment instruction",
                      "human readable", "offer"],
        "patterns": [
            r"\bpayment\s+protocol\b",
            r"\bpayment\s+request\b",
            r"\bbitcoin[\s\-]?uri\b",
            r"\bdns\s+payment\b",
            r"\bhuman[\s\-]?readable\s+(?:payment|address|bitcoin)\b",
        ],
        "weight": 54,
        "bips": ["20", "21", "70", "71", "72", "73", "74", "75", "321", "353"],
    },

    # ── Atomic Swaps & Cross-Chain ───────────────────────────────────
    "atomic-swaps": {
        "desc": "Atomic swaps, HTLCs for swaps, cross-chain, SAS, submarine swaps",
        "keywords": ["atomic swap", "cross-chain", "cross chain",
                      "succinct atomic swap", "hash time-locked",
                      "htlc swap"],
        "patterns": [
            r"\batomic\s+swap[s]?\b",
            r"\bcross[\s\-]?chain\b",
            r"\bsuccinct\s+atomic\b",
        ],
        "weight": 62,
        "bips": ["197", "199"],
    },

    # ── Security & Vulnerability Disclosure ──────────────────────────
    "security": {
        "desc": "Security vulnerabilities, CVEs, responsible disclosure, DoS attacks",
        "keywords": ["cve-", "vulnerability", "disclosure",
                      "exploit", "attack vector", "dos attack",
                      "denial of service", "double spend",
                      "replacement cycling", "free relay",
                      "eclipse attack", "bgp interception",
                      "finney attack", "time dilation"],
        "patterns": [
            r"\bcve[\s\-]\d+\b",
            r"\bvulnerabilit(?:y|ies)\b",
            r"\b(?:full|responsible)\s+disclosure\b",
            r"\bexploit\b",
            r"\bdouble[\s\-]?spend\b",
            r"\breplacement\s+cycling\b",
            r"\bfree[\s\-]?relay\b",
            r"\beclipse\s+attack\b",
            r"\bfinney\s+attack\b",
        ],
        "weight": 64,
        "bips": [],
    },

    # ── Testing, Signet & Devtools ───────────────────────────────────
    "testing-devtools": {
        "desc": "Signet, testnet, regtest, Bitcoin Inquisition, fuzzing, debugging tools, development infrastructure",
        "keywords": ["signet", "testnet", "regtest",
                      "bitcoin inquisition", "inquisition",
                      "fuzzing", "mutation testing",
                      "property-based testing",
                      "debugger", "tapsim", "btcdeb"],
        "patterns": [
            r"\bsignet\b",
            r"\btestnet\s*[34]?\b",
            r"\bregtest\b",
            r"\binquisition\b",
            r"\bfuzz(?:ing|er)\b",
            r"\bmutation\s+testing\b",
        ],
        "weight": 48,
        "bips": ["325", "94"],
    },

    # ── UTXO Set & IBD ───────────────────────────────────────────────
    "utxo-sync": {
        "desc": "UTXO set management, AssumeUTXO, Utreexo, pruning, IBD, SwiftSync",
        "keywords": ["assumeutxo", "utreexo", "utxo set",
                      "utxo commitment", "initial block download",
                      "ibd", "pruning", "pruned node",
                      "swiftsync", "witnessless sync"],
        "patterns": [
            r"\bassumeutxo\b",
            r"\butreexo\b",
            r"\butxo\s+(?:set|commitment|snapshot|database)\b",
            r"\binitial\s+block\s+download\b",
            r"\bibd\b",
            r"\bswiftsync\b",
            r"\bwitnessless\s+sync\b",
        ],
        "weight": 56,
        "bips": [],
    },

    # ── Bitcoin Core Release & Build ─────────────────────────────────
    "core-dev": {
        "desc": "Bitcoin Core releases, build system, repository governance, code review",
        "keywords": ["bitcoin core release", "bitcoin core v",
                      "release candidate", "build system",
                      "cmake", "automake", "guix",
                      "commit access", "merge policy",
                      "repository", "gitlab backup"],
        "patterns": [
            r"\bbitcoin\s+core\s+(?:v?\d|release)\b",
            r"\brelease\s+candidate\b",
            r"\bbuild\s+system\b",
            r"\bguix\b",
            r"\bcommit\s+access\b",
        ],
        "weight": 44,
        "bips": [],
    },

    # ── Nostr Integration ────────────────────────────────────────────
    "nostr": {
        "desc": "Nostr protocol integration with Bitcoin: relay-based payments, transaction relay over Nostr",
        "keywords": ["nostr"],
        "patterns": [
            r"\bnostr\b",
        ],
        "weight": 60,
        "bips": [],
    },

    # ── Scaling (general, beyond block size) ─────────────────────────
    "scaling": {
        "desc": "Scaling discussions: throughput, capacity, batching, aggregation, scalability research",
        "keywords": ["scaling", "scalability", "throughput",
                      "capacity increase", "batching",
                      "signature aggregation", "cross-input",
                      "cisa"],
        "patterns": [
            r"\bscaling\b",
            r"\bscalabilit(?:y|ies)\b",
            r"\bthroughput\b",
            r"\bcisa\b",
            r"\bcross[\s\-]?input\s+(?:signature\s+)?aggregation\b",
        ],
        "weight": 42,
        "bips": [],
    },

    # ── BIP Process & Governance ─────────────────────────────────────
    "bip-process": {
        "desc": "BIP editorial process, governance, community coordination, soft fork philosophy",
        "keywords": ["bip process", "bip acceptance", "bip editor",
                      "bip purpose", "bip classification",
                      "governance", "ossification",
                      "consensus change", "soft fork philosophy"],
        "patterns": [
            r"\bbip\s+(?:process|acceptance|editor|purpose|classification)\b",
            r"\bgovernance\b",
            r"\bossification\b",
            r"\bconsensus\s+change\b",
        ],
        "weight": 46,
        "bips": ["1", "2", "3", "123"],
    },

    # ── Transaction Format & IDs ─────────────────────────────────────
    "transaction-format": {
        "desc": "Transaction format, txid, wtxid, normalized txid, transaction compression",
        "keywords": ["transaction format", "txid", "wtxid",
                      "normalized txid", "flexible transaction",
                      "transaction compression", "compressed transaction"],
        "patterns": [
            r"\btxid\b",
            r"\bwtxid\b",
            r"\bnormalized\s+txid\b",
            r"\btransaction\s+(?:format|compression)\b",
        ],
        "weight": 38,
        "bips": ["131", "134", "136", "140", "337", "339"],
    },

    # ── Merkle Trees & Data Structures ───────────────────────────────
    "data-structures": {
        "desc": "Merkle trees, MATT, Merkleize All The Things, accumulators, commitments",
        "keywords": ["merkle tree", "merkle branch", "merkle proof",
                      "matt framework", "merkleize all",
                      "accumulator", "commitment scheme"],
        "patterns": [
            r"\bmerkle\s+(?:tree|branch|proof|root)\b",
            r"\bmatt\s+framework\b",
            r"\bmerkleize\b",
            r"\baccumulator\b",
        ],
        "weight": 36,
        "bips": ["98"],
    },

    # Catch-all ────────────────────────────────────────────────────────
    "other": {
        "desc": "Uncategorized – does not match any specific topic",
        "keywords": [],
        "patterns": [],
        "weight": 0,
        "bips": [],
    },
}

# ── Build fast lookup structures ─────────────────────────────────────

# Compile all regex patterns once
_COMPILED_PATTERNS: dict = {}
for cat, defn in _CATEGORY_DEFS.items():
    _COMPILED_PATTERNS[cat] = [
        re.compile(p, re.IGNORECASE) for p in defn.get("patterns", [])
    ]

# BIP-number → set of categories
_BIP_TO_CATEGORIES: dict = {}
for cat, defn in _CATEGORY_DEFS.items():
    for bip in defn.get("bips", []):
        _BIP_TO_CATEGORIES.setdefault(bip, set()).add(cat)


# =====================================================================
# CORE FUNCTIONS
# =====================================================================

def extract_bips(text: str) -> list[str]:
    """Extract BIP references from text using regex.

    Handles: BIP 141, BIP-141, BIP141, bip0141, BIP #141
    Returns de-duplicated list of BIP number strings (no leading zeros).
    """
    if not text:
        return []
    pattern = r'\bBIP[\s\-#]*0*(\d{1,4})\b'
    matches = re.findall(pattern, text, re.IGNORECASE)
    return sorted(set(matches), key=lambda x: int(x))


def score_categories(text: str, bip_refs: list[str]) -> dict[str, float]:
    """Return {category: score} for every category with score > 0.

    Scoring:
    1. Each keyword hit   → +1
    2. Each regex hit     → +2 (more precise)
    3. Each BIP match     → +3 (very reliable signal)
    4. Multiply total by category weight / 100 to prefer specific cats.
    """
    if not text:
        return {}

    text_lower = text.lower()
    scores: dict[str, float] = {}

    for cat, defn in _CATEGORY_DEFS.items():
        if cat == "other":
            continue

        raw = 0.0

        # keyword hits
        for kw in defn["keywords"]:
            if kw in text_lower:
                raw += 1

        # regex hits
        for pat in _COMPILED_PATTERNS[cat]:
            if pat.search(text):
                raw += 2

        # BIP hits
        for bip in bip_refs:
            if bip in defn.get("bips", []):
                raw += 3

        if raw > 0:
            weight = defn.get("weight", 50)
            scores[cat] = raw * (weight / 100.0)

    return scores


def categorize_thread(text: str, bip_refs: list[str]
                      ) -> tuple[str, list[str], float]:
    """Return (primary_category, all_categories, confidence).

    Confidence is normalized score of primary vs total.
    """
    scores = score_categories(text, bip_refs)
    if not scores:
        return "other", ["other"], 0.0

    sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    primary = sorted_cats[0][0]
    primary_score = sorted_cats[0][1]

    # All categories that scored at least 20% of the primary score
    threshold = primary_score * 0.20
    all_cats = [c for c, s in sorted_cats if s >= threshold]

    # Confidence: how dominant is the primary?
    total = sum(s for _, s in sorted_cats)
    confidence = round(primary_score / total, 3) if total > 0 else 0.0

    return primary, all_cats, confidence


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 60)
    print("  Categorize Threads – Bitcoin Social Data")
    print("=" * 60)

    if not os.path.exists(INPUT_PARQUET):
        print(f"ERROR: Input file {INPUT_PARQUET} not found.")
        return

    print(f"\nLoading {INPUT_PARQUET} ...")
    df = pd.read_parquet(INPUT_PARQUET)
    print(f"  {len(df):,} messages loaded.")

    # ── Build thread-level aggregated text ───────────────────────────
    thread_groups = df.groupby("thread_id")
    n_threads = len(thread_groups)
    print(f"  {n_threads:,} unique threads.\n")

    print("Categorizing threads ...")

    # Pre-build thread text + compute results
    thread_results: dict = {}   # thread_id → (bips, primary, all_cats, conf)
    done = 0

    for thread_id, group in thread_groups:
        texts = []
        for _, row in group.iterrows():
            subj = str(row.get("subject") or "")
            body = str(row.get("body_snippet") or "")
            texts.append(subj)
            texts.append(body)

        combined = " ".join(texts)

        bips = extract_bips(combined)
        primary, all_cats, conf = categorize_thread(combined, bips)
        thread_results[thread_id] = (bips, primary, all_cats, conf)

        done += 1
        if done % 2000 == 0 or done == n_threads:
            print(f"  {done:>6,} / {n_threads:,} threads processed")

    # ── Map results back to every message row ────────────────────────
    print("\nMapping results to messages ...")
    bip_col = []
    cat_col = []
    cats_col = []
    conf_col = []

    for _, row in df.iterrows():
        tid = row["thread_id"]
        bips, primary, all_cats, conf = thread_results.get(
            tid, ([], "other", ["other"], 0.0)
        )
        bip_col.append(bips)
        cat_col.append(primary)
        cats_col.append(all_cats)
        conf_col.append(conf)

    df["bip_refs"] = bip_col
    df["category"] = cat_col
    df["categories"] = cats_col
    df["category_conf"] = conf_col

    # ── Save ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PARQUET) or ".", exist_ok=True)
    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\nSaved → {OUTPUT_PARQUET}")

    # ── Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  CATEGORIZATION SUMMARY")
    print("=" * 60)

    # Unique thread-level stats
    thread_df = pd.DataFrame([
        {"thread_id": tid, "bip_refs": r[0], "category": r[1],
         "categories": r[2], "category_conf": r[3]}
        for tid, r in thread_results.items()
    ])

    print(f"\n  Total threads:          {len(thread_df):,}")
    has_bips = (thread_df["bip_refs"].apply(len) > 0).sum()
    print(f"  Threads with BIP refs:  {has_bips:,}  "
          f"({100*has_bips/len(thread_df):.1f}%)")

    other_count = (thread_df["category"] == "other").sum()
    print(f"  Categorized (non-other):{len(thread_df) - other_count:,}  "
          f"({100*(len(thread_df)-other_count)/len(thread_df):.1f}%)")
    print(f"  Uncategorized (other):  {other_count:,}  "
          f"({100*other_count/len(thread_df):.1f}%)")

    print(f"\n  {'Category':<28s} {'Threads':>8s}  {'%':>6s}")
    print("  " + "-" * 46)
    cat_counts = thread_df["category"].value_counts()
    for cat, count in cat_counts.items():
        pct = 100 * count / len(thread_df)
        desc = _CATEGORY_DEFS.get(cat, {}).get("desc", "")[:40]
        print(f"  {cat:<28s} {count:>8,}  {pct:>5.1f}%")

    # Multi-label stats
    multi = (thread_df["categories"].apply(len) > 1).sum()
    print(f"\n  Threads with 2+ categories: {multi:,}")

    print("\nDone.")


if __name__ == "__main__":
    main()