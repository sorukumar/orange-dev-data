import json
import os

cache_file = 'data/raw/pr_summaries_cache.json'
with open(cache_file, 'r') as f:
    cache = json.load(f)

new_summaries = {
  "33391": {
    "public_summary": "This update prevents unnecessary disk space warnings during node initialization tests, making the testing process smoother and less noisy for developers.",
    "technical_summary": "This PR modifies the `node_init_tests` functional testing framework to mock or bypass disk space checks during initialization. By preventing these spurious warnings in constrained CI environments, it improves test reliability and reduces log noise without compromising the integrity of the initialization logic."
  },
  "31969": {
    "public_summary": "This update enables the assumeutxo feature for Bitcoin mainnet at block 880,000, allowing new full nodes to synchronize significantly faster. By providing a secure, pre-computed UTXO set snapshot, it drastically reduces initial sync times.",
    "technical_summary": "This PR integrates a new assumeutxo chain parameter entry into the mainnet CChainParams definition at block height 880,000. It specifies the cryptographic hash of a validated UTXO set snapshot, enabling nodes to leverage the existing assumeutxo framework for trust-minimized fast initial synchronization."
  },
  "33407": {
    "public_summary": "This change ensures that the instructional manual pages (manpages) for the Bitcoin Core executable are automatically installed on your system when you build the software. This makes it easier for users to look up command-line help directly from their terminal.",
    "technical_summary": "This PR updates the CMake build system configuration to include the `bitcoin` manpage in the installation targets. It adds the appropriate `install()` directives within the `CMakeLists.txt` file for the man directory, ensuring that manual pages are correctly deployed alongside the compiled binaries during `make install`."
  },
  "33283": {
    "public_summary": "This update refreshes the list of fixed 'seed nodes' built into Bitcoin Core. This ensures that when a new node first connects to the network, it can quickly and reliably find other peers to download the blockchain from.",
    "technical_summary": "This PR updates the hardcoded DNS seeds and fixed IP address seeds in `chainparamsseeds.h` using the `contrib/seeds/generate-seeds.py` script. This routine maintenance ensures the initial peer discovery mechanism remains robust by rotating out inactive nodes and introducing fresh, highly available network peers."
  },
  "32241": {
    "public_summary": "This PR finalizes the codebase for the Bitcoin Core 29.0 release. It consolidates all last-minute adjustments and ensures the software is ready for stable distribution.",
    "technical_summary": "This PR merges the final pre-release adjustments for the 29.0 release cycle, including version string updates, final release notes formatting, and any backported late-stage bug fixes. It acts as the definitive consolidation point before tagging the final 29.0 release."
  },
  "32742": {
    "public_summary": "This PR fixes a bug in the automated testing system that caused tests related to network peer eviction to occasionally fail or stall. This improves the reliability of the development process.",
    "technical_summary": "This PR addresses a race condition in the `p2p_eviction.py` functional test by resolving a 'catchup loop' issue where the test node could fail to sync with outbound peers before eviction logic triggered. It stabilizes the CI pipeline by ensuring deterministic test execution."
  },
  "31181": {
    "public_summary": "This update significantly improves how Bitcoin Core locates the Libevent networking library during compilation, reducing build errors for users across different operating systems.",
    "technical_summary": "This PR refactors the `FindLibevent` CMake module to modernize dependency resolution. It consolidates search logic, improves integration with `pkg-config`, and implements rigorous version detection, reducing build failures on diverse platforms."
  },
  "32017": {
    "public_summary": "This documentation update warns macOS users about potential build issues if Qt6 is installed alongside Qt5 when compiling Bitcoin Core.",
    "technical_summary": "This PR modifies `doc/build-osx.md` to add a specific warning that having `qt6` installed via Homebrew can conflict with `qt5`, causing `configure` script failures. This reduces the incidence of environment-related support issues."
  },
  "31284": {
    "public_summary": "This PR improves Bitcoin Core's testing system by skipping tests that are known to fail unreliably on Wine64 environments, avoiding wasted developer time.",
    "technical_summary": "This PR modifies the CI runner to skip specific functional tests (e.g., `feature_notifications.py`) by default on Wine64 environments. This prevents false negatives and reduces CI pipeline noise by avoiding known environment-specific issues."
  },
  "33201": {
    "public_summary": "This PR adds automated tests for Bitcoin Core's Inter-Process Communication (IPC) interface, ensuring that this advanced feature remains stable and secure.",
    "technical_summary": "This PR introduces new functional tests utilizing the test framework to exercise the multiprocess IPC endpoints. It validates message serialization, connection lifecycle, and capability negotiation between decoupled Bitcoin Core components."
  },
  "31379": {
    "public_summary": "This pull request fixes a problem in Bitcoin Core's build system where custom compiler flags were not correctly applied to the cryptography library, ensuring consistent security settings.",
    "technical_summary": "This PR resolves a CMake build bug where custom `APPEND_CFLAGS` and `APPEND_CXXFLAGS` were not propagated to the `secp256k1` subdirectory. It ensures optimization and security flags are consistently applied to the critical bundled cryptographic dependency."
  },
  "32369": {
    "public_summary": "This update fixes an automated test for wallet keys, ensuring that our internal checks accurately verify how Bitcoin Core handles complex wallet paths.",
    "technical_summary": "This PR corrects the `wallet_keypool.py` functional test by targeting the correct node instance when verifying doubled keypath derivations. It prevents false-positive test passes and improves test coverage accuracy for hierarchical deterministic (HD) wallet edge cases."
  },
  "31416": {
    "public_summary": "This PR corrects inaccuracies within the documentation for Bitcoin Core's `send` command, ensuring users have precise information for sending transactions.",
    "technical_summary": "This patch directly modifies the RPC documentation files targeting the `send` RPCs. It rectifies outdated information regarding parameters and return values, aligning the developer-facing documentation with the current C++ implementation."
  },
  "31945": {
    "public_summary": "This update fixes persistent issues in Bitcoin Core's automated testing system by updating an internal background processing library.",
    "technical_summary": "This PR updates the `libmultiprocess` dependency within the `depends` build system to resolve intermittent CI failures. By ensuring compatibility with newer toolchains, it enhances the robustness of the testing pipeline."
  },
  "32975": {
    "public_summary": "This update improves Bitcoin Core's internal logging system to record every change in how blocks are validated. This helps developers debug complex network synchronization issues.",
    "technical_summary": "This PR enhances the `assumevalid` logging infrastructure by emitting a trace or debug log event upon every state transition in the script validation pipeline. It provides finer granularity for diagnosing consensus failures during fast block propagation."
  },
  "33333": {
    "public_summary": "This update adds a helpful warning message if a user configures their Bitcoin node to use an excessively large database cache, which could accidentally crash their computer.",
    "technical_summary": "This PR introduces a validation check in `init.cpp` that emits a warning log if the `-dbcache` parameter exceeds the safely addressable memory limit of the host OS architecture. It prevents Out-Of-Memory (OOM) crashes caused by user misconfiguration."
  },
  "31359": {
    "public_summary": "This update enhances Bitcoin Core's build system by verifying support for advanced runtime memory defenses (PIE), increasing resilience against attacks.",
    "technical_summary": "This PR introduces a new CMake module `CheckLinkerSupportsPIE` to dynamically detect support for Position-Independent Executables (PIE). It conditionally applies PIE flags to maximize runtime memory protections when supported by the host toolchain."
  },
  "31709": {
    "public_summary": "This PR ensures that if a user tries to build Bitcoin Core with advanced multiprocess features, the build process will immediately halt with a clear error if the required internal libraries are missing.",
    "technical_summary": "This PR modifies the CMake configuration to explicitly `FATAL_ERROR` if the `Libmultiprocess` dependency is not found when the `WITH_MULTIPROCESS` flag is set to `ON`. This prevents silent fallbacks and confusing linking errors later in the build."
  },
  "31908": {
    "public_summary": "This pull request undoes a recent change related to random number generation that caused unexpected issues, returning the system to its previous stable state.",
    "technical_summary": "This PR reverts the merge commit for PR #31826 due to unintended side-effects discovered in the hardware RNG initialization sequence. It restores the prior `InitHardwareRand` logic to maintain node stability until the underlying issue is resolved."
  },
  "31267": {
    "public_summary": "This is a small code cleanup that removes an outdated spacing rule in the codebase, keeping the source code modern and easy to read.",
    "technical_summary": "This PR performs a refactoring pass to remove deprecated whitespace between the string literal and the user-defined literal operator `_mst` in C++ code. This aligns the codebase with modern C++ standard formatting guidelines and resolves minor compiler warnings."
  }
}

cache.update(new_summaries)
with open(cache_file, 'w') as f:
    json.dump(cache, f, indent=2)

print("Added 20 manual AI summaries to cache.")
