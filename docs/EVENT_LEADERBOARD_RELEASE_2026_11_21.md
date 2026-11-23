# Production leaderboard verification — 2026-11-21 Taipei

User-approved runtime release: `733a9255f0b7329f1b5324214663be7194d750d4`, pushed to the existing `codex/tcg-drive-backup` branch. Railway deployment `a15d8f94-824c-446e-9120-112c824e3007` reached SUCCESS at 2026-11-20 17:23:39 UTC. The existing service, volume, credentials and limits were retained: 0.5 requests/second, 1,800-second job deadline, 600-second interval.

The deployment connector cancelled its confirmation twice. After the user separately authorized the CLI, the existing Railway CLI applied the sole staged source-commit change. No source/provider substitution was used for the scan. The scheduled scan completed normally at 17:46:50 UTC with returncode=0.

## Complete finite activity

Event `2026-11-20`: Taipei November 20 00:00 inclusive to November 21 00:00 exclusive.

- Snapshot: `3140f6689c664479a7a24892899c7107`, generated 2026-11-20 17:46:48 UTC; status=final, stale=false, last_error=null.
- Through block: 134723436. All five configured source checkpoints are at next_block=134723437 with the same nonempty canonical block hash.
- 407 canonical participant wallets, 3,607 receipt/payment-verified checkout events, 7,568 paid draws.
- 3,607 checkout-evidence rows frozen with that same snapshot, matching the complete ledger.

| Builder | Wallets | Checkouts | Draws |
| --- | ---: | ---: | ---: |
| all-zero bytes32 | 127 | 1,744 | 5,066 |
| bytes32 ending …0001 | 225 | 1,144 | 1,197 |
| `0x2e622bb0dfd32f36aefb84dc4eb14ced42afa80a84f3a4cb31b21488f2a61501` | 64 | 719 | 1,305 |

Wallets can appear in multiple builder scopes; their counts must not be summed for the global participant count. Builder names and campaign eligibility were not inferred from those codes.

## Live checks

The website-specific read credential was used against the actual production API. Complete results and every page were checked for global competition ranks, no omissions/duplicates, wallet/name/verified-migration-alias search, stable immutable snapshots, all three builder scopes, exact integer payment/draw totals per wallet and rejected mixed-builder checkout cursors. The three scopes also matched the independent full-period event inventory. The campaign consumer's actual checkout-evidence parser accepted the same production snapshot.

74 successful HTTP responses were recorded: median 125ms, maximum 362ms in this run, not an SLA. All recorded API responses used no-store. Anonymous/invalid credentials and website credentials on Profile/POST BBW were rejected. API and Bot health both returned 200 after completion.

Deployed SHA-256 values matched the released checkout:

- scanner.py: `9f27c7e7f43d6889220f52b439f8ac2f7c61e9414e52ea0d06267f70a6ffcbcb`
- http.py: `b00ad0792e7d3761ba800d9549bc3bdd124b8d36cfc48166f4da877bcf41687e`
- store.py: `d80fc9a1c8053575be73c0f376762b7e50c132bdc8d0489487b8120f52bb05c6`

Local verification before release: 75 relevant regressions passed; 24 scanner/evidence checks passed again after formatting. The companion campaign backend passed 34 tests without skips. Isolated fixtures were not used as live acceptance evidence.

## Remaining scope

This release completes the finite paid-draw scan and evidence API. December event dates, campaign builder/pack ticket weights, token bonuses and X task configuration/credentials remain unset. `draw_count` is not an effective-ticket score; actual campaign reward issuance/user-wallet acceptance still requires those rules. The frontend was not published by this API release.

PR #2 remains OPEN/Draft for user review; no merge or worktree cleanup was performed. No 1Password vault/item changed and no secret temporary file was created.
