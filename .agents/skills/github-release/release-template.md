# Release notes template

GitHub title is always `vX.Y.Z` — set with `--title "vX.Y.Z"`. The body opens
with `## What's Changed`.

## Standard release

```markdown
## What's Changed

- Short user-visible change 1
- Short user-visible change 2

**Full Changelog**: https://github.com/subsquid/worker-rs/compare/vPREV...vNEW
```

## When one change dominates

```markdown
## What's Changed

**<Headline concept>.** <1-2 sentence lead — what the operator or the network
sees now that they didn't before. No implementation details.>

- Secondary change 1
- Secondary change 2

**Full Changelog**: https://github.com/subsquid/worker-rs/compare/vPREV...vNEW
```

## Style rules

- **Lead with user-visible impact**, not internal mechanism. "Workers report
  advanced query execution statistics" beats "Added `QueryStats` to the
  response struct".
- **Bullets are one line each.** No sub-bullets, no emoji, no leading version
  numbers.
- **No deployment / ops instructions.** No grace-period settings, no `preStop`,
  no kubectl recipes. Those belong in a doc or a runbook. If a release requires
  operator action, link the doc — the notes don't repeat the recipe.
- **No CI / internal changes.** Skip workflow tweaks, clippy fixes, refactors.
  Release notes are for behavior the user observes.
- **No internal config keys or benchmark numbers in the body.** Generalize:
  a specific tunable becomes "a configurable grace period". Point to a doc for
  the real values.
- **Doc + PR ref**, when a doc exists, as a single line at the end of the prose,
  before the compare link. Anchor the link on the tag (`/blob/vNEW/`) so it
  doesn't rot when `master` evolves.
- **Compare link always last.** `vPREV` is the previous *released* version — not
  necessarily the previous tag, since tags get pushed here without a release
  page. Resolve with `git merge-base vPREV vNEW` when the previous-tag-in-sort-order
  isn't the real parent.

## What to skip entirely

- Install / upgrade commands (`docker pull`, `kubectl apply`) — live in deploy docs.
- The full commit message — `git log` is for the engineer's-eye view; notes are for the user's.
- "Tests" section on patch releases unless the headline is about test coverage.
- Internal cluster names, account IDs, environment labels.
