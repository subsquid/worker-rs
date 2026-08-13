# Release notes template

GitHub title is always `vX.Y.Z` — set with `--title "vX.Y.Z"`. The body opens
with `## What's Changed`.

Notes are prose, not a changelog. `git log` already lists the commits; the
notes exist to say what an operator or a client can do now that they couldn't
before, and what used to go wrong.

Budget: a lead paragraph of 2-3 sentences, then bullets of one sentence each,
two if the old behaviour needs naming. A bare list of commit subjects is too
thin; a paragraph per bullet is too much — the reader is scanning to decide
whether to upgrade.

## Standard release

```markdown
## What's Changed

**<Headline of the dominant change.>** <2-3 sentences: what it enables, what
the old behaviour was, and any limit worth knowing — experimental, opt-in,
feature-gated. No implementation details.>

- **<Bold lede.>** <One sentence, two at most: the new behaviour and the
  failure or friction it replaces.> (#PR)
- **<Bold lede.>** <...> (#PR)

**Full Changelog**: https://github.com/subsquid/worker-rs/compare/vPREV...vNEW
```

A release with no dominant change drops the lead paragraph and keeps the
explained bullets. A single-change release is the lead paragraph alone.

## Style rules

- **Lead with user-visible impact**, not internal mechanism. "Workers report
  advanced query execution statistics" beats "Added `QueryStats` to the
  response struct".
- **Name the old behaviour.** "Used to surface as a bare connection reset" is
  what makes a fix legible; "improved error handling" says nothing. This is the
  main source of substance — dig it out of the PR body or the commit body.
- **State the limits.** Experimental, opt-in per query, gated behind a build
  feature, not active in the default image — if a change doesn't reach a
  default deployment, say so in the same breath. Silence reads as "shipped".
- **No sub-bullets, no emoji, no leading version numbers.**
- **No deployment / ops instructions.** No grace-period settings, no `preStop`,
  no kubectl recipes. Those belong in a doc or a runbook. If a release requires
  operator action, link the doc — the notes don't repeat the recipe.
- **No CI / internal changes.** Skip workflow tweaks, clippy fixes, refactors.
  Release notes are for behavior the user observes.
- **Public names yes, internal knobs no.** Query-API fields, protocol fields,
  and cargo features are part of the contract — name them. Internal tunables,
  dependency revisions, and your own benchmark numbers are not; generalize
  those, and point to a doc for the real values.
- **Doc + PR ref.** PR refs go at the end of the bullet they explain. A doc
  link, when a doc exists, goes on its own line before the compare link,
  anchored on the tag (`/blob/vNEW/`) so it doesn't rot when `master` evolves.
- **Compare link always last.** `vPREV` is the previous *released* version — not
  necessarily the previous tag, since tags get pushed here without a release
  page. Resolve with `git merge-base vPREV vNEW` when the previous-tag-in-sort-order
  isn't the real parent.

## What to skip entirely

- Install / upgrade commands (`docker pull`, `kubectl apply`) — live in deploy docs.
- Protocol tables, scenario matrices, and other reference material from the PR
  body — link the PR, don't inline it.
- The full commit message — `git log` is for the engineer's-eye view; notes are for the user's.
- "Tests" section on patch releases unless the headline is about test coverage.
- Internal cluster names, account IDs, environment labels.
