#!/usr/bin/env python3
"""Standing quality gate for the worker spec suite (spec/).

Single file, stdlib only. Enforces the conventions the suite itself declares
(README §Conventions): reference integrity, dead weight, traceability coverage,
decision-log consistency, normative shape, freshness.

Usage: check_spec.py [SPEC_DIR] [--format text|json|github] [--severity E|W|I]
                     [--only CHECK[,CHECK]] [--strict] [--no-ignore] [--list-checks]
Exit: 1 if error-severity findings survive (or any finding with --strict),
      2 on bad usage, 0 otherwise.
"""

import json
import os
import re
import subprocess
import sys
from collections import defaultdict

# ----------------------------------------------------------------------------
# Configuration — the suite's declared conventions (README.md §Conventions).
# ----------------------------------------------------------------------------

# ID prefix -> home document (the only doc where the prefix is *defined*).
HOME_DOCS = {
    "REQ": "02-requirements.md",
    "OQ": "02-requirements.md",
    "DEF": "03-data-model.md",
    "WP": "04-mutations.md",
    "RP": "05-queries.md",
    "CN": "06-consistency-durability.md",
    "INV": "07-invariants.md",
    "LIV": "08-liveness.md",
    "FM": "09-failure-model.md",
    "RS": "10-retention-space.md",
    "PF": "11-performance.md",
    "SLI": "11-performance.md",
    "HZ": "11-performance.md",
    "OB": "12-observability.md",
    "CT": "13-conformance-tdd.md",
    "MG": "13-conformance-tdd.md",
    "HC": "13-conformance-tdd.md",
    "GAP": "13-conformance-tdd.md",
    "IB": "14-interface-binding.md",
    "G": "01-overview.md",
    "NG": "01-overview.md",
}
PARAM_HOME = "15-parameters.md"       # P-* registry
WPARAM_HOME = "11-performance.md"     # W-* workload table
ADR_DIR = "decisions"
ADR_LOG_DOC = "README.md"             # holds the decision-log table
ADR_STATUSES = ("Accepted (historical)", "Proposed", "Accepted", "Superseded")

MUTABLE_DOCS = {"13-conformance-tdd.md", "15-parameters.md"}
MATRIX_DOC = "13-conformance-tdd.md"
REQ_MATRIX_HEADING = "Requirements"
PROP_MATRIX_HEADING = "Properties"
PROP_MATRIX_PREFIXES = ("INV", "LIV", "FM", "SLI")

# Goals/non-goals are top-level statements; references flow *from* them, so they
# are exempt from the orphan check.
ORPHAN_EXEMPT_PREFIXES = {"G", "NG"}

# impl-leakage: normative core docs only (14 binds the concrete surface by design;
# 13/15 are mutable status docs; decisions/ exempt per the skill).
LEAKAGE_DOCS = [
    "01-overview.md", "02-requirements.md", "03-data-model.md", "04-mutations.md",
    "05-queries.md", "06-consistency-durability.md", "07-invariants.md",
    "08-liveness.md", "09-failure-model.md", "10-retention-space.md",
    "11-performance.md", "12-observability.md",
]
LEAKAGE_TERMS = [
    r"\btokio\b", r"\brayon\b", r"\bpolars\b", r"\brusqlite\b", r"\bsqlite\b",
    r"\breqwest\b", r"\blibp2p\b", r"\bflatbuffers?\b", r"\bprotobuf\b",
    r"\bprost\b", r"\baxum\b", r"\bscopeguard\b", r"\bmimalloc\b", r"\bquic\b",
    r"\bgossipsub\b", r"\bparquet\b", r"\bcargo\b", r"\bclippy\b", r"\brustc?\b",
    r"\bMutex\b", r"\bBTreeSet\b", r"\bsrc/", r"\.rs\b", r"\bworkdir\b",
]
# The terminology cross-reference table in 03 deliberately maps codebase terms.
LEAKAGE_EXEMPT_SECTIONS = {"Terminology cross-reference"}

BARE_CONST_DOCS = LEAKAGE_DOCS  # same scope; tables and exempt sections skipped
BARE_CONST_RE = re.compile(
    r"(?<![\w./-])\d[\d\s]*(?:\.\d+)?\s?(?:ms|s\b|h\b|MiB|KiB|GiB|MB|KB|GB|%)"
)
BARE_CONST_EXEMPT_SECTIONS = {
    "Open questions", "Explicitly unspecified", "Benchmark numbers on record",
    "Terminology cross-reference",
}

FRESHNESS_BASELINE_RE = re.compile(r"baseline(?:\s+commit)?\s+`([0-9a-f]{7,40})`")
AS_OF_RE = re.compile(r"[Aa]s of:?\s*\**\s*(\d{4}-\d{2}-\d{2})")

ALL_PREFIXES = sorted(HOME_DOCS, key=len, reverse=True)
# ADR is referenced like any ID but defined by files in decisions/, not a home doc.
ID_RE = re.compile(
    r"\b(" + "|".join(ALL_PREFIXES + ["ADR"]) + r")-(\d+)"
    r"((?:/\d+)+)?"                      # slash lists: INV-20/21/25
    r"(?:\.\.(?:(?:" + "|".join(ALL_PREFIXES) + r")-)?(\d+))?"  # ranges: CT-2..9
)
PARAM_RE = re.compile(r"\bP-[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+\b")
PARAM_CODE_RE = re.compile(r"\bP_[A-Z][A-Z0-9_]+\b")
WPARAM_RE = re.compile(r"\bW-[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*\b")
STRONG_DEF_RE = re.compile(r"\*\*([A-Z]+)-(\d+)\s+—")
HEADING_DEF_RE = re.compile(r"^#{1,6}\s+([A-Z]+)-(\d+)\s+—\s+(.*)$")
ROW_DEF_RE = re.compile(r"^\|\s*([A-Z]+)-(\d+)\b")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
RFC_TAG_RE = re.compile(r"\[(MUST|MUST NOT|SHOULD|SHOULD NOT|MAY)(?:\s*—[^\]]*)?\]")
SCOPE_TAG_RE = re.compile(r"\[(state|transition|response|recovery)\]")
BAND_LINE_RE = re.compile(r"^(Home doc for|Bands?:)|\bBands?:")

CHECKS = {
    # name: (severity, description)
    "id-undefined": ("E", "every referenced ID is defined in its home doc"),
    "id-duplicate": ("E", "no ID has two strong-form definitions"),
    "param-undefined": ("E", "every P-*/W-* symbol has a registry row"),
    "link-missing-file": ("E", "relative links resolve"),
    "link-missing-anchor": ("E", "#fragments name a real heading"),
    "doc-unlisted": ("E", "every spec doc is in the README document map"),
    "doc-missing": ("E", "every document-map entry exists"),
    "adr-undefined": ("E", "every referenced ADR-n has a file"),
    "id-orphan": ("W", "every defined ID is referenced beyond its definition"),
    "param-unused": ("W", "every registry row is referenced elsewhere"),
    "trace-missing": ("E", "every REQ / INV,LIV,FM,SLI has a matrix row"),
    "trace-unknown": ("E", "matrix rows name only defined IDs"),
    "inv-check-unknown": ("E", "invariant Check: names existing CT classes"),
    "gate-threshold-literal": ("E", "MG thresholds are P-* symbols"),
    "gate-no-capability": ("E", "every MG names an HC"),
    "gate-unknown-class": ("E", "CT named by a gate exists"),
    "hc-orphan": ("W", "every HC is needed by a CT or MG"),
    "ct-unbuildable": ("W", "no CT with only-U capabilities claims C coverage"),
    "adr-missing-file": ("E", "every log row has a file"),
    "adr-missing-row": ("E", "every ADR file is in the README log"),
    "adr-status-mismatch": ("E", "log status matches the file Status: line"),
    "adr-id-mismatch": ("E", "filename, heading and log row agree"),
    "adr-bad-status": ("E", "ADR status is from the allowed set"),
    "adr-log-order": ("W", "accepted log rows ascend"),
    "adr-title-drift": ("I", "log title matches the ADR heading"),
    "req-missing-tag": ("E", "every REQ carries an RFC 2119 tag"),
    "req-missing-acceptance": ("E", "every REQ has Acceptance:"),
    "inv-missing-scope": ("E", "every INV carries a scope tag"),
    "inv-missing-check": ("E", "every INV has Check:"),
    "param-bare-constant": ("W", "no bare dimensioned literal in normative prose"),
    "impl-leakage": ("W", "no implementation names in core normative docs"),
    "heading-duplicate": ("W", "heading slugs unique per file"),
    "warn-unratified": ("W", "every ⚠ routes to an ADR/OQ/GAP/registry row"),
    "stale-matrix": ("W", "status doc as-of date not older than normative docs"),
    "date-inconsistent": ("W", "one as-of date per mutable doc"),
    "stale-baseline": ("W", "pinned baseline commit exists"),
}

# ----------------------------------------------------------------------------


class Finding:
    def __init__(self, check, path, line, msg):
        self.check = check
        self.sev = CHECKS[check][0]
        self.path = path
        self.line = line
        self.msg = msg


def strip_fences(lines):
    out, fenced = [], False
    for ln in lines:
        if ln.lstrip().startswith("```"):
            fenced = not fenced
            out.append("")
            continue
        out.append("" if fenced else ln)
    return out


def slugify(text):
    text = re.sub(r"[`*_\[\]()§]", "", text).strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"[\s]+", "-", text)


def expand_ids(text):
    """Yield (prefix, num, hard) for every ID reference, expanding shorthand."""
    for m in ID_RE.finditer(text):
        prefix, first = m.group(1), int(m.group(2))
        yield prefix, first, True
        if m.group(3):
            for part in m.group(3).strip("/").split("/"):
                yield prefix, int(part), True
        if m.group(4):
            last = int(m.group(4))
            yield prefix, last, True
            for n in range(first + 1, last):
                yield prefix, n, False  # interior of a range: soft


class Suite:
    def __init__(self, root):
        self.root = root
        self.files = {}  # rel path -> list of lines
        for dirpath, _dirs, names in os.walk(root):
            if os.path.basename(dirpath) == "tools":
                continue
            for name in sorted(names):
                if name.endswith(".md"):
                    rel = os.path.relpath(os.path.join(dirpath, name), root)
                    with open(os.path.join(dirpath, name), encoding="utf-8") as fh:
                        self.files[rel] = fh.read().splitlines()
        self.findings = []
        self.defs = {}      # (prefix, num) -> (path, line)
        self.refs = defaultdict(list)   # (prefix, num) -> [(path, line, hard)]
        self.params = {}    # symbol -> (path, line)  (P-* and W-* rows)
        self.param_refs = defaultdict(list)
        self.adr_files = {}  # num -> (relpath, heading_title, status, line)
        self.adr_rows = {}   # num -> (title, status, line)

    def add(self, check, path, line, msg):
        self.findings.append(Finding(check, path, line, msg))

    # -- collection ----------------------------------------------------------

    def collect(self):
        for path, lines in self.files.items():
            clean = strip_fences(lines)
            in_dec = path.startswith(ADR_DIR + os.sep) or path.startswith(ADR_DIR + "/")
            # definitions (home docs and ADR files only)
            for i, ln in enumerate(clean, 1):
                if in_dec:
                    m = HEADING_DEF_RE.match(ln)
                    if m and m.group(1) == "ADR":
                        num = int(m.group(2))
                        status = self._adr_status(lines)
                        self.adr_files[num] = (path, m.group(3).strip(), status, i)
                    continue
                for m in STRONG_DEF_RE.finditer(ln):
                    self._maybe_define(m.group(1), int(m.group(2)), path, i, strong=True)
                m = HEADING_DEF_RE.match(ln)
                if m:
                    self._maybe_define(m.group(1), int(m.group(2)), path, i, strong=True)
                m = ROW_DEF_RE.match(ln)
                if m:
                    self._maybe_define(m.group(1), int(m.group(2)), path, i, strong=False)
                if path == PARAM_HOME:
                    pm = re.match(r"^\|\s*(P-[A-Z0-9-]+)\s*\|", ln)
                    if pm and pm.group(1) not in self.params:
                        self.params[pm.group(1)] = (path, i)
                if path == WPARAM_HOME:
                    wm = re.match(r"^\|\s*(W-[A-Z0-9-]+)\s*\|", ln)
                    if wm and wm.group(1) not in self.params:
                        self.params[wm.group(1)] = (path, i)
            # references (fences NOT stripped: pseudocode refs are real).
            # The header block (everything before the second blank line) holds
            # the band declarations, which legitimately span unassigned numbers
            # — skip ID extraction there.
            blanks = 0
            header_end = 0
            for i, ln in enumerate(lines, 1):
                if not ln.strip():
                    blanks += 1
                    if blanks == 2:
                        header_end = i
                        break
            for i, ln in enumerate(lines, 1):
                if i <= header_end and not in_dec:
                    continue
                if BAND_LINE_RE.search(ln) and "|" not in ln:
                    continue  # band declarations span unassigned numbers
                for prefix, num, hard in expand_ids(ln):
                    self.refs[(prefix, num)].append((path, i, hard))
                for pm in PARAM_RE.finditer(ln):
                    self.param_refs[pm.group(0)].append((path, i))
                for pm in PARAM_CODE_RE.finditer(ln):
                    self.param_refs[pm.group(0).replace("_", "-")].append((path, i))
                for wm in WPARAM_RE.finditer(ln):
                    self.param_refs[wm.group(0)].append((path, i))

    def _maybe_define(self, prefix, num, path, line, strong):
        if prefix not in HOME_DOCS or HOME_DOCS[prefix] != path:
            return
        key = (prefix, num)
        if key in self.defs:
            if strong and self.defs[key][2]:
                self.add("id-duplicate", path, line,
                         f"{prefix}-{num} already defined at "
                         f"{self.defs[key][0]}:{self.defs[key][1]}")
            return
        self.defs[key] = (path, line, strong)

    @staticmethod
    def _adr_status(lines):
        for ln in lines:
            m = re.match(r"^Status:\s*(.+?)\s*$", ln)
            if m:
                return m.group(1)
        return None

    # -- checks --------------------------------------------------------------

    def check_reference_integrity(self):
        # Closing a gap deletes its row, but an accepted ADR is append-only: its context
        # keeps citing ids the register has since dropped. A proposed one is still editable,
        # so it stays checked.
        frozen = {info[0] for info in self.adr_files.values()
                  if (info[2] or "").startswith("Accepted")}
        for (prefix, num), sites in sorted(self.refs.items()):
            if prefix == "ADR":
                continue
            if (prefix, num) not in self.defs:
                hard_sites = [s for s in sites if s[2]]
                if prefix == "GAP":
                    hard_sites = [s for s in hard_sites if s[0] not in frozen]
                if hard_sites:
                    p, l, _ = hard_sites[0]
                    self.add("id-undefined", p, l,
                             f"{prefix}-{num} is referenced but not defined in "
                             f"{HOME_DOCS[prefix]}")
        for (prefix, num), sites in sorted(self.refs.items()):
            if prefix == "ADR" and num not in self.adr_files:
                p, l, _ = sites[0]
                self.add("adr-undefined", p, l, f"ADR-{num} has no file in {ADR_DIR}/")
        for sym, sites in sorted(self.param_refs.items()):
            if sym not in self.params:
                p, l = sites[0]
                home = PARAM_HOME if sym.startswith("P-") else WPARAM_HOME
                self.add("param-undefined", p, l, f"{sym} has no row in {home}")

    def check_links(self):
        for path, lines in self.files.items():
            base = os.path.dirname(os.path.join(self.root, path))
            for i, ln in enumerate(lines, 1):
                for m in LINK_RE.finditer(ln):
                    target = m.group(1)
                    if target.startswith(("http://", "https://", "mailto:")):
                        continue
                    frag = None
                    if "#" in target:
                        target, frag = target.split("#", 1)
                    tpath = os.path.normpath(os.path.join(base, target)) if target \
                        else os.path.join(self.root, path)
                    if target and not os.path.exists(tpath):
                        self.add("link-missing-file", path, i,
                                 f"link target does not exist: {m.group(1)}")
                        continue
                    if frag:
                        rel = os.path.relpath(tpath, self.root)
                        doc = self.files.get(rel.replace(os.sep, "/"))
                        if doc is None:
                            continue
                        slugs = {slugify(h.lstrip("#").strip())
                                 for h in doc if h.startswith("#")}
                        if frag not in slugs:
                            self.add("link-missing-anchor", path, i,
                                     f"#{frag} is not a heading in {rel}")

    def check_doc_map(self):
        readme = self.files.get("README.md", [])
        listed = set()
        for ln in readme:
            for m in LINK_RE.finditer(ln):
                t = m.group(1).split("#")[0]
                if t.endswith(".md"):
                    listed.add(t)
        for ln in readme:
            if re.search(r"^\|\s*decisions/", ln):
                listed.add(ADR_DIR)
        for path in self.files:
            p = path.replace(os.sep, "/")
            if p == "README.md":
                continue
            if p.startswith("decisions/"):
                if ADR_DIR not in listed and p not in listed:
                    self.add("doc-unlisted", "README.md", 1,
                             f"{p} unreachable: decisions/ not in the document map")
                continue
            if p not in listed:
                self.add("doc-unlisted", "README.md", 1,
                         f"{p} is not in the README document map")
        for t in listed:
            if t.endswith(".md") and t.replace("/", os.sep) not in self.files:
                self.add("doc-missing", "README.md", 1,
                         f"document map names a missing file: {t}")

    def check_dead_weight(self):
        closed_re = re.compile(r"^#{1,6}\s+.*(closed|resolved|retired)", re.I)
        for (prefix, num), (path, line, _s) in sorted(self.defs.items()):
            if prefix in ORPHAN_EXEMPT_PREFIXES:
                continue
            ext = [s for s in self.refs.get((prefix, num), [])
                   if not (s[0] == path and s[1] == line)]
            if not ext:
                # exempt closed-register entries
                section = self._section_of(path, line)
                if section and closed_re.match("# " + section):
                    continue
                self.add("id-orphan", path, line,
                         f"{prefix}-{num} is defined but referenced nowhere else")
        for sym, (path, line) in sorted(self.params.items()):
            ext = [s for s in self.param_refs.get(sym, [])
                   if not (s[0] == path and s[1] == line)]
            if not ext:
                self.add("param-unused", path, line,
                         f"{sym} has a registry row but is used nowhere else")

    def _section_of(self, path, line):
        lines = self.files[path]
        for i in range(min(line, len(lines)) - 1, -1, -1):
            if lines[i].startswith("#"):
                return lines[i].lstrip("#").strip()
        return None

    def _matrix_first_cells(self, heading):
        lines = self.files.get(MATRIX_DOC, [])
        cells, active = [], False
        for i, ln in enumerate(lines, 1):
            h = re.match(r"^(#{2,6})\s+(.*)$", ln)
            if h:
                active = h.group(2).strip() == heading
                continue
            if active:
                m = re.match(r"^\|\s*([^|]+?)\s*\|", ln)
                if m and m.group(1) not in ("ID", ":---", "---") \
                        and not set(m.group(1)) <= set(":- "):
                    cells.append((m.group(1), i))
        return cells

    def check_traceability(self):
        covered = defaultdict(set)
        for heading in (REQ_MATRIX_HEADING, PROP_MATRIX_HEADING):
            for cell, line in self._matrix_first_cells(heading):
                for prefix, num, _hard in expand_ids(cell):
                    covered[prefix].add(num)
                    if (prefix, num) not in self.defs:
                        self.add("trace-unknown", MATRIX_DOC, line,
                                 f"matrix row names undefined {prefix}-{num}")
        for (prefix, num) in sorted(self.defs):
            if prefix == "REQ" and num not in covered["REQ"]:
                self.add("trace-missing", MATRIX_DOC, 1,
                         f"REQ-{num} has no row in the {REQ_MATRIX_HEADING} matrix")
            if prefix in PROP_MATRIX_PREFIXES and num not in covered[prefix]:
                self.add("trace-missing", MATRIX_DOC, 1,
                         f"{prefix}-{num} has no row in the "
                         f"{PROP_MATRIX_HEADING} matrix")

    def check_inv_shape(self):
        lines = self.files.get(HOME_DOCS["INV"], [])
        clean = strip_fences(lines)
        entries = [(i, m) for i, ln in enumerate(clean, 1)
                   for m in [STRONG_DEF_RE.search(ln)] if m and m.group(1) == "INV"]
        for idx, (i, m) in enumerate(entries):
            end = entries[idx + 1][0] - 1 if idx + 1 < len(entries) else len(clean)
            block = clean[i - 1:end]
            if not SCOPE_TAG_RE.search(block[0]):
                self.add("inv-missing-scope", HOME_DOCS["INV"], i,
                         f"INV-{m.group(2)} has no [state|transition|response|"
                         f"recovery] tag on its definition line")
            if not any("*Check:*" in ln for ln in block):
                self.add("inv-missing-check", HOME_DOCS["INV"], i,
                         f"INV-{m.group(2)} names no check strategy")
            for ln_off, ln in enumerate(block):
                if "*Check:*" in ln:
                    cts = [n for p, n, _ in expand_ids(ln) if p == "CT"]
                    for n in cts:
                        if ("CT", n) not in self.defs:
                            self.add("inv-check-unknown", HOME_DOCS["INV"],
                                     i + ln_off,
                                     f"INV-{m.group(2)} Check: names CT-{n}, "
                                     f"which is not in the taxonomy")

    def check_req_shape(self):
        lines = strip_fences(self.files.get(HOME_DOCS["REQ"], []))
        entries = [(i, m) for i, ln in enumerate(lines, 1)
                   for m in [STRONG_DEF_RE.search(ln)] if m and m.group(1) == "REQ"]
        for idx, (i, m) in enumerate(entries):
            end = entries[idx + 1][0] - 1 if idx + 1 < len(entries) else len(lines)
            block = lines[i - 1:end]
            if not RFC_TAG_RE.search(" ".join(block[:3])):
                self.add("req-missing-tag", HOME_DOCS["REQ"], i,
                         f"REQ-{m.group(2)} has no RFC 2119 tag")
            if not any("*Acceptance:*" in ln for ln in block):
                self.add("req-missing-acceptance", HOME_DOCS["REQ"], i,
                         f"REQ-{m.group(2)} has no *Acceptance:* criteria")

    def check_gates(self):
        lines = self.files.get(MATRIX_DOC, [])
        hc_status = {}
        for i, ln in enumerate(lines, 1):
            m = re.match(r"^\|\s*HC-(\d+)\s*\|[^|]*\|[^|]*\|\s*\**([CPU])\**\s*\|", ln)
            if m:
                hc_status[int(m.group(1))] = m.group(2)
        needed_hc = set()
        for i, ln in enumerate(lines, 1):
            m = re.match(r"^\|\s*MG-(\d+)\s*\|", ln)
            if not m:
                continue
            cells = [c.strip() for c in ln.strip("|").split("|")]
            gate_text = " ".join(cells)
            threshold = cells[2] if len(cells) > 2 else ""
            enforced = cells[4] if len(cells) > 4 else ""
            if not PARAM_RE.search(threshold):
                self.add("gate-threshold-literal", MATRIX_DOC, i,
                         f"MG-{m.group(1)} threshold is not a P-* symbol: "
                         f"'{threshold}' — register it in {PARAM_HOME}")
            hcs = [n for p, n, _ in expand_ids(enforced) if p == "HC"]
            if not hcs:
                self.add("gate-no-capability", MATRIX_DOC, i,
                         f"MG-{m.group(1)} names no HC-n in its 'enforced by' cell")
            needed_hc.update(hcs)
            for p, n, _ in expand_ids(gate_text):
                if p == "CT" and ("CT", n) not in self.defs:
                    self.add("gate-unknown-class", MATRIX_DOC, i,
                             f"MG-{m.group(1)} names CT-{n}, not in the taxonomy")
        ct_needs = {}
        for i, ln in enumerate(lines, 1):
            m = re.match(r"^\|\s*CT-(\d+)\s*\|", ln)
            if m and "Needs" not in ln:
                cells = [c.strip() for c in ln.strip("|").split("|")]
                if len(cells) >= 4:
                    ct_needs[int(m.group(1))] = \
                        [n for p, n, _ in expand_ids(cells[3]) if p == "HC"]
                    needed_hc.update(ct_needs[int(m.group(1))])
        for num in sorted(hc_status):
            if num not in needed_hc:
                p, l, _ = self.defs.get(("HC", num), (MATRIX_DOC, 1, False))
                self.add("hc-orphan", p, l,
                         f"HC-{num} is needed by no CT class or merge gate")
        # ct-unbuildable: a CT with only-U capabilities while a matrix row it owns
        # claims status C
        c_rows = set()
        for heading in (REQ_MATRIX_HEADING, PROP_MATRIX_HEADING):
            for cell, line in self._matrix_first_cells(heading):
                row = self.files[MATRIX_DOC][line - 1]
                cells = [c.strip() for c in row.strip("|").split("|")]
                if len(cells) >= 3 and cells[2].startswith("C"):
                    for p, n, _ in expand_ids(cells[1]):
                        if p == "CT":
                            c_rows.add(n)
        for ct, needs in sorted(ct_needs.items()):
            if ct in c_rows and needs and all(
                    hc_status.get(h, "U") == "U" for h in needs):
                self.add("ct-unbuildable", MATRIX_DOC, 1,
                         f"CT-{ct} claims C coverage but every capability it "
                         f"needs is status U")

    def check_adr_log(self):
        readme = self.files.get(ADR_LOG_DOC, [])
        row_re = re.compile(
            r"^\|\s*\[ADR-(\d+)\]\(([^)]+)\)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|")
        prev_accepted = 0
        for i, ln in enumerate(readme, 1):
            m = row_re.match(ln)
            if not m:
                if re.match(r"^\|\s*ADR-\d+\s*\|", ln):
                    n = int(re.match(r"^\|\s*ADR-(\d+)", ln).group(1))
                    self.adr_rows[n] = (None, None, i)  # unlinked → external
                continue
            num, path, title, status = (int(m.group(1)), m.group(2),
                                        m.group(3), re.sub(r"\*", "", m.group(4)))
            self.adr_rows[num] = (title, status.strip(), i)
            fpath = os.path.normpath(os.path.join(self.root, path))
            if not os.path.exists(fpath):
                self.add("adr-missing-file", ADR_LOG_DOC, i,
                         f"ADR-{num} log row links a missing file: {path}")
                continue
            fnum_m = re.search(r"ADR-(\d+)", os.path.basename(path))
            if fnum_m and int(fnum_m.group(1)) != num:
                self.add("adr-id-mismatch", ADR_LOG_DOC, i,
                         f"log row ADR-{num} links file named ADR-{fnum_m.group(1)}")
            info = self.adr_files.get(num)
            if info:
                fpath_rel, heading, fstatus, fline = info
                if fstatus is None or not fstatus.startswith(ADR_STATUSES):
                    self.add("adr-bad-status", fpath_rel, 1,
                             f"ADR-{num} Status: line missing or not in "
                             f"{ADR_STATUSES}")
                elif self._norm_status(fstatus) != self._norm_status(status):
                    self.add("adr-status-mismatch", ADR_LOG_DOC, i,
                             f"ADR-{num}: log says '{status.strip()}', file says "
                             f"'{fstatus}'")
                # date agreement, when both carry one
                d_log = re.search(r"\d{4}-\d{2}-\d{2}", status)
                d_file = re.search(r"\d{4}-\d{2}-\d{2}", fstatus or "")
                if d_log and d_file and d_log.group(0) != d_file.group(0):
                    self.add("adr-status-mismatch", ADR_LOG_DOC, i,
                             f"ADR-{num}: log date {d_log.group(0)} != file date "
                             f"{d_file.group(0)}")
                t_log = re.sub(r"\s*\([^)]*\)", "", title).strip().lower()
                t_file = re.sub(r"\s*\([^)]*\)", "", heading).strip().lower()
                if t_log != t_file:
                    self.add("adr-title-drift", ADR_LOG_DOC, i,
                             f"ADR-{num} log title '{title}' vs heading '{heading}'")
                if "Accepted" in (fstatus or ""):
                    if num < prev_accepted:
                        self.add("adr-log-order", ADR_LOG_DOC, i,
                                 f"accepted ADR-{num} listed after ADR-{prev_accepted}")
                    prev_accepted = max(prev_accepted, num)
        for num, (fpath_rel, _h, _s, _l) in sorted(self.adr_files.items()):
            if num not in self.adr_rows:
                self.add("adr-missing-row", fpath_rel, 1,
                         f"ADR-{num} has a file but no row in the README decision log")

    @staticmethod
    def _norm_status(s):
        s = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}\)", "", s or "")
        return s.strip().lower()

    def check_normative_shape(self):
        for path in BARE_CONST_DOCS:
            lines = strip_fences(self.files.get(path, []))
            for i, ln in enumerate(lines, 1):
                if ln.lstrip().startswith("|") or ln.startswith("#"):
                    continue
                section = self._section_of(path, i) or ""
                if any(s.lower() in section.lower()
                       for s in BARE_CONST_EXEMPT_SECTIONS):
                    continue
                if BAND_LINE_RE.search(ln):
                    continue
                m = BARE_CONST_RE.search(ln)
                if m:
                    self.add("param-bare-constant", path, i,
                             f"bare constant '{m.group(0).strip()}' in normative "
                             f"prose — use a P-* symbol")
        for path in LEAKAGE_DOCS:
            lines = strip_fences(self.files.get(path, []))
            for i, ln in enumerate(lines, 1):
                section = self._section_of(path, i) or ""
                if any(s.lower() in section.lower()
                       for s in LEAKAGE_EXEMPT_SECTIONS):
                    continue
                for term in LEAKAGE_TERMS:
                    m = re.search(term, ln, re.I)
                    if m:
                        self.add("impl-leakage", path, i,
                                 f"implementation term '{m.group(0)}' in a "
                                 f"normative core doc")
        for path, lines in self.files.items():
            seen = {}
            for i, ln in enumerate(lines, 1):
                if ln.startswith("#"):
                    slug = slugify(ln.lstrip("#").strip())
                    if slug in seen:
                        self.add("heading-duplicate", path, i,
                                 f"heading slug '{slug}' duplicates line {seen[slug]}")
                    seen[slug] = i
        self._check_warn_markers()

    def _routed_params(self):
        routed = set()
        header_routes = False
        lines = self.files.get(PARAM_HOME, [])
        for ln in lines[:12]:
            if re.search(r"ADR-\d+|ratif|proposed|draft", ln, re.I):
                header_routes = True
        for ln in lines:
            m = re.match(r"^\|\s*((?:P|W)-[A-Z0-9-]+)\s*\|", ln)
            if m and (header_routes
                      or re.search(r"ADR-\d+|OQ-\d+|GAP-\d+|ratif|proposed|draft",
                                   ln, re.I)):
                routed.add(m.group(1))
        return routed

    def _check_warn_markers(self):
        routed = self._routed_params()
        route_re = re.compile(r"\b(ADR|OQ|GAP)-\d+\b")
        for path, lines in self.files.items():
            # README defines the marker; the registry routes via its header rule;
            # decisions/ files ARE the ratification route.
            if path == "README.md" or path == PARAM_HOME \
                    or path.replace(os.sep, "/").startswith(ADR_DIR + "/"):
                continue
            clean = strip_fences(lines)
            i = 0
            while i < len(clean):
                ln = clean[i]
                if "⚠" not in ln:
                    i += 1
                    continue
                if ln.lstrip().startswith("|"):
                    row = ln
                    nxt = clean[i + 1] if i + 1 < len(clean) else ""
                    if set(re.sub(r"[|\s:⚠-]", "", row)) == set() or "---" in row \
                            or re.match(r"^\|[\s:|-]+\|?$", nxt):
                        i += 1
                        continue  # separator, or column-header row (⚠ is a legend)
                    ok = bool(route_re.search(row)) or any(
                        p in routed for p in PARAM_RE.findall(row))
                    if not ok:
                        self.add("warn-unratified", path, i + 1,
                                 "⚠ in this row routes to no ADR/OQ/GAP or "
                                 "routed parameter")
                    i += 1
                    continue
                # prose: search the enclosing paragraph
                start = i
                while start > 0 and clean[start - 1].strip():
                    start -= 1
                end = i
                while end + 1 < len(clean) and clean[end + 1].strip():
                    end += 1
                para = " ".join(clean[start:end + 1])
                ok = bool(route_re.search(para)) or any(
                    p in routed for p in PARAM_RE.findall(para))
                if not ok:
                    self.add("warn-unratified", path, i + 1,
                             "⚠ in this paragraph routes to no ADR/OQ/GAP or "
                             "routed parameter")
                i = end + 1

    def check_freshness(self):
        as_of = {}
        for doc in MUTABLE_DOCS:
            dates = set()
            for ln in self.files.get(doc, []):
                for m in AS_OF_RE.finditer(ln):
                    dates.add(m.group(1))
                for m in re.finditer(r"\(as of (\d{4}-\d{2}-\d{2})\)", ln):
                    dates.add(m.group(1))
            if len(dates) > 1:
                self.add("date-inconsistent", doc, 1,
                         f"multiple as-of dates: {sorted(dates)}")
            if dates:
                as_of[doc] = max(dates)
        baseline = None
        for ln in self.files.get(MATRIX_DOC, []):
            m = FRESHNESS_BASELINE_RE.search(ln)
            if m:
                baseline = m.group(1)
                break
        repo = os.path.dirname(os.path.abspath(self.root))

        def git(*args):
            try:
                return subprocess.run(["git", "-C", repo, *args],
                                      capture_output=True, text=True,
                                      timeout=30).stdout.strip()
            except Exception:
                return ""

        if baseline:
            ok = subprocess.run(
                ["git", "-C", repo, "cat-file", "-e", baseline + "^{commit}"],
                capture_output=True).returncode == 0
            if not ok:
                self.add("stale-baseline", MATRIX_DOC, 1,
                         f"baseline commit {baseline} not found in repository "
                         f"history")
        bar = as_of.get(MATRIX_DOC)
        if bar and git("rev-parse", "--is-inside-work-tree") == "true":
            dirty = set(git("status", "--porcelain", "--", self.root).splitlines())
            dirty_docs = {re.sub(r"^..\s+", "", d).split("/")[-1] for d in dirty}
            status_dirty = any(d in dirty_docs for d in MUTABLE_DOCS)
            for path in self.files:
                if path in MUTABLE_DOCS or path == "README.md" \
                        or path.startswith("decisions"):
                    continue
                last = git("log", "-1", "--format=%as", "--",
                           os.path.join(self.root, path))
                if last and last > bar:
                    self.add("stale-matrix", path, 1,
                             f"last commit {last} is newer than {MATRIX_DOC}'s "
                             f"as-of date {bar} — update the matrix in the same "
                             f"change")
                if os.path.basename(path) in dirty_docs and not status_dirty \
                        and last:
                    self.add("stale-matrix", path, 1,
                             "normative doc is dirty while the status doc is "
                             "clean — they must change together")

    # -- driver --------------------------------------------------------------

    def run(self, only=None):
        self.collect()
        groups = [
            ("id-undefined", self.check_reference_integrity),
            ("link", self.check_links),
            ("doc", self.check_doc_map),
            ("orphan", self.check_dead_weight),
            ("trace", self.check_traceability),
            ("inv", self.check_inv_shape),
            ("req", self.check_req_shape),
            ("gate", self.check_gates),
            ("adr", self.check_adr_log),
            ("shape", self.check_normative_shape),
            ("fresh", self.check_freshness),
        ]
        for _name, fn in groups:
            fn()
        if only:
            self.findings = [f for f in self.findings if f.check in only]


def load_ignores(root, no_ignore):
    path = os.path.join(root, "tools", "check_spec.ignore")
    rules = []
    if no_ignore or not os.path.exists(path):
        return rules
    with open(path, encoding="utf-8") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            parts = [p.strip() for p in ln.split("|")]
            if len(parts) == 3:
                rules.append(parts)
    return rules


def main(argv):
    args, root, fmt, min_sev, only, strict, no_ignore = argv[1:], "spec", "text", \
        None, None, False, False
    it = iter(args)
    for a in it:
        if a == "--format":
            fmt = next(it, "text")
        elif a == "--severity":
            min_sev = next(it, None)
        elif a == "--only":
            only = set(next(it, "").split(","))
        elif a == "--strict":
            strict = True
        elif a == "--no-ignore":
            no_ignore = True
        elif a == "--list-checks":
            for name, (sev, desc) in CHECKS.items():
                print(f"{sev}  {name:24} {desc}")
            return 0
        elif a.startswith("--"):
            print(f"unknown option {a}", file=sys.stderr)
            return 2
        else:
            root = a
    if not os.path.isdir(root):
        print(f"not a directory: {root}", file=sys.stderr)
        return 2
    if only and not only <= set(CHECKS):
        print(f"unknown checks: {sorted(only - set(CHECKS))}", file=sys.stderr)
        return 2

    suite = Suite(root)
    suite.run(only)

    ignores = load_ignores(root, no_ignore)
    kept = []
    for f in suite.findings:
        skip = False
        for check, glob, msg_re in ignores:
            import fnmatch
            if f.check == check and fnmatch.fnmatch(f.path, glob) \
                    and re.search(msg_re, f.msg):
                skip = True
                break
        if not skip:
            kept.append(f)
    order = {"E": 0, "W": 1, "I": 2}
    if min_sev:
        kept = [f for f in kept if order[f.sev] <= order.get(min_sev, 2)]
    kept.sort(key=lambda f: (order[f.sev], f.path, f.line))

    if fmt == "json":
        print(json.dumps([vars(f) for f in kept], indent=2))
    elif fmt == "github":
        for f in kept:
            kind = "error" if f.sev == "E" else "warning" if f.sev == "W" else "notice"
            print(f"::{kind} file={root}/{f.path},line={f.line},"
                  f"title={f.check}::{f.msg}")
    else:
        for f in kept:
            print(f"[{f.sev}] {f.check}: {f.path}:{f.line}: {f.msg}")
        counts = defaultdict(int)
        for f in kept:
            counts[f.sev] += 1
        print(f"\n{len(suite.files)} files · "
              f"{counts['E']} error(s), {counts['W']} warning(s), "
              f"{counts['I']} note(s)")
    errors = [f for f in kept if f.sev == "E" or strict]
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
