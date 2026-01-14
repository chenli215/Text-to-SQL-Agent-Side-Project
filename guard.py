# guard.py
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class GuardResult:
    ok: bool
    reasons: List[str] = field(default_factory=list)
    cleaned_sql: str = ""
    meta: Dict = field(default_factory=dict)


class SQLGuard:
    """
    Lightweight SQL guardrails for BigQuery:
    - Only allow a single SELECT/WITH statement
    - Block scripting / mutation keywords
    - Enforce table allowlist (tables referenced after FROM/JOIN)
    - IMPORTANT: avoid false positives from EXTRACT(... FROM ...)
    """

    def __init__(self, allowed_tables: Set[str], default_dataset_fq: str):
        self.allowed_tables = set(allowed_tables)
        self.default_dataset_fq = default_dataset_fq.strip()

        # NOTE: DO NOT block "end" because CASE ... END is valid SQL.
        self.blocked_keywords = {
            "insert", "update", "delete", "merge",
            "create", "drop", "alter", "truncate",
            "grant", "revoke",
            "call", "execute",
            "begin", "commit", "rollback",
            "transaction",
            "declare", "set",
        }

    def check(self, sql: str) -> GuardResult:
        raw_sql = (sql or "").strip()
        meta: Dict = {"raw_sql": raw_sql}

        if not raw_sql:
            return GuardResult(False, ["Empty SQL."], "", meta)

        cleaned = self._clean_sql(raw_sql)
        meta["cleaned_sql"] = cleaned

        # Multiple statement check: disallow ';' in the middle. Trailing ';' already removed.
        if ";" in cleaned:
            return GuardResult(
                False,
                ["Multiple statements are not allowed (found ';' in the middle of SQL)."],
                cleaned,
                meta,
            )

        first = (cleaned.lstrip().split(None, 1)[0].lower() if cleaned.strip() else "")
        meta["first_token"] = first
        if first not in ("select", "with"):
            return GuardResult(False, [f"Only SELECT/WITH queries are allowed (got '{first}')."], cleaned, meta)

        blocked_found = self._find_blocked_keywords(cleaned)
        meta["blocked_keywords_found"] = blocked_found
        if blocked_found:
            return GuardResult(False, [f"Disallowed keyword(s): {', '.join(blocked_found)}"], cleaned, meta)

        cte_names = self._extract_cte_names(cleaned)
        meta["cte_names"] = sorted(cte_names)

        table_refs_raw = self._extract_from_join_table_tokens(cleaned)
        meta["table_refs_raw"] = table_refs_raw

        table_refs_norm: List[str] = []
        for t in table_refs_raw:
            norm = self._normalize_table_ref(t, self.default_dataset_fq)
            if not norm:
                continue
            if norm in cte_names:
                continue
            table_refs_norm.append(norm)

        meta["table_refs_normalized"] = table_refs_norm

        bad = [t for t in table_refs_norm if t not in self.allowed_tables]
        meta["bad_table_refs"] = bad

        if bad:
            return GuardResult(False, ["Query references tables outside allowlist."], cleaned, meta)

        return GuardResult(True, [], cleaned, meta)

    # -------------------------
    # Internal helpers
    # -------------------------
    def _clean_sql(self, sql: str) -> str:
        s = sql.strip()
        s = re.sub(r"[;\s]+$", "", s)  # remove trailing semicolons/spaces
        return s

    def _find_blocked_keywords(self, sql: str) -> List[str]:
        found = []
        lower = sql.lower()
        for kw in sorted(self.blocked_keywords):
            if re.search(rf"\b{re.escape(kw)}\b", lower):
                found.append(kw)
        return found

    def _extract_cte_names(self, sql: str) -> Set[str]:
        lower = sql.lower()
        if not lower.lstrip().startswith("with"):
            return set()

        names = set()
        for m in re.finditer(r"\b([a-zA-Z_][\w]*)\s+as\s*\(", sql, flags=re.IGNORECASE):
            names.add(m.group(1))
        return names

    def _find_matching_paren(self, s: str, open_idx: int) -> int:
        """Find matching ')' for the '(' at open_idx. Return -1 if not found."""
        depth = 0
        for i in range(open_idx, len(s)):
            if s[i] == "(":
                depth += 1
            elif s[i] == ")":
                depth -= 1
                if depth == 0:
                    return i
        return -1

    def _mask_extract_from(self, sql: str) -> str:
        """
        EXTRACT has syntax: EXTRACT(part FROM expr)
        That 'FROM' is NOT a FROM clause. Mask it so our FROM/JOIN regex won't misread it.
        """
        lower = sql.lower()
        out = []
        i = 0
        while i < len(sql):
            # detect keyword EXTRACT (word boundary-ish)
            if lower.startswith("extract", i) and (i == 0 or not (lower[i - 1].isalnum() or lower[i - 1] == "_")):
                j = i + 7
                # skip whitespace
                while j < len(sql) and sql[j].isspace():
                    j += 1
                if j < len(sql) and sql[j] == "(":
                    end = self._find_matching_paren(sql, j)
                    if end != -1:
                        chunk = sql[i:end + 1]
                        # mask FROM inside EXTRACT(...)
                        chunk = re.sub(r"\bfrom\b", "FR0M", chunk, flags=re.IGNORECASE)
                        out.append(chunk)
                        i = end + 1
                        continue
            out.append(sql[i])
            i += 1
        return "".join(out)

    def _extract_from_join_table_tokens(self, sql: str) -> List[str]:
        """
        Extract table tokens only after FROM or JOIN.

        IMPORTANT:
        - Mask EXTRACT(... FROM ...) first, otherwise it'll produce false table refs like o.created_at
        """
        sql_scan = self._mask_extract_from(sql)

        tokens: List[str] = []

        # Backticked refs: FROM `project.dataset.table`
        for m in re.finditer(r"\b(from|join)\s+`([^`]+)`", sql_scan, flags=re.IGNORECASE):
            tokens.append(m.group(2).strip())

        # Non-backticked refs: FROM project.dataset.table OR dataset.table (but not FROM (subquery))
        for m in re.finditer(
            r"\b(from|join)\s+(?!\()([a-zA-Z_][\w\-]*(?:\.[a-zA-Z_][\w\-]*){1,2})\b",
            sql_scan,
            flags=re.IGNORECASE,
        ):
            ref = m.group(2).strip()
            if ref.lower() in ("select", "with"):
                continue
            tokens.append(ref)

        # De-dup preserve order
        seen = set()
        out = []
        for t in tokens:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def _normalize_table_ref(self, ref: str, default_dataset_fq: str) -> Optional[str]:
        r = (ref or "").strip().strip("`")
        if not r:
            return None

        parts = r.split(".")
        default_parts = default_dataset_fq.split(".")
        if len(default_parts) != 2:
            return None

        default_project, default_dataset = default_parts[0], default_parts[1]

        if len(parts) == 3:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"
        if len(parts) == 2:
            return f"{default_project}.{parts[0]}.{parts[1]}"
        if len(parts) == 1:
            return f"{default_project}.{default_dataset}.{parts[0]}"

        return None
