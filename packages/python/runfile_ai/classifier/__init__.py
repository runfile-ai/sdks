"""PII classifier + redactor — the client-side PII boundary.

Runs in the background flusher, before encryption. Detects sensitive classes in
the payload's string leaves and applies the customer's redaction policy
treatment per class: ``drop`` / ``hash`` / ``pass_through`` (and, once the Vault
client lands, ``tokenize`` / ``tokenize_with_fallback``).

Honest caveats:
- Regex/heuristic detection has false negatives. The real controls are the
  customer's policy, deliberate schema ``data_classification`` tagging, and the
  fact that whatever is missed is still encrypted at rest.
- It is policy-driven: only classes the policy has a rule for are acted on
  (others pass through). Trial tenants get a safe-default policy server-side.
- A string leaf that is itself a JSON object/array (e.g. an MCP tool result
  delivered as text) is parsed and redacted field-by-field, then re-embedded as
  a string — otherwise value-anchored detectors would miss values buried in the
  blob. A bare first name / standalone date still needs a precise policy pattern
  or schema-level ``data_classification`` tagging; that's a policy concern.
- ``tokenize``/``tokenize_with_fallback`` currently fall back to ``drop`` (no
  Vault round-trip yet) — wiring the Vault is the next slice. Dropped, not leaked.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from ..policy import RedactionPolicy

CLASSIFIER_VERSION = "1.0.0"

PII_CLASSES = (
    "person_name",
    "email_address",
    "phone_number",
    "address",
    "ssn",
    "tax_id",
    "bank_account",
    "credit_card",
    "dob",
    "health_record",
    "government_id",
    "internal_id",
)

# Built-in best-effort detectors, keyed by the policy `classification` name.
# Well-defined formats only; name/address/dob detection is deferred (low-precision).
_DETECTORS: dict[str, re.Pattern[str]] = {
    "email_address": re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"),
    "us_ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "phone_number": re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "credit_card": re.compile(r"\b(?:\d[ -]?){13,19}\b"),
    "iban": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
}

# Apply more specific patterns first so they win overlapping spans.
_PRIORITY = ["us_ssn", "credit_card", "iban", "email_address", "phone_number"]


@dataclass
class RedactionResult:
    value: Any
    redacted_classes: list[str] = field(default_factory=list)
    tokenized_classes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Rule:
    classification: str
    treatment: str
    pattern: re.Pattern[str]


def _as_json_container(text: str) -> Any | None:
    """Return the parsed value if ``text`` is a JSON object/array, else ``None``.

    Restricted to objects/arrays (never bare scalars) so we never coerce a string
    like ``"123"`` to an int or ``"true"`` to a bool, and only when it parses
    cleanly. Used to redact structured fields inside JSON-string payloads (tool
    results), which a flat-string walk would otherwise treat as one opaque leaf.
    """
    stripped = text.strip()
    if not stripped or stripped[0] not in "{[":
        return None
    try:
        parsed = json.loads(stripped)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, (dict, list)) else None


def _luhn_ok(value: str) -> bool:
    digits = [int(c) for c in value if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    checksum = 0
    for index, digit in enumerate(reversed(digits)):
        if index % 2 == 1:  # double every second digit from the right
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0


class Redactor:
    """Applies a redaction policy to a payload before encryption."""

    def __init__(self, tokenizer: Callable[[str, str], str | None] | None = None) -> None:
        # tokenizer(value, classification) -> tok_* | None. None until the Vault
        # client is wired; treatments needing it fall back to drop.
        self._tokenizer = tokenizer

    def apply(self, payload: Any, policy: "RedactionPolicy | None") -> RedactionResult:
        rules = self._build_rules(policy)
        if not rules:
            return RedactionResult(value=payload)

        redacted: set[str] = set()
        tokenized: set[str] = set()

        def redact_str(text: str) -> str:
            for rule in rules:

                def repl(match: re.Match[str]) -> str:
                    raw = match.group()
                    if rule.classification == "credit_card" and not _luhn_ok(raw):
                        return raw  # candidate failed Luhn — not a real PAN
                    return self._treat(raw, rule, redacted, tokenized)

                text = rule.pattern.sub(repl, text)
            return text

        def walk(value: Any) -> Any:
            if isinstance(value, str):
                # Tool results commonly arrive as a JSON *string* — one opaque leaf
                # (e.g. an MCP text result). Walking it as a flat string hides the
                # real fields from value-anchored detectors: a `^\d{4}-\d{2}-\d{2}$`
                # dob rule can't match a date embedded mid-blob, so it would leak
                # while unanchored rules (email, full name) still hit substrings.
                # Parse embedded JSON objects/arrays so redaction sees structured
                # leaves, then re-embed as a string to preserve the payload's shape.
                container = _as_json_container(value)
                if container is not None:
                    return json.dumps(walk(container), separators=(",", ":"), default=str)
                return redact_str(value)
            if isinstance(value, dict):
                return {k: walk(v) for k, v in value.items()}
            if isinstance(value, list):
                return [walk(v) for v in value]
            return value

        return RedactionResult(
            value=walk(payload),
            redacted_classes=sorted(redacted),
            tokenized_classes=sorted(tokenized),
        )

    def _treat(
        self, raw: str, rule: _Rule, redacted: set[str], tokenized: set[str]
    ) -> str:
        if rule.treatment == "pass_through":
            return raw
        if rule.treatment == "hash":
            redacted.add(rule.classification)
            return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if rule.treatment in ("tokenize", "tokenize_with_fallback"):
            if self._tokenizer is not None:
                token = self._tokenizer(raw, rule.classification)
                if token is not None:
                    tokenized.add(rule.classification)
                    return token
            # No Vault yet (or tokenize failed): drop rather than leak.
            redacted.add(rule.classification)
            return f"[REDACTED:{rule.classification}]"
        # default: drop
        redacted.add(rule.classification)
        return f"[REDACTED:{rule.classification}]"

    @staticmethod
    def _build_rules(policy: "RedactionPolicy | None") -> list[_Rule]:
        if policy is None:
            return []
        by_class: dict[str, _Rule] = {}
        for raw_rule in policy.classification_rules:
            classification = raw_rule.get("classification")
            treatment = raw_rule.get("treatment")
            if not classification or not treatment:
                continue
            detector = raw_rule.get("detector") or {}
            pattern_src = detector.get("pattern") if isinstance(detector, dict) else None
            if pattern_src:
                try:
                    pattern = re.compile(pattern_src)
                except re.error:
                    continue
            elif classification in _DETECTORS:
                pattern = _DETECTORS[classification]
            else:
                continue  # no way to detect this class
            by_class[classification] = _Rule(classification, treatment, pattern)
        # Stable, specificity-first order.
        ordered = [by_class[c] for c in _PRIORITY if c in by_class]
        ordered += [r for c, r in by_class.items() if c not in _PRIORITY]
        return ordered
