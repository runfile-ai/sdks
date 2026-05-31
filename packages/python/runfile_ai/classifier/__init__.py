"""Best-effort PII classifier + redactor (the client-side PII boundary).

Runs in the background flusher, before encryption. Identifies sensitive classes
(person_name, email_address, phone_number, address, ssn, tax_id, bank_account,
credit_card, dob, health_record, government_id, internal_id) and applies the
customer's redaction policy: drop / tokenize (via Vault) / hash / pass_through /
tokenize_with_fallback.

Honest caveat: regex/heuristic classification has false negatives. The real
controls are the customer's policy, deliberate schema ``data_classification``
tagging, and the fact that whatever is missed is still encrypted at rest.

Skeleton: signatures in place; bodies TODO.
"""

from __future__ import annotations

from typing import Any

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


class Classifier:
    def classify(self, payload: Any) -> Any:
        """Annotate the payload with detected PII spans/classes.

        v1 slice: pass-through (no detection yet). Real regex/heuristic
        classification across :data:`PII_CLASSES` lands in a dedicated slice.
        """
        return payload


class Redactor:
    def apply(self, classified: Any, policy: Any) -> Any:
        """Apply the policy treatment (drop/tokenize/hash/pass_through) per class.

        v1 slice: pass-through. The treatment engine (drop/tokenize-via-Vault/
        hash/pass_through/tokenize_with_fallback) lands with the classifier.
        """
        return classified

