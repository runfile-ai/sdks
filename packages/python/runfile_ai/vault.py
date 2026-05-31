"""Vault tokenization client.

The redactor's ``tokenize`` / ``tokenize_with_fallback`` treatments replace a PII
span with an opaque ``tok_*`` reference whose cleartext mapping lives in the
Vault (under a separate KMS key, resolvable only with justification). The Vault
is a SEPARATE service from the Ingest API (``vault.<region>``), authenticated
with the same ``rf_*`` bearer key.

Tokenization is best-effort: on any failure ``tokenize`` returns ``None`` and the
redactor drops the value (never leaks cleartext) and flags it.

Note the contract mismatch handled here: the redactor's classification names
(``email_address``, ``us_ssn``, ``credit_card`` …) are mapped to the Vault's
``ClassificationEnum`` (``email``, ``phone``, ``tax_id_partial``, ``account_id``,
``other_identifier`` …) before the call.
"""

from __future__ import annotations

import httpx

from ._constants import DEFAULT_REGION

#: Map the redactor's detected class -> the Vault ClassificationEnum.
_VAULT_CLASSIFICATION = {
    "email_address": "email",
    "email": "email",
    "phone_number": "phone",
    "phone": "phone",
    "person_name": "person_name",
    "address": "address",
    "dob": "dob",
    "us_ssn": "tax_id_partial",
    "ssn": "tax_id_partial",
    "tax_id": "tax_id_partial",
    "credit_card": "account_id",
    "bank_account": "account_id",
    "iban": "account_id",
    "account_id": "account_id",
    "user_handle": "user_handle",
    "internal_id": "other_identifier",
}
_DEFAULT_VAULT_CLASSIFICATION = "other_identifier"

_MAX_VALUE_LEN = 4096  # Vault TokenizeRequest.value max


def default_vault_base_url(region: str = DEFAULT_REGION) -> str:
    return f"https://vault.{region}.runfile.ai"


class VaultClient:
    """Thin client for the Vault's SDK-facing tokenize endpoint."""

    def __init__(self, *, client: httpx.Client, base_url: str, api_key: str) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def tokenize(self, value: str, classification: str) -> str | None:
        """Tokenize one value; return ``tok_*`` or ``None`` on any failure.

        The signature matches what :class:`runfile_ai.classifier.Redactor`
        expects for its ``tokenizer`` callable.
        """
        if not value or len(value) > _MAX_VALUE_LEN:
            return None
        vault_classification = _VAULT_CLASSIFICATION.get(
            classification, _DEFAULT_VAULT_CLASSIFICATION
        )
        try:
            resp = self._client.post(
                f"{self._base_url}/v1/tokenize",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={"value": value, "classification": vault_classification},
            )
        except httpx.HTTPError:
            return None
        if resp.status_code != 200:
            return None
        try:
            token = resp.json()["token"]
        except (ValueError, KeyError):
            return None
        return token if isinstance(token, str) else None
