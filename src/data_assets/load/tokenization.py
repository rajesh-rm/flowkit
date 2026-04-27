"""DataFrame-level tokenization helper.

Replaces values in declared sensitive columns with tokenized equivalents
fetched from the external tokenization service. Called from
``write_to_temp`` immediately before any DB write so plaintext PII never
touches a database — not even the temp_store table.
"""

from __future__ import annotations

import logging

import pandas as pd

from data_assets.extract.tokenization_client import TokenizationClient

logger = logging.getLogger(__name__)


def apply_tokenization(
    df: pd.DataFrame,
    sensitive_columns: list[str],
    client: TokenizationClient,
) -> pd.DataFrame:
    """Tokenize each sensitive column in *df* in place.

    For every column listed in *sensitive_columns* that exists in *df*:
      1. Skip if the column is entirely null.
      2. Collect the unique non-null values (deduplicated) and stringify
         them — the API contract is array-of-strings.
      3. Send the deduplicated list to ``client.tokenize`` and receive
         tokens at the same positions.
      4. Build a {original_string: token} mapping.
      5. Apply the mapping to every row in the column, leaving NULLs
         untouched.

    Mutates *df* in place and returns it for chaining (mirrors
    ``_coerce_datetime_strings`` in loader.py).

    Raises:
        TokenizationError: bubbled up from the client on API failure,
            length mismatch, or auth/config error. The caller (``write_to_temp``)
            does not catch this — the run aborts before any DB write.
    """
    for col in sensitive_columns:
        if col not in df.columns:
            continue

        series = df[col]
        non_null = series.dropna()
        if non_null.empty:
            continue

        # Stringify — the endpoint contract is array-of-strings, even for
        # numeric/boolean source columns. The same stringification is used
        # to build the mapping key so lookups are consistent.
        # dict.fromkeys preserves insertion order while deduplicating.
        unique_strs = list(dict.fromkeys(str(v) for v in non_null))

        tokens = client.tokenize(unique_strs)
        mapping = dict(zip(unique_strs, tokens, strict=True))

        df[col] = series.map(
            lambda v, _m=mapping: _m[str(v)] if pd.notna(v) else v,
        )

        logger.debug(
            "Tokenized column '%s': %d rows, %d unique values",
            col, len(non_null), len(unique_strs),
        )

    return df
