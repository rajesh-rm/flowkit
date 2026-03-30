# Extending data_assets

## Adding a New API Source

### 1. Create a Token Manager

Add a new class in `extract/token_manager.py` or create a separate module:

```python
class MySourceTokenManager(TokenManager):
    def __init__(self):
        super().__init__()
        self._token = _resolver.resolve("MY_SOURCE_TOKEN")

    def get_token(self) -> str:
        return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}
```

### 2. Define Asset Classes

Create a new directory under `assets/` with asset classes:

```python
# assets/my_source/items.py
from data_assets.core.api_asset import APIAsset
from data_assets.core.registry import register

@register
class MySourceItems(APIAsset):
    name = "my_source_items"
    source_name = "my_source"
    target_table = "my_source_items"
    token_manager_class = MySourceTokenManager
    # ... columns, pagination, etc.

    def build_request(self, context, checkpoint=None):
        ...

    def parse_response(self, response):
        ...
```

### 3. Import in `__init__.py`

Add an import in `assets/my_source/__init__.py` so auto-discovery finds it.

## Adding a Transform Asset

```python
from data_assets.core.transform_asset import TransformAsset
from data_assets.core.registry import register

@register
class MyMetric(TransformAsset):
    name = "my_metric"
    target_schema = "mart"
    target_table = "my_metric"
    source_tables = ["raw_table_a", "raw_table_b"]
    columns = [...]

    def query(self, context):
        return "SELECT ... FROM raw.table_a JOIN raw.table_b ..."
```

## Custom Validation

Override `validate()` on any asset, or compose built-in validators:

```python
from data_assets.validation.validators import (
    validate_row_count, validate_pk_unique, compose_validators
)

def validate(self, df, context):
    checker = compose_validators(
        lambda d: validate_row_count(d, min_rows=10),
        lambda d: validate_pk_unique(d, self.primary_key),
    )
    return checker(df)
```

## Custom Load Strategy

The three built-in strategies (FULL_REPLACE, UPSERT, APPEND) cover most cases. To add a new one (e.g., SCD Type 2), implement `PromotionStrategy` and add it to `STRATEGY_MAP` in `load/strategies.py`.
