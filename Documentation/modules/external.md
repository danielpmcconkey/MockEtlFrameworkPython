# External Module

`src/etl/modules/external.py`

The External module dispatches execution to user-supplied Python functions
registered under a `typeName` string. In C# this used `Assembly.LoadFrom()` +
reflection to load a DLL at runtime. In Python, a registry maps typeName
strings to callables discovered by scanning module directories at startup.

## How It Works

1. On the first `External.execute()` call, `_load_all()` scans two directories
   for `.py` files (skipping `_`-prefixed files) and loads each via `importlib`:
   - **OG modules:** `src/etl/modules/externals/` (73 files)
   - **RE modules:** `RE/externals/` (populated by the publisher at runtime)
2. Each module file calls `register(type_name, fn)` at import time, adding
   itself to the global `_REGISTRY` dict.
3. When `execute()` is called, it looks up the `typeName` from the job conf
   in the registry and calls the corresponding function with `shared_state`.

No hardcoded import lists. Drop a `.py` file in either directory and the
framework picks it up on next run. Remove a file and nothing breaks.

## Config Properties

| JSON Property | Required | Description |
|---|---|---|
| `type` | Yes | `"External"` |
| `assemblyPath` | Yes | Path string (retained for C# config compatibility, not used at runtime) |
| `typeName` | Yes | Registry key, e.g. `"ExternalModules.AccountCustomerDenormalizer"` |

## Example

```json
{
  "type": "External",
  "assemblyPath": "ExternalModules/bin/Release/net8.0/ExternalModules.dll",
  "typeName": "ExternalModules.AccountCustomerDenormalizer"
}
```

## Writing an External Module

Each external module file must:
1. Define `def execute(shared_state: dict[str, object]) -> dict[str, object]`
2. Call `register()` at module level to bind a typeName to the function

```python
from etl.modules.external import register

def execute(shared_state: dict[str, object]) -> dict[str, object]:
    # ... your logic here ...
    return shared_state

register("ExternalModules.MyProcessor", execute)
```
