# External Module

`src/etl/modules/external.py`

**Stub -- pending implementation.** The External module exists in the codebase as a placeholder. Calling `execute()` raises `NotImplementedError("External modules are Phase 2")`.

## Intent

The C# implementation loads a user-supplied .NET assembly from disk via reflection and delegates execution to a named type implementing `IExternalStep`. The Python equivalent will need to provide an analogous plugin mechanism (e.g., dynamic module import via `importlib`) to allow teams to inject arbitrary Python logic into a job pipeline without modifying the framework.

## Current Constructor

Accepts `assembly_path` and `type_name` parameters to match the C# config contract, but neither is used at runtime.

## Config Properties

| JSON Property | Required | Description |
|---|---|---|
| `type` | Yes | `"External"` |
| `assemblyPath` | Yes | Path placeholder (not used -- stub) |
| `typeName` | Yes | Type name placeholder (not used -- stub) |

## Example

```json
{
  "type": "External",
  "assemblyPath": "placeholder/path",
  "typeName": "placeholder.TypeName"
}
```

When this module is implemented, the config contract will likely change to reference a Python module path and callable name rather than a .NET assembly.
