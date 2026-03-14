"""External module implementations — one file per C# ExternalModules class.

External modules intentionally iterate row-by-row (via iterrows()) rather than
using vectorized pandas operations.  This mirrors the C# production code's
per-row processing semantics for behavioral fidelity — the goal is identical
output, not pandas-idiomatic speed.
"""
