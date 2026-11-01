# CHIP-3: User-provided documentation for feature definitions

https://github.com/airbnb/chronon/issues/975

## Motivation

Programmatic access to feature documentation is useful for integrating with systems aimed at ML explainability (e.g. [SHAP](https://shap.readthedocs.io/en/latest/)) and feature discovery (e.g. feature catalogs).

Currently, it's possible to inspect Chronon definitions to determine _how_ a feature was computed, which is a type of documentation. However, there are other aspects of ML feature development that are not
captured and documented within the feature definition itself, for example: context, domain-specific knowledge, assumptions, caveats. This CHIP aims to fill those gaps with user-provided documentation.

## Proposed Change

There are 3 main changes in this proposal:

1. Add a `description` field to `MetaData` in the Thrift API.

```thrift
struct MetaData {
    ...
    xx: optional string description
}
```

2. Add a `metaData` field to `Aggregation` and `Derivation` in the Thrift API.

```thrift
struct Aggregation {
    ...
    xx: optional MetaData metaData
}
struct Derivation {
    ...
    xx: optional MetaData metaData
}
```

3. Update Chronon's Python API to handle an optional `description` parameter for the following objects: `Join`, `GroupBy`, `ExternalSource`, `ContextualSource`, `StagingQuery`, `Aggregation`, `Derivation`. When present, it will be passed through to the enclosed `MetaData`. Example implementation for `Derivation` (the simplest one):

```python
def Derivation(name: str, expression: str, description: Optional[str] = None) -> ttypes.Derivation:
    ...
    metadata = ttypes.MetaData(description=description) if description else None
    return ttypes.Derivation(name, expression, metadata)
```

## New or Changed Public Interfaces

The Thrift API will change. However, all the changes to the definition are additive, no existing fields will be touched.

There will be an effect (Chronon object diffs) on existing implementations that happen to coincidentally pass `description` as `kwargs`, since those arbitrary params get thrown into `MetaData.customJson`.
However, this is not a public API contract and would not be expected to have an effect on feature computation.

## Rejected Alternatives

- Support a `description` parameter at the Python API level without changes to the Thrift definitions. In this implementation, the descriptions would be collected and bubbled up to the top-level object (e.g. `Join` or `GroupBy`), similar to how `tags` are handled ([code](https://github.com/airbnb/chronon/blob/3e138e86d9922a6742709adc69b9b6ccbd18852c/api/py/ai/chronon/group_by.py#L529)).
  - Pros
    - Consistency with implementation for `tags`.
    - No changes to the public Thrift API.
  - Cons
    - Obscures support for feature documentation, since data in `customJson` looks adhoc and generally less discoverable.
    - Would require some sort of mapping from objects to descriptions in the top-level metadata. E.g. mapping a description to its corresponding derivation, perhaps through output columns. This could be brittle and significantly increases the size of data in `customJson`.
    - Bubbling up parameters via dynamic and undocumented fields (as it's done with `tags`, which are not part of the Thrift definition for `Aggregation`) is less maintainable as things may easily break if code is moved around without handling those fields correctly.
- Defining a separate Thrift struct for holding documentation.
  - Pros
    - Isolation from existing `MetaData` definition, which has a lot of fields that are only relevant in specific contexts.
  - Cons
    - Slippery slope towards having various diffent types of metadata.
    - Unclear what the actual definition would be and the boundary with the generic `MetaData` definition. After all, data description is a type of metadata.