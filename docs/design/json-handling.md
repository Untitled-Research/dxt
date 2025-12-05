# JSON Output Handling in DXT

> Design document for handling JSON data in DXT pipelines

## Problem Statement

When extracting data from JSON-based sources (REST APIs, document databases like MongoDB/Firestore, JSON files), we need strategies for:

1. **Schema Specification**: How to define expected output structure
2. **Nested Data Handling**: How to deal with nested JSON objects and arrays
3. **Target Mapping**: How to map JSON to relational targets (PostgreSQL, etc.)
4. **Type Preservation**: When to keep JSON as-is vs. flatten to columns

## Design Goals

1. **Flexibility**: Support multiple strategies based on use case
2. **Simplicity**: Default behavior should work for common cases
3. **Consistency**: Same patterns work across JSON sources (APIs, MongoDB, files)
4. **Performance**: Minimize parsing overhead

---

## JSON Handling Strategies

### Strategy 1: JSON Path Field Mapping (Recommended Default)

Extract specific values from nested JSON using dot-notation paths.

```yaml
streams:
  - id: users_from_api
    source:
      type: http
      value: /users
    target: public.users
    fields:
      - id: user_id
        dtype: int64
        source: id                    # Top-level field
      - id: name
        dtype: string
        source: name
      - id: city
        dtype: string
        source: address.city          # Nested path
      - id: lat
        dtype: float64
        source: address.geo.lat       # Deeply nested
      - id: company_name
        dtype: string
        source: company.name
      - id: first_tag
        dtype: string
        source: tags.0                # Array index access
```

**Pros:**
- Explicit control over output schema
- Works well for relational targets
- Self-documenting

**Cons:**
- Requires knowing the source schema upfront
- Verbose for wide tables

**Implementation:**
- Already implemented in `RestExtractor._map_record()` and `_extract_value()`
- Uses dot-notation: `address.city`, `items.0.name`

---

### Strategy 2: Full JSON Column

Store entire nested objects as JSON/JSONB columns.

```yaml
streams:
  - id: users_with_json
    source:
      type: http
      value: /users
    target: public.users_raw
    fields:
      - id: user_id
        dtype: int64
        source: id
      - id: name
        dtype: string
        source: name
      # Store nested objects as JSON columns
      - id: address
        dtype: json
        source: address               # Keeps full nested object
      - id: company
        dtype: json
        source: company
```

**PostgreSQL Result:**
```sql
CREATE TABLE users_raw (
    user_id BIGINT,
    name TEXT,
    address JSONB,
    company JSONB
);
```

**Pros:**
- Preserves full structure
- Flexible querying with JSON operators
- No data loss

**Cons:**
- Less queryable without JSON operators
- Schema-on-read complexity
- Larger storage

**When to Use:**
- Source schema is unstable/evolving
- Need to preserve arbitrary nested data
- Target supports JSONB efficiently (PostgreSQL)

---

### Strategy 3: Auto-Flatten with Prefix

Automatically flatten nested objects to columns with path-based naming.

```yaml
streams:
  - id: users_flat
    source:
      type: http
      value: /users
    target: public.users_flat
    extract:
      json_handling: flatten          # Enable auto-flattening
      flatten_separator: "_"          # address.city → address_city
      flatten_arrays: false           # Don't expand arrays
      max_depth: 3                    # Limit recursion
```

**Source JSON:**
```json
{
  "id": 1,
  "name": "John",
  "address": {
    "city": "Boston",
    "geo": {"lat": 42.3, "lng": -71.0}
  }
}
```

**Flattened Output:**
```
id: 1
name: "John"
address_city: "Boston"
address_geo_lat: 42.3
address_geo_lng: -71.0
```

**Pros:**
- No manual field mapping needed
- Discoverable column names
- Works with schema inference

**Cons:**
- Column explosion with deeply nested data
- Array handling is complex
- Column name length issues

---

### Strategy 4: Hybrid Approach

Combine flattening with explicit overrides.

```yaml
streams:
  - id: users_hybrid
    source:
      type: http
      value: /users
    target: public.users
    extract:
      json_handling: flatten
      flatten_separator: "_"
    fields:
      # Override specific flattened fields
      - id: user_id
        source: id                    # Rename 'id' → 'user_id'
      - id: coordinates
        dtype: json
        source: address.geo           # Keep geo as JSON instead of flattening
      # Remaining fields auto-flattened
```

---

## Array Handling Strategies

### Option A: First Element Only

```yaml
fields:
  - id: primary_tag
    source: tags.0                    # Get first element
```

### Option B: JSON Array Column

```yaml
fields:
  - id: tags
    dtype: array                      # Store as ARRAY type or JSON
    source: tags
```

### Option C: Comma-Separated String

```yaml
fields:
  - id: tags_csv
    dtype: string
    source: tags
    transform: join(",")              # Future: transforms
```

### Option D: Normalize to Child Table (Future)

```yaml
streams:
  - id: users
    source: /users
    target: public.users
    # ... user fields

  - id: user_tags
    source: /users
    target: public.user_tags
    extract:
      array_field: tags               # Explode array to rows
    fields:
      - id: user_id
        source: id
      - id: tag
        source: _item                 # Current array element
```

---

## Schema Inference

For sources without explicit field definitions, infer schema from data.

### Inference Algorithm

```python
def infer_schema(records: list[dict]) -> list[Field]:
    """Infer schema from sample records."""
    field_types = {}

    for record in records:
        collect_types(record, "", field_types)

    return [
        Field(
            id=field_id,
            dtype=infer_dxt_type(types),
            nullable=(None in types)
        )
        for field_id, types in field_types.items()
    ]
```

### Type Inference Rules

| Python Type | DXT Type | Notes |
|-------------|----------|-------|
| `int` | INT64 | |
| `float` | FLOAT64 | |
| `bool` | BOOL | |
| `str` | STRING | |
| `dict` | JSON | Nested object |
| `list` | ARRAY | Or JSON |
| `None` | nullable=True | |
| Mixed | STRING | Fallback |

### Configuration

```yaml
extract:
  infer_schema: true                  # Enable inference
  infer_sample_size: 100              # Records to sample
  infer_nested_as: json               # json or flatten
```

---

## Implementation Plan

### Phase 1: JSON Path Mapping (Current)
- [x] Implement dot-notation path extraction
- [x] Support array index access (`.0`, `.1`)
- [x] Test with JSONPlaceholder API
- [ ] Add to documentation

### Phase 2: JSON Column Support
- [ ] Ensure JSONB type mapping in PostgresTypeMapper
- [ ] Test round-trip: API → Buffer → PostgreSQL JSONB
- [ ] Handle serialization in buffers (Parquet, CSV)

### Phase 3: Auto-Flatten
- [ ] Implement flatten algorithm with configurable separator
- [ ] Add depth limiting
- [ ] Add array handling options
- [ ] Schema inference integration

### Phase 4: Document Store Integration
- [ ] Apply patterns to MongoDB extractor
- [ ] Apply patterns to Firestore extractor
- [ ] Unified JSON handling in Buffer

---

## Configuration Reference

### Stream-Level JSON Options

```yaml
streams:
  - id: example
    source:
      type: http
      value: /data
      options:
        data_path: results            # Path to records array in response
        method: GET
        params:
          limit: 100

    extract:
      # JSON handling options
      json_handling: path_mapping     # path_mapping | flatten | preserve

      # For path_mapping (default)
      # Fields define explicit mappings

      # For flatten
      flatten_separator: "_"
      flatten_arrays: false
      max_depth: 5

      # For preserve
      # Entire record stored as single JSON column

    fields:
      - id: field_name
        dtype: string                 # int64, float64, bool, string, json, array
        source: path.to.field         # JSON path
        nullable: true
```

### Global JSON Defaults

```yaml
extract:
  json_handling: path_mapping
  infer_schema: false
  infer_sample_size: 100
```

---

## Examples

### Example 1: REST API to PostgreSQL with Nested Data

**Source (JSONPlaceholder /users):**
```json
{
  "id": 1,
  "name": "Leanne Graham",
  "email": "Sincere@april.biz",
  "address": {
    "street": "Kulas Light",
    "city": "Gwenborough",
    "zipcode": "92998-3874",
    "geo": {"lat": "-37.3159", "lng": "81.1496"}
  },
  "company": {
    "name": "Romaguera-Crona",
    "bs": "harness real-time e-markets"
  }
}
```

**Pipeline:**
```yaml
version: 1
name: api_to_postgres

source:
  connection: https://jsonplaceholder.typicode.com

target:
  connection: postgresql://user:pass@localhost/db

streams:
  - id: users
    source:
      type: http
      value: /users
    target: public.api_users
    fields:
      - id: id
        dtype: int64
      - id: name
        dtype: string
      - id: email
        dtype: string
      - id: city
        dtype: string
        source: address.city
      - id: zipcode
        dtype: string
        source: address.zipcode
      - id: lat
        dtype: float64
        source: address.geo.lat
      - id: lng
        dtype: float64
        source: address.geo.lng
      - id: company_name
        dtype: string
        source: company.name
      - id: company_data
        dtype: json
        source: company                # Keep as JSONB
```

**Result Table:**
```sql
CREATE TABLE api_users (
    id BIGINT,
    name TEXT,
    email TEXT,
    city TEXT,
    zipcode TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    company_name TEXT,
    company_data JSONB
);
```

### Example 2: MongoDB to PostgreSQL

```yaml
version: 1
name: mongo_to_postgres

source:
  provider: mongodb
  hosts: ["mongo:27017"]
  database: myapp

target:
  connection: postgresql://localhost/analytics

streams:
  - id: orders
    source:
      type: collection
      collection: orders
    target: public.orders
    fields:
      - id: order_id
        dtype: string
        source: _id
      - id: customer_id
        dtype: string
        source: customer._id
      - id: customer_email
        dtype: string
        source: customer.email
      - id: total_amount
        dtype: float64
        source: totals.grand_total
      - id: line_items
        dtype: json
        source: items                 # Array as JSON
      - id: first_item_sku
        dtype: string
        source: items.0.sku           # First item's SKU
```

---

## Open Questions

1. **Conflict Resolution**: What if `address_city` exists both as flattened and explicit field?
   - Proposal: Explicit fields always win

2. **Null Handling**: How to handle missing nested paths?
   - Current: Return `None`
   - Option: Add `default` field option

3. **Type Coercion**: What if `address.geo.lat` is string in source but `float64` in target?
   - Proposal: Add optional type coercion in field definition

4. **Array Explosion**: Should we support normalizing arrays to child tables?
   - Defer to future version

5. **JSONPath vs Dot Notation**: Should we support full JSONPath syntax (`$.store.book[0].title`)?
   - Current: Simple dot notation
   - Future: Consider JSONPath for complex queries
