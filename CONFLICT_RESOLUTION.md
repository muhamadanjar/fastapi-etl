# Conflict Resolution Strategy for Phase 6 (Load)

**Document Purpose:** Define how conflicting data is resolved when loading standardized records into processed entities.

**Version:** 1.0  
**Last Updated:** 2026-05-02  
**Phase:** Phase 6 (Load)

---

## Overview

During Phase 6 (Load), when a standardized record matches an existing entity, the system must decide which data values to keep. This document defines the conflict resolution strategies available and their use cases.

### Key Concepts

**Conflict:** Occurs when the same field has different values in:
- **Existing Entity Data:** Current value in the processed.entities table
- **New Standardized Data:** Value from the standardized_data table being loaded

**Resolution:** Decision-making process to determine which value "wins"

**Strategy:** Algorithm/rule set for making the resolution decision

---

## Conflict Resolution Strategies

### 1. Newer Wins (Default)

**Strategy Name:** `newer_wins`

**Description:** New standardized data values unconditionally replace existing entity data.

**When to Use:**
- Data sources are trusted and fresh
- New data is always more accurate than old data
- Timestamp-based freshness can be relied upon
- Simple use case: just overwrite everything

**Implementation:**
```python
merged = existing_data.copy()
merged.update(new_data)  # New values override all
```

**Example:**
```
Field: "email"
Existing: "john.old@example.com"
New:      "john.new@example.com"
Result:   "john.new@example.com"  (always use new)
```

**Pros:**
- Simple, predictable, deterministic
- No configuration needed
- Fast execution

**Cons:**
- May overwrite correct existing data with incorrect new data
- No intelligence about data quality
- Risk of losing manually-curated values

---

### 2. Score-Based (Recommended for Matching)

**Strategy Name:** `score_based`

**Description:** Resolution based on confidence/quality scores. Higher confidence wins per-field.

**When to Use:**
- Data quality varies significantly
- Want to preserve high-confidence existing data
- New data has moderate-to-low confidence scores
- Smart merge needed without full validation

**Thresholds:**
- **Confidence >= 0.9:** Use new value (very confident)
- **Confidence 0.7-0.9:** Selective merge (moderate confidence)
- **Confidence < 0.7:** Keep existing (low confidence)

**Implementation:**
```python
if confidence_score >= 0.9:
    merged.update(new_data)  # Very confident, trust new data
else:
    # Selective merge for moderate confidence
    for key, new_value in new_data.items():
        if key not in merged:
            merged[key] = new_value  # Always add new fields
        elif isinstance(new_value, dict) and isinstance(merged[key], dict):
            merged[key].update(new_value)  # Merge nested objects
        elif isinstance(new_value, (int, float)):
            merged[key] = max(merged[key], new_value)  # Use higher value
```

**Example 1: High Confidence**
```
Confidence: 0.95
Field: "email"
Existing: "john@old.com"
New:      "john@new.com"
Result:   "john@new.com"  (0.95 >= 0.9, use new)
```

**Example 2: Moderate Confidence**
```
Confidence: 0.80
Field: "age"
Existing: 30
New:      31
Result:   31  (both numbers, use max)

Field: "country"
Existing: "USA"
New:      "Canada"
Result:   "USA"  (both strings, keep existing due to moderate confidence)
```

**Pros:**
- Intelligent field-by-field decisions
- Respects data quality signals
- Configurable thresholds
- Balances trust and caution

**Cons:**
- More complex logic
- Requires accurate confidence scores
- Different behavior per field type

---

### 3. Conservative

**Strategy Name:** `conservative`

**Description:** Existing data is never overwritten. Only new fields are added.

**When to Use:**
- Existing data has been manually curated
- Historical values are valuable
- Risk of incorrect new data is high
- Gradual data updates only

**Implementation:**
```python
merged = existing_data.copy()
for key, new_value in new_data.items():
    if key not in merged:
        merged[key] = new_value  # Only add new fields
    # Existing fields unchanged
```

**Example:**
```
Field: "name"
Existing: "John Smith" (manually verified)
New:      "Jon Smith"
Result:   "John Smith"  (keep existing)

Field: "phone_verified_by_admin"
Existing: not present
New:      true
Result:   true  (new field, add it)
```

**Pros:**
- Protects manually-curated data
- Zero risk of data degradation
- Auditable: clear what didn't change
- Best for slow-moving reference data

**Cons:**
- May never update incorrect existing data
- Can accumulate stale values over time
- Requires manual intervention to fix errors

---

### 4. Intelligent Merge

**Strategy Name:** `merge`

**Description:** Field-type-aware merging with recursive object handling and list deduplication.

**When to Use:**
- Handling complex nested data structures
- Need to preserve both old and new information
- Arrays/lists contain complementary data
- Objects contain partial information from different sources

**Implementation:**
```python
for key, new_value in new_data.items():
    if key not in merged:
        merged[key] = new_value
    elif isinstance(new_value, dict) and isinstance(merged[key], dict):
        # Recursively merge nested objects
        merged[key] = {**merged[key], **new_value}
    elif isinstance(new_value, list):
        # Merge lists, remove duplicates
        merged[key] = list(set(merged.get(key, []) + new_value))
    else:
        # For scalars, keep existing
        pass
```

**Example 1: Nested Objects**
```
Field: "address"
Existing: {"street": "123 Main St", "city": "New York"}
New:      {"city": "Boston", "zip": "02101"}
Result:   {"street": "123 Main St", "city": "Boston", "zip": "02101"}
```

**Example 2: Lists**
```
Field: "tags"
Existing: ["active", "verified"]
New:      ["verified", "premium"]
Result:   ["active", "verified", "premium"]  (unique merged)
```

**Pros:**
- Maximizes data retention
- Handles complex structures naturally
- Idempotent merging
- Good for multi-source data

**Cons:**
- May create inconsistencies if fields contradict
- More processing overhead
- Complex object merging can be unpredictable
- Duplicate detection in lists is basic (equality-based)

---

## Field-Type-Specific Handling

### Strings
- **newer_wins:** Use new value
- **score_based:** Keep existing unless high confidence
- **conservative:** Keep existing
- **merge:** Keep existing

### Numbers (Int, Float, Decimal)
- **newer_wins:** Use new value
- **score_based:** Use max(existing, new)
- **conservative:** Keep existing
- **merge:** Keep existing

### Booleans
- **newer_wins:** Use new value
- **score_based:** Use new if high confidence, else keep existing
- **conservative:** Keep existing
- **merge:** Keep existing

### Dates/Timestamps
- **newer_wins:** Use new value (later timestamp)
- **score_based:** Use newer timestamp if high confidence
- **conservative:** Keep existing
- **merge:** Use max(existing, new)

### Arrays/Lists
- **newer_wins:** Use new value
- **score_based:** Merge uniquely
- **conservative:** Keep existing
- **merge:** Merge uniquely, deduplicate

### Objects/Dicts
- **newer_wins:** Shallow merge (new overwrites existing keys)
- **score_based:** Selective field merge based on confidence
- **conservative:** Keep existing
- **merge:** Deep/recursive merge

---

## Configuration

### Default Configuration

```python
load_config = {
    "entity_type": "PERSON",
    "key_fields": ["id", "name"],
    "batch_size": 1000,
    "similarity_threshold": 0.85,
    "conflict_resolution_strategy": "newer_wins"  # Default
}
```

### Per-Field Configuration (Future Enhancement)

```python
# Define strategy per field or field pattern
conflict_rules = {
    "email": "score_based",           # Smart email handling
    "phone": "score_based",           # Smart phone handling
    "verified_*": "conservative",     # Keep verified fields
    "score": "score_based",           # Use score-based for scores
    "tags": "merge",                  # Merge tag lists
    "address.*": "merge"              # Merge address objects
}
```

---

## Master Entity ID Assignment

When duplicates are identified, the master entity is determined by:

1. **First match wins:** The earliest existing entity becomes the master
2. **ID ordering:** Lower entity_id becomes the master
3. **Confidence-based:** Higher confidence entity becomes the master (future enhancement)

### Duplicate Tracking

```
Master Entity:
  - entity_id: 1
  - duplicate_count: 5
  - master_entity_id: 1 (self-reference)

Duplicate Entities:
  - entity_id: 2, master_entity_id: 1
  - entity_id: 3, master_entity_id: 1
  - entity_id: 4, master_entity_id: 1
  - entity_id: 5, master_entity_id: 1
  - entity_id: 6, master_entity_id: 1

EntityRelationship:
  - entity_from: 2, entity_to: 1, relationship_type: "duplicate_of"
  - entity_from: 3, entity_to: 1, relationship_type: "duplicate_of"
  - ... (etc)
```

---

## Transaction and Rollback

### Transaction Boundaries

Phase 6 uses **nested transactions (savepoints)** for atomicity:

```python
transaction = db.begin_nested()  # Start savepoint

try:
    for record in records:
        # Process record
        # INSERT entity
        # INSERT lineage
        # INSERT relationships
        db.commit()  # Flush changes
        
    transaction.commit()  # Commit savepoint
    
except Exception as e:
    transaction.rollback()  # Rollback entire batch
    # Log error
```

### Failure Scenarios

| Scenario | Action | Rollback |
|----------|--------|----------|
| Record 1-5 success, Record 6 fails | Roll back entire batch | Complete batch |
| Entity creation fails | Roll back record + transaction | Yes |
| Relationship creation fails | Roll back record + transaction | Yes |
| Lineage creation fails | Roll back record + transaction | Yes |

---

## Decision Tree

```
New record matches existing entity?
├─ YES (exact hash match)
│  └─ confidence = 1.0
│     └─ No merge needed, just update metadata
│
├─ YES (fuzzy match > threshold)
│  ├─ confidence > 0.9?
│  │  ├─ YES → Apply "newer_wins"
│  │  │       All new data overwrites existing
│  │  │
│  │  └─ NO → Apply chosen strategy
│  │        Score-based, Conservative, or Merge
│  │
│  └─ Update entity
│     ├─ INSERT change_log
│     ├─ UPDATE entities
│     ├─ INSERT data_lineage
│     └─ records_merged += 1
│
├─ DUPLICATE (score very high but entity different)
│  └─ Mark duplicate
│     ├─ UPDATE master: duplicate_count++, master_entity_id=primary
│     ├─ INSERT entity_relationships (duplicate_of)
│     ├─ INSERT data_lineage
│     └─ records_duplicated += 1
│
└─ NEW (no match found)
   └─ Create new entity
      ├─ INSERT entities
      ├─ INSERT data_lineage
      └─ records_loaded += 1
```

---

## Examples by Use Case

### Use Case 1: Customer Data (Daily Load)

**Config:**
```python
{
    "entity_type": "CUSTOMER",
    "conflict_resolution_strategy": "score_based",
    "similarity_threshold": 0.90
}
```

**Rationale:**
- Customer data changes daily but has quality variance
- Email/phone may be incorrect in source system
- Score-based allows selective updates
- High threshold (0.90) for matching

---

### Use Case 2: Reference Data (Lookup Tables)

**Config:**
```python
{
    "entity_type": "COUNTRY",
    "conflict_resolution_strategy": "conservative",
    "similarity_threshold": 0.95
}
```

**Rationale:**
- Reference data is manually curated
- Risk of corruption > value of updates
- Very high threshold (0.95) for matching
- Conservative keeps existing definitions

---

### Use Case 3: Event Logs (Streaming)

**Config:**
```python
{
    "entity_type": "EVENT",
    "conflict_resolution_strategy": "newer_wins",
    "similarity_threshold": 0.80
}
```

**Rationale:**
- Events are immutable, newer is always correct
- High confidence in source data
- Lower threshold (0.80) to find all related events
- Simpler processing for high-volume

---

### Use Case 4: Complex Profiles (Multi-Source)

**Config:**
```python
{
    "entity_type": "PERSON",
    "conflict_resolution_strategy": "merge",
    "similarity_threshold": 0.85
}
```

**Rationale:**
- Person profiles have nested structures (addresses, phones)
- Data comes from multiple sources with complementary info
- Merge strategy preserves all information
- Medium threshold (0.85) for moderate matching

---

## Monitoring and Alerts

### Metrics to Track

1. **Merge Rate:** (records_merged / records_loaded) %
2. **Duplicate Rate:** (records_duplicated / records_loaded) %
3. **Field Override Rate:** How many field updates per record
4. **Strategy Effectiveness:** Success rate of chosen strategy

### Alert Conditions

- Merge rate > 50% (possible duplicate detection issue)
- Duplicate rate > 30% (check threshold sensitivity)
- High conflict rate (strategy may need adjustment)
- Transaction rollbacks > 5% (data quality issue)

---

## Testing

### Test Cases

1. **Exact Match:** Record with identical hash → No merge
2. **Fuzzy Match (high confidence):** Similar record with 0.95 score → Merge with newer_wins
3. **Fuzzy Match (medium confidence):** Similar record with 0.80 score → Selective merge
4. **Duplicate:** Very similar record → Mark duplicate_of
5. **New Record:** No match found → Create new entity
6. **Nested Object Merge:** Complex address → Preserve and merge fields
7. **Array Merge:** List of tags → Deduplicate and combine
8. **Transaction Rollback:** Failure mid-batch → Rollback all

### Example Test Query

```sql
-- Verify master entity has correct duplicate_count
SELECT 
  e.entity_id,
  e.duplicate_count,
  e.master_entity_id,
  COUNT(er.relationship_id) as relationship_count
FROM processed.entities e
LEFT JOIN processed.entity_relationships er 
  ON e.entity_id = er.entity_to 
  AND er.relationship_type = 'duplicate_of'
WHERE e.entity_type = 'PERSON'
GROUP BY e.entity_id
HAVING e.duplicate_count != COUNT(er.relationship_id);
-- Should return 0 rows (no mismatches)
```

---

## Summary Table

| Strategy | Data Preservation | Complexity | Trust Required | Best For |
|----------|------------------|-----------|----------------|----------|
| newer_wins | Low | Low | High | Trusted, fresh data |
| score_based | Medium | Medium | Medium | Quality-varying data |
| conservative | High | Low | Low | Curated, reference data |
| merge | Highest | High | Medium | Multi-source complexity |

---

**Document Status:** Approved for Production  
**Next Review:** 2026-06-02
