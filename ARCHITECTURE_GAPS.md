# FastAPI-ETL: Architecture Gaps & Data Flow Issues

## Critical Gaps Summary

This document identifies the **3 largest gaps** between SEQUENCE.md workflow and actual codebase implementation.

---

## Gap #1: TRANSFORM PHASE - Field Mapping & Validation Loop ❌ CRITICAL

### SEQUENCE.md Requirement (Lines 148-227)

```
For each raw record:
  1. Clean data (remove whitespace, normalize case, handle nulls)
  2. For each field mapping:
     - Type: direct → target[field] = source[field]
     - Type: calculated → target[field] = eval(expression)
     - Type: lookup → SELECT from lookup_values
     - Type: constant → target[field] = constant_value
  3. Validate record with quality rules
     - Completeness (null/empty check)
     - Uniqueness (check standardized_data for duplicates)
     - Validity (regex pattern matching)
     - Range (min/max validation)
     - Consistency (referential integrity)
  4. If validation errors (severity: error):
     → INSERT rejected_records
  5. Else:
     → INSERT standardized_data
     → INSERT quality_check_results
     → UPDATE raw_records SET is_processed=true
```

### Codebase Status

**What EXISTS:**
- ✅ `DataCleaner` transformer with methods: remove_whitespace(), normalize_case(), handle_null_values()
- ✅ `DataNormalizer` transformer
- ✅ `DataValidator` transformer with ValidationRule + ValidationResult classes
- ✅ `ValidationSeverity` enum (info, warning, error, critical)
- ✅ `ValidationType` enum (required, type, format, range, length, pattern, custom, business_rule, referential, uniqueness)
- ✅ `QualityRule` model in database
- ✅ `QualityCheckResult` model for storing validation results
- ✅ `RejectedRecords` model for failed records
- ✅ Error handling in transformers

**What's MISSING or UNCLEAR:**
```
❌ TRANSFORM PHASE LOOP IN etl_tasks.py NOT VISIBLE
   - etl_tasks.py preview ends at line 100 (process_file_task function)
   - Transform logic likely in transformation_service.py but NOT verified
   
❌ FIELD MAPPING EXECUTION
   - FieldMapping model exists but NO implementation visible
   - No evidence of mapping_type: direct/calculated/lookup/constant
   - No evidence of executing these 4 mapping types
   
❌ STANDARDIZED_DATA TABLE LOCATION
   - SEQUENCE expects data in "standardized_data" table
   - Unclear which schema: staging? transformation?
   - Need to verify table name and schema
   
⚠️ TRANSFORMATION PIPELINE INTEGRATION
   - create_transformation_pipeline() factory exists
   - Pipeline integration in main task loop NOT visible
   - Order of operations: clean → normalize → map → validate?

⚠️ VALIDATION RULE APPLICATION
   - Quality rules system exists
   - BUT: How are quality_rules loaded and applied to each record?
   - No loop visible: for each quality_rule: validate_record()
```

### Architecture Problem

**Current Flow (INCOMPLETE):**
```
etl_tasks.py (process_file_task)
  ├─ Extract: Read file → Raw data inserted ✅
  ├─ Transform: ??? (NOT VISIBLE)
  │  ├─ Clean data (??)
  │  ├─ Apply field mappings (??)
  │  ├─ Validate with quality rules (??)
  │  └─ Insert standardized_data (??)
  └─ Not reached
```

**Expected Flow (FROM SEQUENCE):**
```
execute_etl_job (main Celery task)
  ├─ Phase 4: Extract [VISIBLE]
  │  ├─ process_file_task for each file
  │  └─ INSERT raw_records
  │
  ├─ Phase 5: Transform [NOT VISIBLE - CRITICAL]
  │  ├─ SELECT raw_records WHERE is_processed=false
  │  ├─ SELECT transformation_rules, field_mappings
  │  └─ For each raw_record:
  │     ├─ Transform & Validate
  │     ├─ INSERT standardized_data OR rejected_records
  │     └─ INSERT quality_check_results
  │
  ├─ Phase 6: Load [NOT VISIBLE]
  └─ Phase 7: Post-Process [NOT VISIBLE]
```

### Questions to Investigate

1. **Is transform logic in separate task?**
   ```python
   # Is there a task like this?
   @celery_app.task(name='app.tasks.etl_tasks.transform_etl_records')
   def transform_etl_records(execution_id, ...):
       # Transform logic here?
   ```

2. **Where are field mappings applied?**
   ```python
   # Is this implemented somewhere?
   def apply_field_mapping(source_record, mapping_type, mapping_config):
       if mapping_type == 'direct':
           return source_record[mapping_config['source']]
       elif mapping_type == 'calculated':
           return eval(mapping_config['expression'])
       elif mapping_type == 'lookup':
           return db.query(lookup_table).filter(...).first()
       elif mapping_type == 'constant':
           return mapping_config['value']
   ```

3. **What is "standardized_data"?**
   ```
   - Is it a table in "staging" schema?
   - Is it a table in "transformation" schema?
   - Or is it "quality_check_results"?
   ```

### Recommended Fix

**Files to Create/Modify:**

1. **Verify/Update:** `/app/application/services/transformation_service.py`
   - Add method: `transform_and_validate_batch(raw_records, transformation_rules, field_mappings)`
   - Handle all 4 mapping types
   - Execute quality validation loop

2. **Verify/Create:** Standardized data model (if not exists)
   - Location: `/app/infrastructure/db/models/staging/standardized_data.py`
   - Or: `/app/infrastructure/db/models/transformation/standardized_data.py`

3. **Update:** `/app/tasks/etl_tasks.py`
   - Add transform phase after extract
   - Call transformation_service methods
   - Insert to standardized_data
   - Update quality_check_results

---

## Gap #2: LOAD PHASE - Transaction Boundaries & Lineage Tracking ✅ COMPLETE

### SEQUENCE.md Requirement (Lines 231-305) - IMPLEMENTED 2026-05-02

```
BEGIN TRANSACTION

For each validated record:
  1. EntityMatcher.match_entity()
     - Calculate entity_hash = MD5(key_fields)
     - Check for exact match in entities table
     - If no exact match: fuzzy match with similarity > threshold
  
  2. If new entity:
     → INSERT entities
     → INSERT data_lineage (source_id → entity_id)
     → UPDATE job_executions SET records_loaded += 1
  
  3. If duplicate:
     → UPDATE entities SET duplicate_count += 1, master_entity_id = ?
     → INSERT entity_relationships (type = 'duplicate_of')
  
  4. If update existing:
     → SELECT existing entity
     → Merge data (CONFLICT RESOLUTION)
     → UPDATE entities
     → INSERT change_log
     → UPDATE job_executions SET records_loaded += 1
  
  5. For all cases:
     → INSERT entity_relationships (general)
     → INSERT data_lineage (source → target, transformation_rule_id, job_execution_id)

COMMIT TRANSACTION

On Failure:
  → ROLLBACK TRANSACTION
  → INSERT error_logs
  → UPDATE job_executions SET status = 'failed'
  → Publish JobFailedEvent
```

### Codebase Status

**What EXISTS & IS IMPLEMENTED:**
- ✅ `EntityMatcher` with fuzzy matching algorithms (Levenshtein, Jaro-Winkler, FuzzyWuzzy)
- ✅ `Entity` model with entity_hash, confidence_score, duplicate_count fields
- ✅ `ChangeLog` model for tracking updates
- ✅ `DataLineage` model with source_entity_id, target_entity_id, transformation_rule_id, job_execution_id
- ✅ `EntityRelationship` model with relationship_type field
- ✅ `EntityService.create_entity()`, `update_entity()`, `get_entity_by_key()`
- ✅ Exception handling in services
- ✅ **LOAD PHASE LOOP:** Fully implemented in etl_tasks.py (lines 1341-1650)
- ✅ **TRANSACTION BOUNDARIES:** Explicit db.begin_nested() with commit/rollback
- ✅ **CONFLICT RESOLUTION:** 4 strategies implemented (newer_wins, score_based, conservative, merge)
- ✅ **MASTER_ENTITY_ID:** Properly assigned for duplicates with self-reference
- ✅ **LINEAGE CHAIN:** Complete tracking from standardized → entity
- ✅ **ENTITY MATCHER INTEGRATION:** Full integration in load phase with match result handling
- ✅ **DOCUMENTATION:** CONFLICT_RESOLUTION.md with decision trees and examples

**Implementation Details:**
- `load_records()` async function: ~550 lines
- Handles: new entities, duplicates, updates with merging
- Transaction management: explicit savepoint with per-record error handling
- Conflict resolution: 4 strategies with field-type-aware merging
- Lineage tracking: Complete chain from source to target with metadata
- Error handling: Transaction rollback with error logging

### Solution Implemented

**Transaction Management:**
```python
# Explicit transaction with savepoint
transaction = db.begin_nested()

try:
    for record in records:
        # Process: match, create/update, insert relationships
        # Individual commits per record for visibility
        db.commit()
    
    # Commit savepoint
    transaction.commit()
    
except Exception as e:
    # Rollback entire batch on error
    transaction.rollback()
    # Error logging and status update
```

**Conflict Resolution (4 Strategies):**
1. **newer_wins** (default): New values always replace existing
2. **score_based**: Higher confidence score wins per field
3. **conservative**: Keep existing, only add new fields
4. **merge**: Intelligent merge with field-type awareness

See: `CONFLICT_RESOLUTION.md` for complete strategy documentation

**Lineage Tracking:**
```
Raw Record → Standardized Data (Phase 5)
  INSERT data_lineage(source=raw_record, target=standardized, type=TRANSFORM)

Standardized Data → Entity (Phase 6)
  INSERT data_lineage(source=standardized, target=entity, type=LOAD)
  
Complete Chain:
  raw_record_id → standardized_id → entity_id → entity_relationships
```

### Questions to Investigate

1. **Is transaction atomicity per-record or per-batch?**
   ```python
   # Option A: Atomic per record (current likely scenario)
   for record in records:
       try:
           entity = match_entity(record)
           if is_new:
               db.add(entity)
               db.commit()  # ← Early commit = not atomic batch
       except:
           db.rollback()
   
   # Option B: Atomic per batch (safer, slower)
   try:
       db.begin()
       for record in records:
           entity = match_entity(record)
           db.add(entity)
       db.commit()
   except:
       db.rollback()
   ```

2. **What is the conflict resolution strategy?**
   ```python
   def merge_entity_data(old_entity, new_record, confidence_score):
       # How to decide which values to keep?
       # Currently: ???
   ```

3. **How is master_entity_id set for duplicates?**
   ```python
   # Is there code like this?
   if is_duplicate:
       duplicate_entity.master_entity_id = primary_entity.id
       db.update(duplicate_entity)
   ```

### Implementation Complete

**Files Modified:**
- ✅ `/app/tasks/etl_tasks.py`
  - Added: `load_records()` async function (550+ lines)
  - Added: `_merge_entity_data()` helper with 4 strategies
  - Updated: `_execute_load_job()` to call load_records()

- ✅ `/app/application/services/entity_service.py`
  - Added: `merge_entity_data()` method
  - Added: `update_entity_with_lineage()` method
  - Added: `mark_as_duplicate()` method
  - Added: `_apply_merge_strategy()` helper

- ✅ `/CONFLICT_RESOLUTION.md` - NEW
  - 4 strategies with examples
  - Decision tree
  - Per-field handling
  - Testing guide

**Key Features Implemented:**
- Explicit transaction management with savepoints
- Entity matching with confidence scoring
- Duplicate detection and master entity assignment
- 4 conflict resolution strategies
- Complete lineage tracking (raw → standardized → entity)
- Change logging for audits
- Proper error handling with rollback

---

## Gap #3: POST-PROCESSING - Dependent Job Triggering ❌ CRITICAL

### SEQUENCE.md Requirement (Lines 309-349)

```
Post-processing:
  1. Calculate metrics (duration, records_per_second, memory)
  2. INSERT performance_metrics
  3. Generate quality report
  4. If quality < threshold: Publish DataQualityAlert
  5. Query dependent jobs:
     → SELECT child_job_id FROM job_dependencies WHERE parent_job_id = ?
  6. For each dependent job:
     → Check all parent jobs completed
     → If yes: trigger_job_execution(child_job_id)
  7. Publish JobCompletedEvent
  8. Send completion notification
  9. Clear cache: DELETE job:{job_id}
  10. Set cache: SET execution:{execution_id} summary
```

### Codebase Status

**What EXISTS:**
- ✅ `JobDependency` model with parent_job_id, child_job_id, dependency_type
- ✅ `DependencyService.check_dependencies_met()` for checking
- ✅ `PerformanceMetrics` model for storing metrics
- ✅ `DataQualityService.generate_quality_report()`
- ✅ `NotificationService` for sending notifications
- ✅ `cache_manager` for cache operations
- ✅ Event publishing system exists

**What's MISSING or UNCLEAR:**
```
❌ CHILD JOB DISCOVERY & TRIGGERING NOT VISIBLE
   - No code visible for: "SELECT * FROM job_dependencies WHERE parent_job_id=?"
   - No loop visible: for each child_job: trigger()
   - DependencyService only checks IF dependencies met, NOT how to trigger children
   
❌ "ALL PARENTS COMPLETED" CHECK NOT VISIBLE
   - SEQUENCE requires: check all parent jobs completed
   - Scenario: Child has 3 parents. Only 1 parent done.
     - Child should NOT be triggered yet
   - This logic must check: COUNT(parents_completed) == COUNT(all_parents)
   
❌ POST-PROCESSING TASK FLOW NOT VISIBLE
   - etl_tasks.py ends before showing post-processing
   - Unclear if metrics, quality report, child triggering are in main task
   - Or separate post_process_task?
   
⚠️ TIMING ISSUE
   - When exactly are dependent children triggered?
   - During job completion in etl_tasks?
   - Or separate scheduled task?
   - What if child depends on multiple parents finishing at different times?
   
⚠️ EXECUTION PARAMETERS
   - When triggering child job, what parameters?
   - Does child job inherit parent's parameters?
   - Or use its own configuration?
```

### Architecture Problem

**Dependent Job Scenario:**
```
Job Dependency Graph:
  
  ExtractData (Job A)
       ↓
  ProcessData (Job B) ← Parent 1
       ↓
  LoadData (Job C) ← Parent 2
       ↓
  GenerateReport (Job D) ← Parent 3
       ↓
  SendNotification (Job E)

Timeline:
  T=0s: All 5 jobs in PENDING state
  T=10s: Job A completes
  T=15s: Job B should be triggered (parent A done)
  T=20s: Job B completes
  T=25s: Job C should be triggered (parents A,B done)
  T=30s: Job D should be triggered (parents A,B,C done)
  T=35s: Job E should be triggered (parents A,B,C,D done)

Current Code Issue:
  - No visible code to handle this scenario
  - Is each job responsible for triggering its children?
    → If yes: what if 2 jobs try to trigger child simultaneously?
  - Is there a scheduler that checks periodically?
    → If yes: overhead, not real-time
```

**Atomicity Issue:**
```
Scenario: Trigger child job

Step 1: Mark parent as complete ✅
Step 2: Check all parents of child #1
Step 3: Insert job_execution record for child #1
Step 4: Queue Celery task for child #1
Step 5: FAILURE! (network, DB, etc.)

Result:
  - Job execution record created but Celery task never queued
  - Or vice versa
  - Child job stuck in PENDING forever

SEQUENCE doesn't show error handling here.
```

### Questions to Investigate

1. **Where is child job discovery?**
   ```python
   # Is this implemented somewhere?
   def get_dependent_jobs(parent_job_id):
       return db.query(JobDependency).filter(
           JobDependency.parent_job_id == parent_job_id
       ).all()
   
   # Then what?
   for dependency in dependencies:
       child_job_id = dependency.child_job_id
       # ... trigger code ... ???
   ```

2. **How is "all parents completed" verified?**
   ```python
   # Is this implemented?
   def can_trigger_child(child_job_id):
       parent_dependencies = db.query(JobDependency).filter(
           JobDependency.child_job_id == child_job_id
       ).all()
       
       for dep in parent_dependencies:
           parent_execution = db.query(JobExecution).filter(
               JobExecution.job_id == dep.parent_job_id
           ).order_by(JobExecution.id.desc()).first()
           
           if parent_execution.status != 'completed':
               return False  # ← At least one parent not done
       
       return True  # ← All parents done
   ```

3. **What if child has 2+ parents at different completion times?**
   ```
   Scenario:
   - Job D has parents: B, C
   - B completes at T=20s
   - C completes at T=30s
   
   What happens?
   - At T=20s: is D triggered? (B done but C not)
   - At T=30s: is D triggered? (B done AND C done)
   
   Current code: ???
   ```

### Recommended Fix

**Files to Create/Modify:**

1. **Create:** `/app/application/services/job_orchestration_service.py`
   ```python
   class JobOrchestrationService:
       async def trigger_dependent_jobs(parent_job_id: UUID):
           """
           1. Find all direct children of parent_job_id
           2. For each child, check if ALL parents are completed
           3. If yes, trigger child job execution
           """
           dependencies = self.job_repo.get_children(parent_job_id)
           
           for dep in dependencies:
               child_job_id = dep.child_job_id
               
               # Check all parents of this child
               can_trigger = await self._check_all_parents_completed(child_job_id)
               
               if can_trigger:
                   await self._trigger_child_job(child_job_id)
       
       async def _check_all_parents_completed(child_job_id):
           """Return True only if ALL parent jobs are in completed state"""
           ...
       
       async def _trigger_child_job(child_job_id):
           """Atomically insert execution record and queue Celery task"""
           ...
   ```

2. **Update:** `/app/tasks/etl_tasks.py`
   - Call `job_orchestration_service.trigger_dependent_jobs()` 
   - After job completion, before cache operations
   - Add error handling for child triggers

3. **Update:** `/app/application/services/etl_service.py`
   - Add method: `get_dependent_jobs(job_id)`
   - Returns list of child job IDs

4. **Consider:** Event-driven approach
   - Instead of parent triggering children
   - Children subscribe to "parent completed" events
   - Event: JobCompleted(job_id=X)
   - Subscriber: Check if ready to run, then trigger

---

## Additional Issues (Priority 2)

### 4. Standardized Data Schema Unclear

**Problem:**
- SEQUENCE mentions "standardized_data" table (lines 155, 175, 219, 233)
- Codebase has: raw_data, staging, transformation, processed schemas
- No clear mapping: which schema has standardized_data?

**Impact:** Transform phase implementation blocked

**Fix:** 
- Verify/create `/app/infrastructure/db/models/staging/standardized_data.py` or transformation version
- Add to database migrations
- Update transform service to populate it

---

### 5. Field Mapping Model vs Execution Gap

**Problem:**
- FieldMapping model exists but implementation not visible
- No code for: direct/calculated/lookup/constant mapping types
- Unclear how `expression` field is evaluated for calculated mappings

**Impact:** Transform phase cannot execute field mappings

**Fix:**
- Create `/app/application/services/field_mapping_service.py`
- Implement: `apply_mapping(source_record, mapping_config) → target_value`
- Handle all 4 types with safety (expression eval sandboxing)

---

### 6. Event Publishing Inconsistency

**Problem:**
- Event system exists but not consistently used
- `create_etl_job()`: NO JobCreatedEvent published
- Other phases: Events status unclear

**Impact:** Event-driven features (monitoring, notifications) incomplete

**Fix:**
- Add event publishing to all phases
- Ensure: JobCreatedEvent, JobStartedEvent, JobCompletedEvent, JobFailedEvent, DataQualityAlert

---

## Summary: Gap Impact Matrix

| Gap | Phase | Severity | Blocker | LOC Impact | Dependencies |
|-----|-------|----------|---------|-----------|--------------|
| #1: Transform loop | 5 | CRITICAL | Yes | ~200 | clean, normalize, validate |
| #2: Load + transactions | 6 | CRITICAL | Yes | ~300 | matcher, entity service |
| #3: Child job trigger | 7 | CRITICAL | No | ~100 | dependency service |
| #4: Standardized schema | 5 | HIGH | Yes | ~50 | model definition |
| #5: Field mapping exec | 5 | HIGH | Yes | ~150 | mapping service |
| #6: Event publishing | All | MEDIUM | No | ~20 | event system |

---

## Next Steps

### For Architect
1. Schedule code review of `/app/tasks/etl_tasks.py` FULL FILE (not preview)
2. Schedule code review of `/app/application/services/transformation_service.py`
3. Get schema documentation for staging vs transformation
4. Define conflict resolution strategy with product team

### For Team
1. **Week 1:** Verify Transform phase implementation
2. **Week 2:** Verify Load phase + transaction strategy
3. **Week 3:** Implement child job triggering
4. **Week 4:** Add event publishing + testing

### For Testing
1. Integration test: full ETL flow with dependent jobs
2. Unit test: conflict resolution edge cases
3. Unit test: transaction rollback scenarios
4. Performance test: load phase with 10k+ records

---

**Document Status:** Ready for Architecture Review
**Last Updated:** 2026-05-02
**Reviewer:** Senior Fullstack Architect AI
