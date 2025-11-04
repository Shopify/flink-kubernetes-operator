# Handling savepointRedeployNonce Changes During Transitions

## Overview

This document describes the behavior when a user changes `savepointRedeployNonce` while a FlinkBlueGreenDeployment is in the middle of a transition.

## Scenario

**Initial State:**
- BLUE deployment is running (ACTIVE_BLUE state)
- User triggers a transition to GREEN (e.g., by changing parallelism)
- State changes to TRANSITIONING_TO_GREEN
- GREEN deployment is starting up, restoring from a savepoint taken from BLUE

**User Action During Transition:**
- While in TRANSITIONING_TO_GREEN state
- User changes `savepointRedeployNonce` to a new value
- User sets `initialSavepointPath` to a different savepoint location

## Implemented Behavior (Option 1: Abort Transition)

### What Happens

1. **Detection Phase**
   - Operator detects spec change during `monitorTransition()`
   - Calls `handleSpecChangesDuringTransition()`
   - Identifies diff type as `SAVEPOINT_REDEPLOY`

2. **Abort Phase**
   - Operator deletes the in-progress GREEN deployment
   - Reverts BlueGreen state to `ACTIVE_BLUE`
   - Clears savepoint trigger ID
   - Marks spec as reconciled

3. **Restart Phase**
   - Next reconciliation cycle detects new spec
   - Triggers a fresh transition to GREEN
   - Uses the NEW `initialSavepointPath` specified by user
   - GREEN starts from the user-specified savepoint

### Code Location

**File:** `BlueGreenDeploymentService.java`

**Method:** `handleSavepointRedeployDuringTransition()`

```java
private UpdateControl<FlinkBlueGreenDeployment> handleSavepointRedeployDuringTransition(
        BlueGreenContext context,
        BlueGreenDeploymentType currentBlueGreenDeploymentType)
```

**Called from:** `handleSpecChangesDuringTransition()` when `diffType == SAVEPOINT_REDEPLOY`

## Behavior Details

### Logs

When this occurs, the operator logs:
```
WARN: savepointRedeployNonce changed during transition from BLUE to GREEN.
      Aborting current transition and will restart with new savepoint from: s3://bucket/new-savepoint
INFO: Deleted transitioning FlinkDeployment 'my-app-green' due to savepointRedeployNonce change
```

### State Transitions

```
TRANSITIONING_TO_GREEN
  → (nonce changed)
  → ACTIVE_BLUE
  → (next reconciliation)
  → TRANSITIONING_TO_GREEN (with new savepoint)
```

### Timing

- Deletion: Immediate (current reconciliation)
- Restart: Next reconciliation cycle (~5 seconds with default settings)
- Total delay: ~5-10 seconds before new transition begins

## Why This Approach?

### User Intent
When a user changes `savepointRedeployNonce` during a transition, their intent is clear:
- "Stop what you're doing"
- "Use THIS specific savepoint instead"
- "Start the transition over with the correct state"

### Alternative Approaches Considered

**Option 2: Reject the Change**
- Pros: Simple, preserves in-progress work
- Cons: User has to wait and retry manually, confusing with GitOps

**Option 3: Queue the Change**
- Pros: No transition interruption
- Cons: Complex implementation, delayed behavior (user expects immediate action)

**Why Option 1 (Abort) is Better:**
- ✅ Clear semantics: nonce = "redeploy NOW"
- ✅ Matches user expectation
- ✅ Consistent with FlinkDeployment behavior
- ✅ No silent failures (previous behavior)

## Testing

### Unit Test Scenario

**Test Name:** `testSavepointRedeployDuringTransition`

**Steps:**
1. Create BlueGreen with BLUE running
2. Trigger transition to GREEN (change parallelism)
3. While TRANSITIONING_TO_GREEN, change savepointRedeployNonce
4. Verify GREEN is deleted
5. Verify state reverts to ACTIVE_BLUE
6. Verify next reconciliation starts new transition
7. Verify new GREEN uses new savepoint path

### E2E Test

**File:** `e2e-tests/test_bluegreen_savepoint_redeploy.sh`

**Test Section:** (To be added)
```bash
# Test 4: Nonce change during transition
# 1. Start transition BLUE → GREEN
# 2. Before GREEN is stable, change nonce
# 3. Verify GREEN is deleted
# 4. Verify new transition uses new savepoint
```

## Edge Cases

### Case 1: Deletion Fails
**Scenario:** GREEN deployment cannot be deleted (finalizer stuck, etc.)

**Behavior:**
- Operator logs warning
- Retries deletion on next reconciliation
- Remains in ACTIVE_BLUE state until deletion succeeds

### Case 2: Multiple Nonce Changes
**Scenario:** User changes nonce multiple times rapidly

**Behavior:**
- Each change triggers abort and restart
- Last nonce change wins
- May cause multiple deletion/creation cycles

**Recommendation:** Wait for state to stabilize before changing nonce again

### Case 3: Nonce Change Right Before Completion
**Scenario:** GREEN is about to become STABLE when nonce changes

**Behavior:**
- GREEN is deleted (wasted work)
- Transition restarts from beginning
- New GREEN created with new savepoint

**Note:** This is intentional - user's explicit nonce change takes precedence

## Migration from Previous Behavior

### Old Behavior (Before Fix)
- Nonce change during transition was **silently ignored**
- Green deployment continued with old savepoint
- User's new savepoint path was never used
- Spec marked as reconciled (appearing to work)

### New Behavior (After Fix)
- Nonce change during transition **aborts transition**
- Green deployment is deleted
- Transition restarts with new savepoint
- User intent is honored

### Impact
- **Breaking Change:** No (old behavior was buggy)
- **User Visible:** Yes (different logs, temporary deletion)
- **Requires Documentation Update:** Yes

## Related Features

- **savepointRedeployNonce**: Triggers redeploy from specific savepoint
- **initialSavepointPath**: Specifies which savepoint to restore from
- **SAVEPOINT_REDEPLOY DiffType**: Detected when nonce changes
- **Blue/Green Transitions**: Normal deployment switching mechanism

## References

- Implementation: `BlueGreenDeploymentService.java:440-540`
- Diff Detection: `FlinkBlueGreenDeploymentSpecDiff.java:58-89`
- Nonce Annotation: `JobSpec.java:98`
- E2E Tests: `e2e-tests/test_bluegreen_savepoint_redeploy.sh`
