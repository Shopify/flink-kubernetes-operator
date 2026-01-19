# Feasibility Assessment: `statelessRedeployNonce`

## Summary

Adding a `statelessRedeployNonce` to the Flink Kubernetes Operator is **highly feasible** with **low-to-moderate complexity**. The existing codebase already has a well-established pattern for similar nonces that makes this a straightforward addition.

## Existing Nonce Patterns in the Operator

The operator already implements several nonces in `JobSpec` that serve as templates:

1. **`restartNonce`** (in `AbstractFlinkSpec`) - Triggers restart following the current `upgradeMode` setting
2. **`savepointRedeployNonce`** - Triggers full redeploy from `initialSavepointPath` 
3. **`savepointTriggerNonce`** / **`checkpointTriggerNonce`** - Trigger snapshot operations (deprecated)
4. **`autoscalerResetNonce`** - Resets autoscaler state

## How the Existing Pattern Works

### `savepointRedeployNonce` Implementation

1. **Spec Definition** (`JobSpec.java:98-99`):
   ```java
   @SpecDiff(value = DiffType.SAVEPOINT_REDEPLOY, onNullIgnore = true)
   private Long savepointRedeployNonce;
   ```

2. **Diff Detection**: When the nonce changes from the last reconciled spec, `ReflectiveDiffBuilder` detects this as a `DiffType.SAVEPOINT_REDEPLOY` change.

3. **Reconciler Handling** (`AbstractJobReconciler.java:127-131`):
   ```java
   if (diffType == DiffType.SAVEPOINT_REDEPLOY) {
       redeployWithSavepoint(ctx, deployConfig, resource, status, currentDeploySpec, desiredJobState);
       return true;
   }
   ```

4. **Redeployment Logic** (`AbstractJobReconciler.java:590-616`):
   - Cancels the job with `SuspendMode.STATELESS`
   - Sets upgrade mode to `UpgradeMode.SAVEPOINT`
   - Deploys with the `initialSavepointPath`
   - Marks spec as stable (disabling rollbacks)

5. **Validation** (`DefaultValidator.java:448-457`):
   - Validates that `initialSavepointPath` is not empty when nonce changes

## Implementation Approach for `statelessRedeployNonce`

### Option 1: New DiffType (Recommended)

Add a new `DiffType.STATELESS_REDEPLOY` similar to `SAVEPOINT_REDEPLOY`:

```java
// DiffType.java
public enum DiffType {
    IGNORE,
    SCALE,
    UPGRADE,
    SAVEPOINT_REDEPLOY,
    STATELESS_REDEPLOY;  // NEW
    // ...
}
```

```java
// JobSpec.java
@SpecDiff(value = DiffType.STATELESS_REDEPLOY, onNullIgnore = true)
private Long statelessRedeployNonce;
```

```java
// AbstractJobReconciler.java
if (diffType == DiffType.STATELESS_REDEPLOY) {
    redeployStateless(ctx, deployConfig, resource, status, currentDeploySpec, desiredJobState);
    return true;
}

private void redeployStateless(
        FlinkResourceContext<CR> ctx,
        Configuration deployConfig,
        CR resource,
        STATUS status,
        SPEC currentDeploySpec,
        JobState desiredJobState) throws Exception {
    LOG.info("Redeploying statelessly");
    cancelJob(ctx, SuspendMode.STATELESS);
    currentDeploySpec.getJob().setUpgradeMode(UpgradeMode.STATELESS);
    status.getJobStatus().setUpgradeSavepointPath(null);
    
    if (desiredJobState == JobState.RUNNING) {
        deploy(ctx, currentDeploySpec, ctx.getDeployConfig(currentDeploySpec), Optional.empty(), false);
    }
    ReconciliationUtils.updateStatusForDeployedSpec(resource, deployConfig, clock);
    status.getReconciliationStatus().markReconciledSpecAsStable();
}
```

### Option 2: Reuse Existing Infrastructure

Alternatively, leverage the existing `SAVEPOINT_REDEPLOY` flow with a flag:
- Check if `statelessRedeployNonce` changed
- If so, force `UpgradeMode.STATELESS` and null out `initialSavepointPath`
- Reuse `redeployWithSavepoint` with empty path handling

This is less clean but reduces code duplication.

## Files to Modify

| File | Change |
|------|--------|
| `flink-kubernetes-operator-api/.../api/spec/JobSpec.java` | Add `statelessRedeployNonce` field with annotation |
| `flink-kubernetes-operator-api/.../api/diff/DiffType.java` | Add `STATELESS_REDEPLOY` enum value |
| `flink-kubernetes-operator/.../reconciler/deployment/AbstractJobReconciler.java` | Handle new diff type, add `redeployStateless()` method |
| `flink-kubernetes-operator/.../reconciler/deployment/AbstractFlinkResourceReconciler.java` | Add `STATELESS_REDEPLOY` to scaling check condition |
| `helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml` | Add `statelessRedeployNonce` field |
| `helm/flink-kubernetes-operator/crds/flinksessionjobs.flink.apache.org-v1.yml` | Add `statelessRedeployNonce` field |
| `helm/flink-kubernetes-operator/crds/flinkbluegreendeployments.flink.apache.org-v1.yml` | Add `statelessRedeployNonce` field |
| Tests | Add unit tests for new nonce handling |
| Documentation | Update `job-management.md` and `reference.md` |

## Complexity Assessment

**Low-to-Moderate** (estimated 2-4 days for implementation + testing):

- **Clear pattern to follow**: The `savepointRedeployNonce` provides an exact template
- **Well-defined boundaries**: The reconciliation logic is modular
- **No architectural changes**: Uses existing diff/reconcile infrastructure
- **Main complexity**: Ensuring proper edge case handling (job in various states)

## Key Behavioral Differences from `savepointRedeployNonce`

| Aspect | `savepointRedeployNonce` | `statelessRedeployNonce` |
|--------|--------------------------|--------------------------|
| Requires `initialSavepointPath` | Yes (validated) | No |
| Restores state | Yes, from specified savepoint | No, starts fresh |
| Use case | Manual recovery | Fresh start, schema changes |

## Potential Discussion Points for Open-Source Community

1. **Naming Convention**: 
   - `statelessRedeployNonce` - matches existing pattern
   - `resetStateNonce` - more descriptive of what it does
   - `freshStartNonce` - user-friendly

2. **Warning/Confirmation Mechanism**: Should the operator emit a warning event when state is being dropped?

3. **Rollback Behavior**: Like `savepointRedeployNonce`, rollbacks should be disabled after stateless redeploy (spec marked stable)

4. **Interaction with `upgradeMode`**: Should this temporarily override the `upgradeMode` or permanently set it to stateless?

5. **Concurrent Nonce Changes**: What happens if both `statelessRedeployNonce` and `savepointRedeployNonce` change? (Recommend: `STATELESS_REDEPLOY` takes precedence or validation error)

## Recommendation

This is a **clean, well-scoped feature** that aligns perfectly with existing operator patterns. The implementation is straightforward with clear precedent in the codebase. I recommend proceeding with a discussion with the open-source community, as this would be a valuable addition for use cases where a fresh start is needed without deleting and recreating the entire resource.

The existing `savepointRedeployNonce` implementation can serve as almost a copy-paste template, with the main difference being:
- No savepoint path validation
- Use `SuspendMode.STATELESS` 
- Deploy with `Optional.empty()` for savepoint
- Set `UpgradeMode.STATELESS`
