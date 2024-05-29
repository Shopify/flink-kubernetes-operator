package org.apache.flink.kubernetes.operator.mutator;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** Flink Initial Savepoint Mutator. */
public class FlinkSavepointMutator implements FlinkResourceMutator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSavepointMutator.class);

    @Override
    public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {
        String ns = deployment.getMetadata().getNamespace();
        String testPipeline = "staging-products-unrestricted-iyu9";

        LOG.info("STARTING MUTATION FOR NAMESPACE: {}", ns);
        if (ns.contains(testPipeline)) {
            String snapshotPath =
                    "gs://staging-products-unrestricted-iyu9/flink-checkpoints/6ee4d2f82e861a681eec4ad74254e774/chk-780";
            deployment.getSpec().getJob().setInitialSavepointPath(snapshotPath);
            LOG.info("Job: {} | Set snapshot path as: {}", testPipeline, snapshotPath);
        }
        return deployment;
    }

    @Override
    public FlinkSessionJob mutateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {

        return sessionJob;
    }
}
