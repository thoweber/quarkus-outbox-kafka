package io.github.thoweber.quarkus.outbox.kafka.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class QuarkusOutboxKafkaProcessor {

    private static final String FEATURE = "quarkus-outbox-kafka";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }
}
