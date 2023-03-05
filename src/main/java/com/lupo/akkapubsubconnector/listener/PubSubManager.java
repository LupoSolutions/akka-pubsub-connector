package com.lupo.akkapubsubconnector.listener;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import akka.stream.alpakka.googlecloud.pubsub.PubSubConfig;

@Component
public class PubSubManager {

    public static final String PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST";
    public static final String PUBSUB_EMULATOR_PORT = "PUBSUB_EMULATOR_PORT";
    @Value("${PUBSUB_EMULATOR_HOST}")
    private String pubsubEmulatorHost;
    @Value("${PUBSUB_EMULATOR_PORT}")
    private String pubsubEmulatorPort;
    @Value("${PUBSUB_PULL_IMMEDIATELY:true}")
    private boolean pullImmediately;
    @Value("${PUBSUB_BATCH_SIZE:500}")
    private Integer gcpBatchSize;

    public PubSubConfig createPubSubConfig() {
        System.setProperty(PUBSUB_EMULATOR_HOST,
                           pubsubEmulatorHost);
        System.setProperty(PUBSUB_EMULATOR_PORT,
                           pubsubEmulatorPort);
        return PubSubConfig.create(this.pullImmediately,
                                   this.gcpBatchSize);
    }
}
