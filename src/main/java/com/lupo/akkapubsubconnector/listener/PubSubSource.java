package com.lupo.akkapubsubconnector.listener;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import akka.NotUsed;
import akka.stream.RestartSettings;
import akka.stream.alpakka.googlecloud.pubsub.PubSubConfig;
import akka.stream.alpakka.googlecloud.pubsub.ReceivedMessage;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSub;
import akka.stream.javadsl.RestartSource;

@Component
public class PubSubSource {

    @Value("${PUBSUB_SUBSCRIPTION_IN}")
    private String subscriptionIn;

    public akka.stream.javadsl.Source<ReceivedMessage, NotUsed> createSource(final PubSubConfig pubSubConfig) {
        final RestartSettings restartSettings = createRestartSettings();
        return RestartSource.onFailuresWithBackoff(restartSettings,
                                                   () ->  GooglePubSub.subscribe(this.subscriptionIn,
                                                                                 pubSubConfig));
    }

    private RestartSettings createRestartSettings() {
        final Duration minBackOff = Duration.ofSeconds(3);
        final Duration maxBackOff = Duration.ofSeconds(30);

        final int amountOfRestartsAllowed = 20;
        final Duration restartsAllowedWithin = Duration.ofMinutes(5);

        return RestartSettings.create(minBackOff,
                                      maxBackOff,
                                      0.2)
                              .withMaxRestarts(amountOfRestartsAllowed,
                                               restartsAllowedWithin);
    }
}
