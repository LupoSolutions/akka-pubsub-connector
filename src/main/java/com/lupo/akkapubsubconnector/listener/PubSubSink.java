package com.lupo.akkapubsubconnector.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.lupo.akkapubsubconnector.model.Message;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.RestartSettings;
import akka.stream.alpakka.googlecloud.pubsub.AcknowledgeRequest;
import akka.stream.alpakka.googlecloud.pubsub.PubSubConfig;
import akka.stream.alpakka.googlecloud.pubsub.PublishMessage;
import akka.stream.alpakka.googlecloud.pubsub.PublishRequest;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSub;
import akka.stream.javadsl.Flow;

@Component
public class PubSubSink<MESSAGE> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

    @Autowired
    private Gson gson;

    @Value("${PUBSUB_TOPIC_OUT}")
    private String topicOut;

    public Flow<Message<MESSAGE>, Pair<String, PublishMessage>, NotUsed> convertToPublishMessage() {
        return Flow.<Message<MESSAGE>>create()
                   .map(message -> {
                       final String jsonMessage = gson.toJson(message);
                       final String base64OfMessage = new String(Base64.getEncoder()
                                                                       .encode(jsonMessage.getBytes()));
                       final PublishMessage publishMessage = PublishMessage.create(base64OfMessage);
                       return Pair.create(message.getAcknowledgmentId(),
                                          publishMessage);
                   });
    }

    public Flow<List<Pair<String, PublishMessage>>, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> convertToPublishRequest() {
        return Flow.<List<Pair<String, PublishMessage>>>create()
                   .map(idsAndMessages -> {
                       final List<PublishMessage> messages = idsAndMessages.stream()
                                                                           .map(Pair::second)
                                                                           .collect(Collectors.toList());
                       final List<String> acknowledgmentIds = idsAndMessages.stream()
                                                                            .map(Pair::first)
                                                                            .collect(Collectors.toList());
                       return Pair.create(AcknowledgeRequest.create(acknowledgmentIds),
                                          PublishRequest.create(messages));
                   });
    }

    public Flow<Pair<AcknowledgeRequest, PublishRequest>, List<String>, NotUsed> pubSubPublish(final PubSubConfig pubSubConfig) {
        return Flow.<Pair<AcknowledgeRequest, PublishRequest>>create()
                   .map(Pair::second)
                   .via(GooglePubSub.publish(topicOut,
                                             pubSubConfig,
                                             1));
    }
}


