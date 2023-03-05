package com.lupo.akkapubsubconnector.listener;


import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.springframework.beans.factory.annotation.Value;

import com.lupo.akkapubsubconnector.model.Message;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.japi.function.Function;
import akka.stream.ActorAttributes;
import akka.stream.FlowShape;
import akka.stream.Supervision;
import akka.stream.Supervision.Directive;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.alpakka.google.GoogleAttributes;
import akka.stream.alpakka.google.GoogleSettings;
import akka.stream.alpakka.googlecloud.pubsub.AcknowledgeRequest;
import akka.stream.alpakka.googlecloud.pubsub.PubSubConfig;
import akka.stream.alpakka.googlecloud.pubsub.PublishRequest;
import akka.stream.alpakka.googlecloud.pubsub.ReceivedMessage;
import akka.stream.alpakka.googlecloud.pubsub.javadsl.GooglePubSub;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;


public abstract class Listener<MESSAGE> {

    private final PubSubSink pubSubSink;
    private final PubSubSource pubSubSource;
    private final PubSubManager pubSubPubSubManager;
    @Value("${PARALLELISM}")
    private int parallelism;
    @Value("${PUBSUB_PROJECT_ID}")
    private String projectId;

    @Value("${PUBSUB_SUBSCRIPTION_IN}")
    private String subscriptionIn;

    public Listener(final PubSubSink pubSubSink,
                    final PubSubSource pubSubSource,
                    final PubSubManager pubSubPubSubManager) {
        this.pubSubSink = pubSubSink;
        this.pubSubSource = pubSubSource;
        this.pubSubPubSubManager = pubSubPubSubManager;
    }

    public void start() {
        final ActorSystem actorSystem = ActorSystem.create();
        final PubSubConfig pubSubConfig = this.pubSubPubSubManager.createPubSubConfig();

        final Source<ReceivedMessage, NotUsed> pubSubSource = this.pubSubSource.createSource(pubSubConfig);
        final Flow<ReceivedMessage, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> parallelProcess = processMessages();

        final Flow<Pair<AcknowledgeRequest, PublishRequest>, List<String>, NotUsed> publish = this.pubSubSink.pubSubPublish(pubSubConfig);

        final Sink<AcknowledgeRequest, CompletionStage<Done>> sink = GooglePubSub.acknowledge(this.subscriptionIn,
                                                                                              pubSubConfig);

        final RunnableGraph<CompletionStage<Done>> runnableGraph =
                pubSubSource.via(parallelProcess)
                            .via(PassThroughFlow.create(publish))
                            .map(value -> value.second()
                                               .first())
                            .toMat(sink,
                                   Keep.right());

        runGraph(runnableGraph,
                 actorSystem);
    }

    private void runGraph(final RunnableGraph<CompletionStage<Done>> runnableGraph,
                          final ActorSystem actorSystem) {
        final GoogleSettings defaultSettings = GoogleSettings.create(actorSystem);
        final GoogleSettings customSettings = defaultSettings.withProjectId(this.projectId);


        final Function<Throwable, Supervision.Directive> supervisorStrategy = streamSupervisorStrategy();

        final RunnableGraph<CompletionStage<Done>> withCustomSupervision =
                runnableGraph.withAttributes(ActorAttributes.withSupervisionStrategy(supervisorStrategy))
                             .addAttributes(GoogleAttributes.settings(customSettings));

        withCustomSupervision.run(actorSystem);
    }

    private Function<Throwable, Supervision.Directive> streamSupervisorStrategy() {
        return exception -> (Directive) Supervision.resume();
    }

    private Flow<ReceivedMessage, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> processMessages() {
        final Flow<ReceivedMessage, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> ListenerFlow = messageProcessFlow();
        return makeFlowParallel(ListenerFlow);
    }

    private Flow<ReceivedMessage, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> messageProcessFlow() {
        return Flow.of(ReceivedMessage.class)
                   .via(processFlow())
                   .via(pubSubSink.convertToPublishMessage())
                   .groupedWithin(1,
                                  Duration.ofSeconds(1))
                   .via(pubSubSink.convertToPublishRequest());
    }

    abstract public Flow<ReceivedMessage, Message<MESSAGE>, NotUsed> processFlow();

    private Flow<ReceivedMessage, Pair<AcknowledgeRequest, PublishRequest>, NotUsed> makeFlowParallel(final Flow<ReceivedMessage,
            Pair<AcknowledgeRequest,
                    PublishRequest>, NotUsed> flow) {
        return Flow.fromGraph(
                GraphDSL.create((builder) -> {
                    final UniformFanInShape<Pair<AcknowledgeRequest, PublishRequest>, Pair<AcknowledgeRequest, PublishRequest>> mergeShape
                            = builder.add(Merge.create(parallelism));
                    final UniformFanOutShape<ReceivedMessage, ReceivedMessage> dispatchShape = builder.add(Balance.create(parallelism));

                    for (int i = 0; i < parallelism; i++) {
                        builder.from(dispatchShape.out(i))
                               .via(builder.add(flow.async()))
                               .toInlet(mergeShape.in(i));
                    }
                    return FlowShape.of(dispatchShape.in(),
                                        mergeShape.out());
                })
        );
    }
}
