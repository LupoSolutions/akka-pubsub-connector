# akka-pubsub-connector #


akka-pubsub-connector is a java library for subscribing and publishing to pubsub. It utilizes multi-threading, for high throughput, and an efficient 
design to minimize 
the amount of threads for producing, allowing for those saved resources to be directed at parallelization.  

### Usage: ###
I don't have my artifacts published as of yet, for now one will have to publish to their maven local and reference the snapshot in your project's 
build.gradle's dependencies. 

Gradle:
```
implementation group: 'com.lupo', name: 'akka-pubsub-connector', version: '0.0.1-SNAPSHOT'
```

### Properties: ###
Add the following to your property files

PUBSUB_EMULATOR_HOST=localhost
PUBSUB_EMULATOR_PORT=8432
PUBSUB_TOPIC=suggest-search-metric-data
PUBSUB_TOPIC_OUT=outTopic

PUBSUB_SUBSCRIPTION=bby-s-osh-suggest-curator
PUBSUB_SUBSCRIPTION_OUT=curatorOut
PUBSUB_PROJECT_ID=localproject
PUBSUB_PULL_IMMEDIATELY=TRUE
PUBSUB_BATCH_SIZE=500
PARALLELISM=2

### How to use: ###
In your project that has the akka-pubsub-connector as a dependency, create a class that extends the Listener class. Override the processFlow() 
like the below. Also make sure you have the Properties in the property section above added to your property files. 
```
@Component
public class ListenerExample extends Listener {
    private static final Logger LOG = LoggerFactory.getLogger(ListenerExample.class);
    
    @Autowired
    private ReplaceWithClassThatContainsMethodsYouWantToCall replaceClass;

    public ListenerExample(final PubSubSink pubSubSink,
                           final PubSubSource pubSubSource,
                           final PubSubManager pubSubPubSubManager) {
        super(pubSubSink,
              pubSubSource,
              pubSubPubSubManager);
    }

    @Override
    public Flow<ReceivedMessage, Message<Suggestion>, NotUsed> processFlow() {
        return Flow.<ReceivedMessage>create()
        .via(replaceClass.performLogicLikeFilteringEventLoggingEnrichment())
                   .map(receivedMessage -> {

                    
                       return message;
                   });
    }

    @PostConstruct
    public void startThis() {
        start();
    }
}
```

### Running Tests: ###
This is a personal project, and I am the sole contributor. Since it's mine, and I'm not worried about breaking it there's no need for tests. 
Though if this were not a personal project it would have unit and integration tests.
