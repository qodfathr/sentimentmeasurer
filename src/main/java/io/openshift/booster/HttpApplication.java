package io.openshift.booster;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import io.vertx.ext.web.client.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import twitter4j.*;
import twitter4j.conf.*;
import io.vertx.servicediscovery.*;
import io.vertx.servicediscovery.types.*;
import io.vertx.core.Handler;
import io.vertx.core.AsyncResult;
import java.util.Set;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.eventbus.MessageConsumer;

import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.bridge.PermittedOptions;
/**
 *
 */
public class HttpApplication extends AbstractVerticle {

    private ConfigRetriever conf;
    private String message;

    private static final Logger LOGGER = LogManager.getLogger(HttpApplication.class);
    private JsonObject config;
    
    private boolean online = false;
    
    private static final String TWITTER_EVENTBUS_NAME = "twitter-data-name";
    private static final String TWITTER_EVENTBUS_ADDRESS = "twitter-data-address";

    @Override
    public void start(Future<Void> future) {
        setUpConfiguration();

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx)
            .register("server-online", fut -> fut.complete(online ? Status.OK() : Status.KO()));
        
        Router router = Router.router(vertx);
        router.get("/api/greeting").handler(this::greeting);
        router.get("/api/topic").handler(this::topic);
        router.get("/api/sentiment").handler(this::sentiment);
        router.get("/health").handler(rc -> rc.response().end("OK"));
        router.get("/*").handler(StaticHandler.create());
        
        BridgeOptions options = new BridgeOptions()
            .addInboundPermitted(new PermittedOptions().setAddress(TWITTER_EVENTBUS_ADDRESS))
            .addOutboundPermitted(new PermittedOptions().setAddress(TWITTER_EVENTBUS_ADDRESS));
        System.out.println("BRIDGING");
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx).bridge(options, event -> {
            event.complete(true);
        });
        System.out.println("BRIDGED");
        
        router.route("/eventbus/*").handler(sockJSHandler);

        retrieveMessageTemplateFromConfiguration()
            .setHandler(ar -> {
                // Once retrieved, store it and start the HTTP server.
                message = ar.result();
                vertx
                    .createHttpServer()
                    .requestHandler(router::accept)
                    .listen(
                        // Retrieve the port from the configuration,
                        // default to 8080.
                        config().getInteger("http.port", 8080), ar2 -> {
                            online = ar2.succeeded();
                            future.handle(ar2.mapEmpty()); 
                        });
        });


        // It should use the retrieve.listen method, however it does not catch the deletion of the config map.
        // https://github.com/vert-x3/vertx-config/issues/7
        vertx.setPeriodic(2000, l -> {
            conf.getConfig(ar -> {
                if (ar.succeeded()) {
                    if (config == null || !config.encode().equals(ar.result().encode())) {
                        config = ar.result();
                        LOGGER.info("New configuration retrieved: {}",
                            ar.result().getString("message"));
                        message = ar.result().getString("message");
                        String level = ar.result().getString("level", "INFO");
                        LOGGER.info("New log level: {}", level);
                        setLogLevel(level);
                    }
                } else {
                    message = null;
                }
            });
        });
        
        discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions().setBackendConfiguration(config()));
        tweetStorm();
    }
    
    @Override
    public void stop(Future<Void> stopFuture) {
        if (discovery != null) discovery.close();
        if (twitterStream != null) twitterStream.shutdown();
        stopFuture.complete();
    }
    
    protected ServiceDiscovery discovery;
    protected Set<Record> registeredRecords = new ConcurrentHashSet<>();

    public void publishMessageSource(String name, String address, Handler<AsyncResult<Void>> completionHandler) {
        Record record = MessageSource.createRecord(name, address);
        publish(record, completionHandler);
        
        // We can consume the tweets from the event bus here. Nothing for us to do in this app, but
        // useful for debugging to ensure that the event bus is receiving and publishing events
        MessageSource.<JsonObject>getConsumer(discovery, new JsonObject().put("name",name), ar -> {
            if (ar.succeeded()) {
                MessageConsumer<JsonObject> consumer = ar.result();
                
                consumer.handler(message -> {
                    //System.out.println(message.body().toString());
                });
            } else {
                    System.out.println("fail");
            }
        });
    }

    protected void publish(Record record, Handler<AsyncResult<Void>> completionHandler) {
        /*
        if (discovery == null) {
            try {
                start();
            } catch (Exception e) {
                throw new RuntimeException("Cannot create discovery service");
            }
        }
        */
       
        discovery.publish(record, ar -> {
            if (ar.succeeded()) {
                registeredRecords.add(record);
            }
            completionHandler.handle(ar.map((Void)null));
        });
    }
  
    protected TwitterStream twitterStream;
    
    private void tweetStorm() {
        
        // Publish the services in the discovery infrastructure.
        publishMessageSource(TWITTER_EVENTBUS_NAME, TWITTER_EVENTBUS_ADDRESS, rec -> {
            if (!rec.succeeded()) {
                rec.cause().printStackTrace();
            }
            System.out.println("Twitter-Data service published : " + rec.succeeded());
        });
    
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(consumerKey)
            .setOAuthConsumerSecret(consumerSecret)
            .setOAuthAccessToken(accessToken)
            .setOAuthAccessTokenSecret(accessTokenSecret);
        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(twitter4j.Status status) {
                if (!status.isPossiblySensitive()) {
                    //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                    String tweetText = status.getText();
                    calculateSentimentForTweet(tweetText, res -> {
                        if (res.succeeded())
                        {
                            JsonObject tweetInfo = new JsonObject()
                                .put("user", status.getUser().getScreenName())
                                .put("tweet", status.getText())
                                .put("sentiment", res.result());
                            vertx.eventBus().publish(TWITTER_EVENTBUS_ADDRESS, tweetInfo);
                        }
                    });
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                //System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        //twitterStream.sample();   // this takes in a sampling of the entire twitter real-time stream, unfiltered.
        startStreamingTopic("openshiftio");
    }
    
    private void startStreamingTopic(String topic)
    {
        FilterQuery filter = new FilterQuery();
        String[] keywordsArray = { topic };
        filter.track(keywordsArray);
        twitterStream.filter(filter);
    }

    private void setLogLevel(String level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.getLevel(level));
        ctx.updateLoggers();
    }

    private void topic(RoutingContext rc) {
        if (message == null) {
            rc.response().setStatusCode(500)
                .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
                .end(new JsonObject().put("content", "no config map").encode());
            return;
        }
        
        String topic = rc.request().getParam("topic");
        if (topic == null) {
            topic = "openshiftio";
        }
        startStreamingTopic(topic);

        LOGGER.debug("Replying to request, parameter={}", topic);
        JsonObject response = new JsonObject()
            .put("content", String.format(message, topic));

        rc.response()
            .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
            .end(response.encodePrettily());
    }
    
    private void greeting(RoutingContext rc) {
        if (message == null) {
            rc.response().setStatusCode(500)
                .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
                .end(new JsonObject().put("content", "no config map").encode());
            return;
        }
        String name = rc.request().getParam("name");
        if (name == null) {
            name = "World";
        }

        LOGGER.debug("Replying to request, parameter={}", name);
        JsonObject response = new JsonObject()
            .put("content", String.format(message, name));

        rc.response()
            .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
            .end(response.encodePrettily());
    }
    
    private void calculateSentimentForTweet(String tweet, Handler<AsyncResult<Double>> handler) {
      WebClient client = WebClient.create(vertx);
      
     client
        .post(443,"ussouthcentral.services.azureml.net", "/workspaces/9dbf016f411f4388b7a574524b137656/services/954b60a6ae1c4903a9751a2a17ff988f/execute")
        .putHeader("Content-Type", "application/json")
        .putHeader("Authorization", "Bearer "+System.getenv("SENTIMENT_APIKEY"))
        .addQueryParam("api-version", "2.0")
        .addQueryParam("format", "swagger")
        .ssl(true)
        .sendJsonObject(new JsonObject("{\"Inputs\": {\"input1\": [{\"sentiment_label\":\"2\",\"tweet_text\":\"" + tweet.replace("\"", "\\\"").replace("\r","").replace("\n","") + "\"}]},\"GlobalParameters\": {}}")
            , ar -> {
                if (ar.succeeded()) {
                    io.vertx.ext.web.client.HttpResponse<Buffer> response = ar.result();
                    JsonObject sentimentResult = new JsonObject(response.bodyAsString());
                    JsonObject xyz = sentimentResult.getJsonObject("Results");
                    if (xyz != null)
                    {
                        try {
                            JsonArray xyz2 = xyz.getJsonArray("output1");
                            JsonObject xyz3 = xyz2.getJsonObject(0);
                            double score = Double.parseDouble(xyz3.getString("Score"));
                            handler.handle(Future.succeededFuture(score));
                        } catch(Exception e) {
                            handler.handle(Future.failedFuture(e.getMessage()));
                        }
                    } else {
                        handler.handle(Future.failedFuture("failed"));
                    }
                } else {
                    handler.handle(Future.failedFuture("failed ar"));
            }
        });
        
    }
    
    private void sentiment(RoutingContext rc) {
        calculateSentimentForTweet("have a nice day", res ->{
            if (res.succeeded())
                rc.response().end("Score: " + res.result().toString());
        });
    }
    
    private Future<String> retrieveMessageTemplateFromConfiguration() {
        Future<String> future = Future.future();
        conf.getConfig(ar ->
            future.handle(ar
                .map(json -> json.getString("message"))
                .otherwise(t -> null)));
        return future;
    }

    private void setUpConfiguration() {
        String path = System.getenv("VERTX_CONFIG_PATH");
        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        if (path != null) {
            ConfigStoreOptions appStore = new ConfigStoreOptions();
            appStore.setType("file")
                .setFormat("yaml")
                .setConfig(new JsonObject()
                    .put("path", path));
            options.addStore(appStore);
        }
        conf = ConfigRetriever.create(vertx, options);
    }
}