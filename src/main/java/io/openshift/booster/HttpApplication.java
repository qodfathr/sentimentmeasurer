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

import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.auth.oauth2.providers.TwitterAuth;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.rxjava.ext.auth.oauth2.AccessToken;

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
/**
 *
 */
public class HttpApplication extends AbstractVerticle {

    private ConfigRetriever conf;
    private String message;

    private static final Logger LOGGER = LogManager.getLogger(HttpApplication.class);
    private JsonObject config;
    
    private boolean online = false;

    @Override
    public void start(Future<Void> future) {
        setUpConfiguration();

        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx)
            .register("server-online", fut -> fut.complete(online ? Status.OK() : Status.KO()));
        
        Router router = Router.router(vertx);
        router.get("/api/greeting").handler(this::greeting);
        router.get("/api/sentiment").handler(this::sentiment);
        router.get("/api/twit").handler(this::twit);
        router.get("/health").handler(rc -> rc.response().end("OK"));
        router.get("/").handler(StaticHandler.create());

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
    
    protected ServiceDiscovery discovery;
    protected Set<Record> registeredRecords = new ConcurrentHashSet<>();


    public void publishMessageSource(String name, String address, Handler<AsyncResult<Void>> completionHandler) {
        Record record = MessageSource.createRecord(name, address);
        publish(record, completionHandler);
        
        MessageSource.<JsonObject>getConsumer(discovery, new JsonObject().put("name",name), ar -> {
            if (ar.succeeded()) {
                MessageConsumer<JsonObject> consumer = ar.result();
                
                consumer.handler(message -> {
                    System.out.println(message.body().toString());
                });
            } else {
                    System.out.println("fail");
            }
        });
    }

    protected void publish(Record record, Handler<AsyncResult<Void>> completionHandler) {
        if (discovery == null) {
            try {
                start();
            } catch (Exception e) {
                throw new RuntimeException("Cannot create discovery service");
            }
        }

        discovery.publish(record, ar -> {
            if (ar.succeeded()) {
                registeredRecords.add(record);
            }
            completionHandler.handle(ar.map((Void)null));
        });
        
  }
  
    private void tweetStorm() {
        
        // Publish the services in the discovery infrastructure.
        publishMessageSource("twitter-data", "tweets", rec -> {
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
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(twitter4j.Status status) {
                //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                vertx.eventBus().publish("tweets", new JsonObject().put("tweet", status.getText()));
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

    private void setLogLevel(String level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.getLevel(level));
        ctx.updateLoggers();
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
    
    private void sentiment(RoutingContext rc) {
      WebClient client = WebClient.create(vertx);
      
      client
        .post(443,"ussouthcentral.services.azureml.net", "/workspaces/9dbf016f411f4388b7a574524b137656/services/954b60a6ae1c4903a9751a2a17ff988f/execute")
        .putHeader("Content-Type", "application/json")
        .putHeader("Authorization", "Bearer "+System.getenv("SENTIMENT_APIKEY"))
        .addQueryParam("api-version", "2.0")
        .addQueryParam("format", "swagger")
        .ssl(true)
        .sendJsonObject(new JsonObject("{\"Inputs\": {\"input1\": [{\"sentiment_label\":\"2\",\"tweet_text\":\"have a nice day\"}]},\"GlobalParameters\": {}}")
            , ar -> {
                if (ar.succeeded()) {
                    io.vertx.ext.web.client.HttpResponse<Buffer> response = ar.result();
                    JsonObject sentimentResult = new JsonObject(response.bodyAsString());
                    JsonObject xyz = sentimentResult.getJsonObject("Results");
                    JsonArray xyz2 = xyz.getJsonArray("output1");
                    JsonObject xyz3 = xyz2.getJsonObject(0);
                    rc.response().end(xyz3.getString("Sentiment") + " : " + xyz3.getString("Score"));
                } else {
                    rc.response().end("FAIL: " + ar.cause().getMessage());
            }
        });
    }
    
    private static final String AUTH_URL = "https://api.twitter.com/oauth2/token";
    private static final String TWEET_SEARCH_URL = "https://api.twitter.com/1.1/search/tweets.json";

    private void twit(RoutingContext rc) {
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String B64_ENCODED_AUTH = java.util.Base64.getEncoder().encodeToString((consumerKey+":"+consumerSecret).getBytes());
        WebClient client = WebClient.create(vertx);

        String queryToSearch = "vertx";

        // First we need to authenticate our call.
        String authHeader = "Basic " + B64_ENCODED_AUTH;
        client.postAbs(AUTH_URL)
            .as(BodyCodec.jsonObject())
            .addQueryParam("grant_type", "client_credentials")
            .putHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
            .putHeader("Authorization", authHeader)
            .send(authHandler -> {
                // Authentication successful.
                if (authHandler.succeeded() && 200 == authHandler.result().statusCode()) {
                  JsonObject authJson = authHandler.result().body();
                    String accessToken = authJson.getString("access_token");
                    String header = "Bearer " + accessToken;
                    // Making call to search tweets.
                    client.getAbs(TWEET_SEARCH_URL)
                        .as(BodyCodec.jsonObject())
                        .addQueryParam("q", queryToSearch)
                        .putHeader("Authorization", header)
                        .send(handler -> {
                            if (handler.succeeded() && 200 == handler.result().statusCode()) {
                                rc.response().end(handler.result().body().toString());
                            } else {
                                rc.response().end(handler.cause().getMessage());
                            }
                    });
                } else { // Authentication failed
                    rc.response().end(authHandler.cause().getMessage());
                }
            });
    }

/*
    private void twit2(RoutingContext rc) {
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        OAuth2Auth oauth = TwitterAuth.create(vertx, consumerKey, consumerSecret);
        
        oauth.authenticate(new JsonObject(), res -> {
            if (res.failed()) {
                rc.response().end("Access Token Error: " + res.cause().getMessage());
            } else {
                // Get the access token object (the authorization code is given from the previous step).
                AccessToken token = (AccessToken)res.result();
                rc.response().end(token.accessToken().toString());
            }
        });
    }
*/

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