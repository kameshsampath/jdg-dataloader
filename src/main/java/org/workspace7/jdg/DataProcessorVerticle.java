package org.workspace7.jdg;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author kameshs
 */
public class DataProcessorVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(DataProcessorVerticle.class);

    private RemoteCache<String, Object> cache1;
    private RemoteCache<String, Object> cache2;

    @Override
    public void start() throws Exception {

        LOGGER.info("Starting Data Processor Verticle");

        EventBus eventBus = vertx.eventBus();
        String dataDir = config().getString("dataDir");

        Boolean clear = config().getBoolean("clearCache", false);

        Objects.requireNonNull(dataDir, "Invalid Data Dir :" + dataDir);

        FileSystem fileSystem = vertx.fileSystem();

        MessageConsumer<JsonObject> fileConsumer = eventBus
                .<JsonObject>consumer("DATA_LOADER", message -> {
                    JsonObject data = message.body();

                    String dataFileName = data.getString("fileName");
                    Long startTime = data.getLong("startTime");

                    LOGGER.debug("Loading data form file :" + dataFileName);

                    fileSystem.open(dataFileName, new OpenOptions(), ares -> {

                        AsyncFile file = ares.result();

                        if (ares.succeeded()) {

                            file.handler(buffer -> {

                                String contentBuffer = buffer.toString();

                                String[] contentStrings = contentBuffer.split("\n");

                                if (contentStrings == null || contentStrings.length <= 0) {
                                    JsonObject reply = new JsonObject()
                                            .put("dataFile", dataFileName)
                                            .put("status", "Warning")
                                            .put("message", "No data found in file");
                                }

                                Map<String, Object> cacheableValues = Stream.of(contentStrings)
                                        .map(s -> s.replaceAll("\r", ""))
                                        .filter(s -> s.contains(":") && s.indexOf(":") != -1)
                                        .map(s -> s.split(":"))
                                        .collect(Collectors.toMap(o -> o[0], o -> o[1]));

                                putInJDG(cacheableValues, putResult -> {

                                    long timeDiff = System.currentTimeMillis() - startTime;

                                    if (putResult.succeeded()) {
                                        JsonObject reply = new JsonObject()
                                                .put("dataFile", dataFileName)
                                                .put("recordCount", cacheableValues.entrySet().size())
                                                .put("status", "Successful")
                                                .put("message", "Loaded data from file ")
                                                .put("timeTaken", timeDiff + "(ms)");
                                        message.reply(reply);
                                    }
                                });


                            });
                        } else {
                            LOGGER.error("Error processing data", ares.cause());
                            JsonObject reply = new JsonObject()
                                    .put("dataFile", dataFileName)
                                    .put("status", "Error")
                                    .put("message", ares.cause().getMessage());
                            message.reply(reply);
                        }

                    });

                }).pause(); // don't start to consume file immediately as we need JDG client for processing

        //Configure the JDG client
        configureJDG(jdgHandler ->

        {
            if (jdgHandler.succeeded()) {
                RemoteCacheManager cacheManager = jdgHandler.result();
                cache1 = cacheManager.getCache("TEST_CACHE_ONE");
                cache2 = cacheManager.getCache("TEST_CACHE_TWO");
                if (clear) {
                    LOGGER.info("Clearing Cache:TEST_CACHE_ONE");
                    cache1.clear();
                    LOGGER.info("Clearing Cache:TEST_CACHE_TWO");
                    cache2.clear();
                }
                fileConsumer.resume();
            }
        });
    }

    private void configureJDG(
            Handler<AsyncResult<RemoteCacheManager>> jdgHandler) {
        String jdgServers = config().getString("jdgServers");
        Objects.requireNonNull(jdgServers,
                "Invalid JDG Servers :" + jdgServers);
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServers(jdgServers);
        RemoteCacheManager cacheManager = new RemoteCacheManager(
                builder.build());
        jdgHandler.handle(Future.succeededFuture(cacheManager));
    }

    protected void putInJDG(Map<String, Object> cacheableValues, Handler<AsyncResult<Void>> putHandler) {
        vertx.executeBlocking(future -> {
                    try {
                        cache1.putAll(cacheableValues);
                        future.complete();
                    } catch (Exception e) {
                        future.fail(e);
                    }
                },
                (AsyncResult<Object> prevPutResult) -> {
                    if (prevPutResult.succeeded()) {
                        try {
                            Map<String, String> reverseMap = cacheableValues.
                                    entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(
                                            o -> String.valueOf(o.getValue()),
                                            o -> o.getKey(), (u, u2) -> u));
                            cache2.putAll(reverseMap);
                            putHandler.handle(Future.succeededFuture());
                        } catch (Exception e) {
                            putHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        LOGGER.error("Unable to add data :", prevPutResult.cause());
                        putHandler.handle(Future.failedFuture(prevPutResult.cause()));
                    }
                });
    }

}
