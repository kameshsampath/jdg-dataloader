package org.workspace7.jdg;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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

                    fileSystem.readFile(dataFileName, bufferAsyncResult -> {

                        if (bufferAsyncResult.succeeded()) {

                            String contentBuffer = bufferAsyncResult.result()
                                    .toString();

                            rx.Observable<String> cacheValues = rx.Observable
                                    .from(contentBuffer.split("\n"));

                            cacheValues.subscribe(s -> {

                                if (s.contains(":") && s.indexOf(":") != -1) {
                                    String[] keyValuePair = s.split(":");

                                    LOGGER.trace("Adding {}={}",
                                            keyValuePair[0], keyValuePair[1]);

                                    putInJDG(keyValuePair[0], keyValuePair[1], putResult -> {

                                        if (putResult.succeeded()) {
                                            long stopTime = System.currentTimeMillis();
                                            long timeDiff = stopTime - startTime;
                                            message.reply("Successfully loaded data from  file [" + dataFileName + "] in " + timeDiff + "(ms)");
                                        } else {
                                            message.reply("Failed Loading data from  file [" + dataFileName + "]");
                                        }
                                    });
                                }
                            }, err -> {
                                LOGGER.error("Error processing data", err);
                                message.reply("Failed Loading data from  file[ " + dataFileName + "]");
                            });

                        } else {
                            LOGGER.error("Error loading file[ " + dataFileName + "]",
                                    bufferAsyncResult.cause());
                            message.reply("Failed Loading data from  file [" + dataFileName + "]");
                        }
                    });

                }).pause(); // don't start to consume file immediately as we need JDG client for processing

        //Configure the JDG client
        configureJDG(jdgHandler -> {
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

    protected void putInJDG(String key, String value, Handler<AsyncResult<Void>> putHandler) {
        vertx.executeBlocking(future -> {
                    try {
                        cache1.put(key, value);
                        future.complete();
                    } catch (Exception e) {
                        future.fail(e);
                    }
                },
                prevPutResult -> {
                    if (prevPutResult.succeeded()) {
                        cache2.put(value, key);
                        putHandler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error("Unable to add data :", prevPutResult.cause());
                        putHandler.handle(Future.failedFuture(prevPutResult.cause()));
                    }
                });
    }

}
