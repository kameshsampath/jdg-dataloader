package org.workspace7.jdg;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.rxjava.core.file.FileSystem;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author kameshs
 */
public class DataProcessorVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(DataProcessorVerticle.class);

    private final Pattern PATTERN = Pattern.compile("(?ms)(?<key>\\w+\\d*)(:)(?<value>\\w+\\d*)$");

    private RemoteCache<String, Object> cache1;
    private RemoteCache<String, Object> cache2;

    @Override
    public void start() throws Exception {

        LOGGER.info("Starting Data Processor Verticle");

        EventBus eventBus = vertx.eventBus();
        String dataDir = config().getString("dataDir");

        Objects.requireNonNull(dataDir, "Invalid Data Dir :" + dataDir);

        boolean clear = config().getBoolean("clearCache");

        FileSystem fileSystem = vertx.fileSystem();

        MessageConsumer<JsonObject> fileConsumer = eventBus.consumer("DATA_LOADER", message -> {

            JsonObject data = message.body();

            String dataFileName = data.getString("fileName");
            Long startTime = data.getLong("startTime");

            LOGGER.info("Loading data form file {}", dataFileName);

            ConcurrentHashMap cacheableValues = new ConcurrentHashMap();

            fileSystem
                    .rxReadFile(dataFileName)
                    .flatMapObservable(buffer ->
                            Observable.from(buffer.toString("utf-8").split("\r?\\n$")))
                    .filter(line -> !line.trim().isEmpty())
                    .map(line -> {
                        Matcher matcher = PATTERN.matcher(line);
                        HashMap<String, Object> temp = new HashMap<>();
                        while (matcher.find()) {
                            temp.put(matcher.group("key"), matcher.group("value"));
                        }
                        return temp;
                    })
                    .subscribe(objectMap -> cacheableValues.putAll(objectMap)
                            , throwable -> LOGGER.error("Error:", throwable), () -> {
                                LOGGER.info("Loading {} keys", cacheableValues.size());
                                putInJDG(cacheableValues,
                                        putResult -> {
                                            long timeDiff = System.currentTimeMillis() - startTime;
                                            if (putResult.succeeded()) {
                                                JsonObject reply = new JsonObject()
                                                        .put("dataFile", dataFileName)
                                                        .put("recordCount", cacheableValues.keySet().size())
                                                        .put("status", "Successful")
                                                        .put("message", "Loaded data from file ")
                                                        .put("timeTaken", timeDiff + "(ms)");
                                                message.reply(reply);
                                            }

                                        });
                            });
        });

        //Configure the JDG client
        configureJDG(jdgHandler ->

        {
            if (jdgHandler.succeeded()) {
                RemoteCacheManager cacheManager = jdgHandler.result();
                JsonArray reverseCaches = config().getJsonArray("reverseCaches");
                cache1 = cacheManager.getCache(reverseCaches.getString(0), false);
                cache2 = cacheManager.getCache(reverseCaches.getString(1), false);
                if (clear) {
                    LOGGER.info("Clearing Cache:{}", reverseCaches.getString(0));
                    cache1.clear();
                    LOGGER.info("Clearing Cache:{}", reverseCaches.getString(1));
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
