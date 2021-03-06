package org.workspace7.jdg;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Single;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kameshs
 */
public class DataLoaderVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataLoaderVerticle.class);

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        EventBus eventBus = vertx.eventBus();

        FileSystem fileSystem = vertx.fileSystem();

        String dataDir = config().getString("dataDir");
        String jdgServers = config().getString("jdgServers");
        Boolean clearCache = config().getBoolean("clearCache", false);
        int workerInstances = config().getInteger("instances", 1);

        JsonArray reverseCaches = config().getJsonArray("reverseCaches");

        Objects.requireNonNull(jdgServers, "Required valued values for JDG Servers 'jdgServers'");
        Objects.requireNonNull(reverseCaches, "Required cache names 'reverseCaches'");

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(new JsonObject()
                .put("dataDir", dataDir)
                .put("jdgServers", jdgServers)
                .put("clearCache", clearCache)
                .put("reverseCaches", reverseCaches));
        deploymentOptions.setInstances(workerInstances);

        deploymentOptions.setWorker(true);
        AtomicInteger fileCount = new AtomicInteger();

        fileSystem.readDir(dataDir, fileListResult -> {

            if (fileListResult.succeeded()) {

                vertx.deployVerticle(DataProcessorVerticle.class.getName(), deploymentOptions);

                Observable<String> filesObservables = Observable.from(fileListResult.result());

                filesObservables.subscribe(dataFileName -> {

                    if (dataFileName.endsWith(".txt")) {
                        LOGGER.info("Processing file: {}", dataFileName);

                        JsonObject processingData = new JsonObject();
                        processingData.put("fileName", dataFileName);
                        processingData.put("startTime", System.currentTimeMillis());
                        Single<Message<JsonObject>> reply = eventBus.rxSend("DATA_LOADER", processingData);
                        reply.subscribe(resp -> LOGGER.info("{}", resp.body()),
                                throwable -> LOGGER.error("Error", throwable));
                    }

                }, err -> LOGGER.error("Error reading file ", err));
            }
        });
    }
}
