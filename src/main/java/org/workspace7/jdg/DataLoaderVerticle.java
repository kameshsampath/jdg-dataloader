package org.workspace7.jdg;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

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

        Objects.requireNonNull(jdgServers, "Required valued values for JDG Servers");

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(new JsonObject()
                .put("dataDir", dataDir)
                .put("jdgServers", jdgServers)
                .put("clearCache", clearCache));

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
                        processingData.put("fileName",dataFileName);
                        processingData.put("startTime",System.currentTimeMillis());
                        eventBus.send("DATA_LOADER", processingData, reply -> {
                            if (reply.succeeded()) {
                                LOGGER.info(">>>" + reply.result().body());
                                fileCount.incrementAndGet();
                            } else {
                                LOGGER.error("Error processing file :", reply.cause());
                            }
                        });
                    }

                }, err -> LOGGER.error("Error reading file ", err));
            }
        });
    }
}
