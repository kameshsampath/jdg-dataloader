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

import java.util.List;
import java.util.Objects;

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

        vertx.deployVerticle(DataProcessorVerticle.class.getName(), deploymentOptions);

        fileSystem.readDir(dataDir, fileListResult -> {
            if (fileListResult.succeeded()) {
                List<String> files = fileListResult.result();

                Observable<String> filesObservables = Observable.from(files);

                filesObservables.subscribe(dataFileName -> {

                    LOGGER.debug("Processing file: {}", dataFileName);

                    eventBus.send("DATA_LOADER", dataFileName, reply -> {
                        if (reply.succeeded()) {
                            LOGGER.info(">>>" + reply.result().body());
                        } else {
                            LOGGER.error("Error processing file :", reply.cause());
                        }
                    });

                }, err -> LOGGER.error("Error reading file ", err));
            }
        });
    }
}
