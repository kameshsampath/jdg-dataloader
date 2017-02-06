package org.workspace7.jdg;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author kameshs
 */
public class DataCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataCheck.class);

    public static void main(String[] args) {

        try {
            Yaml yaml = new Yaml();

            Map appConfig = (Map) yaml.load(new FileInputStream("src/main/conf/application.yml"));

            String jdgServers = (String) appConfig.get("jdgServers");

            Objects.requireNonNull(jdgServers,
                    "Invalid JDG Servers :" + jdgServers);

            ConfigurationBuilder builder = new ConfigurationBuilder();

            builder.addServers(jdgServers);

            RemoteCacheManager cacheManager = new RemoteCacheManager(
                    builder.build());

            List<String> reverseCaches = (List<String>) appConfig.get("reverseCaches");

            String forwardCache = reverseCaches.get(0);
            String reverseCache = reverseCaches.get(1);

            RemoteCache<String, Object> cacheOne = cacheManager.getCache(forwardCache);
            RemoteCache<String, Object> cacheTwo = cacheManager.getCache(reverseCache);

            String forwardValue = (String) cacheOne.get("78BBBAC0796Q1ZG");
            Objects.requireNonNull(forwardValue, "Value for key 78BBBAC0796Q1ZG must be present");
            String reverseValue = (String) cacheTwo.get(forwardValue);
            Objects.requireNonNull(forwardValue, "Value for key " + forwardValue + " must be present");

            LOGGER.info("Cache {}: 78BBBAC0796Q1ZG={}", forwardCache, forwardValue);

            LOGGER.info("Cache {}: {}={}", reverseCache, forwardValue, reverseValue);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
