package io.dropwizard.elasticsearch.managed;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

import com.google.common.io.Resources;

import io.dropwizard.elasticsearch.config.EsConfiguration;
import io.dropwizard.elasticsearch.util.TransportAddressHelper;
import io.dropwizard.lifecycle.Managed;

/**
 * A Dropwizard managed Elasticsearch {@link RestClient}. A {@link RestHighLevelClient} is created and its lifecycle is managed by Dropwizard.
 *
 * @see <a href="http://www.elasticsearch.org/guide/reference/java-api/client/#transportclient">Transport Client</a>
 */
public class ManagedEsClient implements Managed {
    private RestHighLevelClient client = null;

    /**
     * Create a new managed Elasticsearch {@link Client}. If {@link EsConfiguration#nodeClient} is {@literal true}, a Node Client is being created, otherwise a
     * {@link TransportClient} is being created with {@link EsConfiguration#servers} as transport addresses.
     *
     * @param config
     *            a valid {@link EsConfiguration} instance
     */
    public ManagedEsClient(final EsConfiguration config) {
        checkNotNull(config, "EsConfiguration must not be null");

        final Settings.Builder settingsBuilder = Settings.builder();
        if (!isNullOrEmpty(config.getSettingsFile())) {
            Path path = Paths.get(config.getSettingsFile());
            if (!path.toFile().exists()) {
                try {
                    final URL url = Resources.getResource(config.getSettingsFile());
                    path = new File(url.toURI()).toPath();
                } catch (URISyntaxException | NullPointerException e) {
                    throw new IllegalArgumentException("settings file cannot be found", e);
                }
            }
            try {
                settingsBuilder.loadFromPath(path);
            } catch (IOException ioe) {
                throw new IllegalArgumentException("exception loading file", ioe);
            }
        }

        final Settings settings = settingsBuilder
                                                 .putProperties(config.getSettings(), key -> key)
                                                 .put("cluster.name", config.getClusterName())
                                                 .build();

        final HttpHost[] addresses = TransportAddressHelper.httpHostsfromHostAndPorts(config.getServers());
        
        RestClientBuilder rcb = RestClient.builder(addresses);
        
        // TODO settings?
        
        this.client = new RestHighLevelClient(rcb);
    }

    /**
     * Stops the Elasticsearch {@link RestHighLevelClient}. Called <i>after</i> the service is no longer accepting requests.
     *
     * @throws Exception
     *             if something goes wrong.
     */
    @Override
    public void stop() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void start() throws Exception {
        
    }
}
