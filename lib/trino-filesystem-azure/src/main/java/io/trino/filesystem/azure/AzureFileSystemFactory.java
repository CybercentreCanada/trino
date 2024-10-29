/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    private static final Logger log = Logger.get(AzureFileSystemFactory.class);

    private final AzureAuth auth;
    private final String endpoint;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final TracingOptions tracingOptions;
    private final OkHttpClient okHttpClient;
    private final HttpClient httpClient;

    @Inject
    public AzureFileSystemFactory(OpenTelemetry openTelemetry, AzureAuth azureAuth, AzureFileSystemConfig config)
    {
        this(openTelemetry,
                azureAuth,
                config.getEndpoint(),
                config.getReadBlockSize(),
                config.getWriteBlockSize(),
                config.getMaxWriteConcurrency(),
                config.getMaxSingleUploadSize(),
                config.getMaxHttpRequests());
    }

    public AzureFileSystemFactory(
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth,
            String endpoint,
            DataSize readBlockSize,
            DataSize writeBlockSize,
            int maxWriteConcurrency,
            DataSize maxSingleUploadSize,
            int maxHttpRequests)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");
        this.tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);

        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(maxHttpRequests);
        dispatcher.setMaxRequestsPerHost(maxHttpRequests);
        okHttpClient = new OkHttpClient.Builder()
                .dispatcher(dispatcher)
                .build();
        HttpClientOptions clientOptions = new HttpClientOptions();
        clientOptions.setTracingOptions(tracingOptions);
        // Double the default idle connection pool size from 5 to 10
        clientOptions.setMaximumConnectionPoolSize(10);
        httpClient = createAzureHttpClient(okHttpClient, clientOptions);
    }

    @PreDestroy
    public void destroy()
    {
        okHttpClient.dispatcher().executorService().shutdownNow();
        okHttpClient.connectionPool().evictAll();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new AzureFileSystem(httpClient, tracingOptions, auth, endpoint, readBlockSize, writeBlockSize, maxWriteConcurrency, maxSingleUploadSize);
    }

    public static HttpClient createAzureHttpClient(OkHttpClient okHttpClient, HttpClientOptions clientOptions)
    {
        Integer poolSize = clientOptions.getMaximumConnectionPoolSize();
        // By default, OkHttp uses a maximum idle connection count of 5.
        int maximumConnectionPoolSize = (poolSize != null && poolSize > 0) ? poolSize : 5;

        // Set lower idle connection timeout than default
        long idleConnectionTimeout = 30 * 1000; // in milliseconds

        try {
            builder = new OkHttpAsyncHttpClientBuilder(okHttpClient)
                    .proxy(clientOptions.getProxyOptions())
                    .configuration(clientOptions.getConfiguration())
                    .connectionTimeout(clientOptions.getConnectTimeout())
                    .writeTimeout(clientOptions.getWriteTimeout())
                    .readTimeout(clientOptions.getReadTimeout())
                    .connectionPool(new ConnectionPool(maximumConnectionPoolSize,
                            idleConnectionTimeout, TimeUnit.MILLISECONDS));
            // Log OkHttpClient properties
            log.info("Creating OkHttpClient with; Connection pool: %s; Connect timeout: %s ms; Read timeout: %s ms; Write timeout: %s ms; Dispatcher: %s; Proxy options: %s",
                builder.connectionPool,
                builder.connectionTimeout,
                builder.readTimeout,
                builder.writeTimeout,
                builder.dispatcher,
                builder.proxyOptions);
            return builder.build();
        }
        catch (Exception e) {
            // Log the error and rethrow as a runtime exception
            log.error("Failed to create Azure HTTP client: %s", e.getMessage());
            throw new RuntimeException("Unable to create Azure HTTP client", e);
        }
    }
}
