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
package io.trino.blob.cache.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.CacheFilter;
import alluxio.conf.AlluxioConfiguration;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Comparator.comparingInt;
import static java.util.Locale.ENGLISH;

/**
 * A cache filter that decides whether a path should be cached based on configured patterns.
 * <p>
 * This filter supports three modes via {@link FilterType}:
 * <ul>
 *     <li>{@code CACHE_ALL} - all paths are allowed to be cached.</li>
 *     <li>{@code ALLOW_LIST} - only paths starting with a configured prefix are allowed to be cached.</li>
 *     <li>{@code BLOCK_LIST} - paths starting with a configured prefix are blocked from caching.</li>
 * </ul>
 * <p>
 * The filter is initialized from a JSON configuration file which must contain:
 * <ul>
 *     <li>{@code filterType} - one of {@code CACHE_ALL}, {@code ALLOW_LIST}, or {@code BLOCK_LIST}.</li>
 *     <li>{@code pathPrefixPatterns} - an optional list of literal path prefixes.</li>
 * </ul>
 */
public class PatternBasedCacheFilter
        implements CacheFilter, AutoCloseable
{
    private static final Logger log = Logger.get(PatternBasedCacheFilter.class);

    public enum FilterType
    {
        CACHE_ALL,
        ALLOW_LIST,
        BLOCK_LIST,
    }

    private final Path configPath;
    private final ScheduledThreadPoolExecutor reloadConfigExecutor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("alluxio-cache-filter-reload"));
    private volatile FilterConfig config;

    private record JsonConfig(String filterType, List<String> pathPrefixPatterns) {}

    private record FilterConfig(FilterType filterType, List<String> prefixes) {}

    /**
     * Creates a {@code PatternBasedCacheFilter} based on a configuration file.
     *
     * @param conf the Alluxio configuration (not currently used for pattern evaluation)
     * @param cacheConfigFile path to the JSON config file containing filterType and optional patterns
     * @throws RuntimeException if the config file cannot be read or contains invalid configuration
     */
    public PatternBasedCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        log.debug("Initializing PatternBasedCacheFilter with config file: %s", cacheConfigFile);
        this.configPath = Path.of(cacheConfigFile);
        reloadConfig();
        reloadConfigExecutor.scheduleWithFixedDelay(() -> {
            try {
                reloadConfig();
            }
            catch (RuntimeException e) {
                log.error(e, "Failed to reload cache filter config %s; retaining previous configuration", configPath);
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    void reloadConfig()
    {
        try (BufferedReader reader = Files.newBufferedReader(configPath)) {
            JsonConfig input = new Gson().fromJson(reader, JsonConfig.class);
            checkArgument(input != null && input.filterType() != null, "Missing 'filterType' in cache filter config %s", configPath);
            FilterType filterType = FilterType.valueOf(input.filterType().toUpperCase(ENGLISH));
            List<String> prefixes = List.of();
            if (input.pathPrefixPatterns() != null) {
                checkArgument(input.pathPrefixPatterns().stream().allMatch(prefix -> prefix != null), "Null prefix in cache filter config %s", configPath);
                prefixes = input.pathPrefixPatterns().stream()
                        .sorted(comparingInt(String::length).reversed())
                        .toList();
            }
            config = new FilterConfig(filterType, prefixes);
            log.debug("Cache filter loaded from %s: %s, prefixes: %s", configPath, filterType, prefixes);
        }
        catch (IOException | JsonParseException e) {
            throw new IllegalArgumentException("Failed to read cache filter config " + configPath, e);
        }
    }

    /**
     * Determines whether the given path should be cached based on the filter type and patterns.
     *
     * @param uriStatus the path status to evaluate
     * @return {@code true} if the path should be cached, {@code false} otherwise
     */
    @Override
    public boolean needsCache(URIStatus uriStatus)
    {
        String path = uriStatus.getPath();
        FilterConfig config = this.config;

        if (config.filterType() == FilterType.CACHE_ALL) {
            log.debug("CACHE_ALL enabled, using cache for path: %s", path);
            return true;
        }

        boolean matches = false;
        for (String prefix : config.prefixes()) {
            if (path.startsWith(prefix)) {
                matches = true;
                break;
            }
        }

        log.debug("%s cache filter match for path %s: %s", config.filterType(), path, matches);
        return switch (config.filterType()) {
            case CACHE_ALL -> true;
            case ALLOW_LIST -> matches;
            case BLOCK_LIST -> !matches;
        };
    }

    @Override
    public void close()
    {
        reloadConfigExecutor.shutdownNow();
    }
}
