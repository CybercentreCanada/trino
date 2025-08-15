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
package io.trino.filesystem.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.CacheFilter;
import alluxio.conf.AlluxioConfiguration;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;

/**
 * A cache filter that decides whether a path should be cached based on configured patterns.
 * <p>
 * This filter supports three modes via {@link FilterType}:
 * <ul>
 *     <li>{@code CACHE_ALL} - all paths are allowed to be cached.</li>
 *     <li>{@code ALLOW_LIST} - only paths matching the configured regex patterns are allowed to be cached.</li>
 *     <li>{@code BLOCK_LIST} - paths matching the configured regex patterns are blocked from caching.</li>
 * </ul>
 * <p>
 * The filter is initialized from a JSON configuration file which must contain:
 * <ul>
 *     <li>{@code filterType} - one of {@code CACHE_ALL}, {@code ALLOW_LIST}, or {@code BLOCK_LIST}.</li>
 *     <li>{@code regxPatternStrList} - a list of regex strings, required for ALLOW_LIST or BLOCK_LIST.</li>
 * </ul>
 */
public class PatternBasedCacheFilter
        implements CacheFilter
{
    private static final Logger log = Logger.get(PatternBasedCacheFilter.class);

    public enum FilterType
    {
        CACHE_ALL,
        ALLOW_LIST,
        BLOCK_LIST
    }

    private final Path configPath;
    private final ScheduledThreadPoolExecutor reloadConfigExecutor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("reload-config"));
    private volatile FilterType filterType;
    private volatile List<Pattern> patterns;

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
        this.configPath = Paths.get(cacheConfigFile);
        this.reloadConfigExecutor.scheduleWithFixedDelay(this::reloadConfig, 0, 60, TimeUnit.SECONDS);
    }

    private void reloadConfig()
    {
        try {
            try (BufferedReader reader = Files.newBufferedReader(configPath)) {
                Map<String, Object> config = new Gson().fromJson(
                        reader,
                        new TypeToken<Map<String, Object>>() {}.getType());

                String filterTypeStr = (String) config.get("filterType");
                if (filterTypeStr == null) {
                    throw new IllegalArgumentException("Missing 'filterType' in cache filter config.");
                }

                filterType = FilterType.valueOf(filterTypeStr.toUpperCase());

                List<String> patternStrs = (List<String>) config.get("regxPatternStrList");

                if ((filterType == FilterType.ALLOW_LIST || filterType == FilterType.BLOCK_LIST) && (patternStrs == null || patternStrs.isEmpty())) {
                    throw new IllegalArgumentException("'regxPatternStrList' must be provided for ALLOW_LIST or BLOCK_LIST.");
                }

                if (patternStrs == null) {
                    patterns = Collections.emptyList();
                }
                else {
                    patterns = patternStrs.stream()
                        .map(Pattern::compile)
                        .collect(Collectors.toList());
                }

                log.debug("Cache Filter initialized with filterType: %s", filterType);
                if (!patterns.isEmpty()) {
                    log.debug("Cache Filter regex patterns:");
                    for (Pattern p : patterns) {
                        log.debug("  - %s", p.pattern());
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize PatternBasedCacheFilter", e);
        }
    }

    /**
     * Determines whether the given path should be cached based on the filter type and patterns.
     *
     * @param uriStatus the path status to evaluate
     * @return {@code true} if the path should be cached, {@code false} otherwise
     * @throws IllegalStateException if the filter type is unsupported
     */
    @Override
    public boolean needsCache(URIStatus uriStatus)
    {
        String path = uriStatus.getPath();

        if (filterType == FilterType.CACHE_ALL) {
            log.debug("CACHE_ALL enabled, using cache for path: %s", path);
            return true;
        }

        boolean matches = patterns.stream().anyMatch(p -> p.matcher(path).matches());

        switch (filterType) {
            case ALLOW_LIST:
                log.debug("ALLOW_LIST cache filter match for path %s: %s", path, matches);
                return matches;
            case BLOCK_LIST:
                log.debug("BLOCK_LIST cache filter match for path %s: %s", path, matches);
                return !matches;
            default:
                throw new IllegalStateException("Unsupported filter type: " + filterType);
        }
    }
}
