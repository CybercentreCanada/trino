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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PatternBasedCacheFilter
        implements CacheFilter
{
    private static final Logger LOG = Logger.get(PatternBasedCacheFilter.class);

    public enum FilterType
    {
        CACHE_ALL,
        ALLOW_LIST,
        BLOCK_LIST
    }

    private final FilterType filterType;
    private final List<Pattern> patterns;

    public PatternBasedCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        LOG.debug("Initializing PatternBasedCacheFilter with config file: %s", cacheConfigFile);

        try {
            Path configPath = Paths.get(cacheConfigFile);
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

                LOG.info("Cache Filter initialized with filterType: %s", filterType);
                if (!patterns.isEmpty()) {
                    LOG.info("Cache Filter regex patterns:");
                    for (Pattern p : patterns) {
                        LOG.info("  - %s", p.pattern());
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to initialize PatternBasedCacheFilter", e);
        }
    }

    @Override
    public boolean needsCache(URIStatus uriStatus)
    {
        String path = uriStatus.getPath();

        if (filterType == FilterType.CACHE_ALL) {
            LOG.debug("CACHE_ALL enabled, caching path: %s", path);
            return true;
        }

        boolean matches = patterns.stream().anyMatch(p -> p.matcher(path).matches());

        switch (filterType) {
            case ALLOW_LIST:
                LOG.debug("ALLOW_LIST filter match for path %s: %s", path, matches);
                return matches;
            case BLOCK_LIST:
                LOG.debug("BLOCK_LIST filter match for path %s: %s", path, matches);
                return !matches;
            default:
                throw new IllegalStateException("Unsupported filter type: " + filterType);
        }
    }
}
