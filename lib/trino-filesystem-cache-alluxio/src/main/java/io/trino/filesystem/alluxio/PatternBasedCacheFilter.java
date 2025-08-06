package io.trino.filesystem.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;

import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.airlift.log.Logger;

public class PatternBasedCacheFilter implements CacheFilter {
    private static final Logger LOG = Logger.get(PatternBasedCacheFilter.class);

    public enum FilterType
    {
        CACHE_ALL,
        ALLOW_LIST,
        BLOCK_LIST
    }

    public PatternBasedCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        LOG.debug("Initializing PatternBasedCacheFilter with config file: %s", configFilePath);

        try {
            Map<String, Object> config = new Gson().fromJson(
                new FileReader(configFilePath),
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
            } else {
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
        } catch (Exception e) {
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
