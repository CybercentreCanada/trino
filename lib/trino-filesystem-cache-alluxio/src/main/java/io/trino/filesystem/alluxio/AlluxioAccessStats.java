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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class AlluxioAccessStats
        implements Runnable
{
    private static final Logger log = Logger.get(AlluxioAccessStats.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, Stats> externalReads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Stats> cacheReads = new ConcurrentHashMap<>();

    private static class Stats
    {
        final LongAdder hits = new LongAdder();
        final LongAdder bytes = new LongAdder();

        void add(int byteCount)
        {
            hits.increment();
            bytes.add(byteCount);
        }

        Map<String, Long> toMap()
        {
            return Map.of(
                "hits", hits.sum(),
                "bytes", bytes.sum());
        }
    }

    private String normalizePath(String path)
    {
        int idx = path.indexOf("/data/");
        if (idx > 0) {
            return path.substring(0, idx);
        }

        return path;
    }

    public void recordExternalRead(int bytes, Location location)
    {
        externalReads.computeIfAbsent(normalizePath(location.toString()), p -> new Stats()).add(bytes);
    }

    public void recordCacheRead(int bytes, Location location)
    {
        cacheReads.computeIfAbsent(normalizePath(location.toString()), p -> new Stats()).add(bytes);
    }

    @Override
    public void run()
    {
        // Take snapshots and clear maps atomically to avoid race conditions
        Map<String, Map<String, Long>> externalSnapshot = externalReads.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().toMap()));
        externalReads.clear();

        Map<String, Map<String, Long>> cacheSnapshot = cacheReads.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().toMap()));
        cacheReads.clear();

        // Skip logging if both are empty
        if (externalSnapshot.isEmpty() && cacheSnapshot.isEmpty()) {
            return;
        }

        Map<String, Object> output = Map.of(
                "externalReads", externalSnapshot,
                "cacheReads", cacheSnapshot);

        try {
            String json = mapper.writeValueAsString(output);
            log.info("AlluxioAccessStats: " + json);
        }
        catch (JsonProcessingException e) {
            log.error("Failed to serialize AlluxioAccessStats", e);
        }
    }
}
