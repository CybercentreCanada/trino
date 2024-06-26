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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;

public class TestBucketBalancerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BucketBalancerConfig.class)
                .setBalancerEnabled(true)
                .setBalancerInterval(new Duration(6, HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("storage.balancer-enabled", "false")
                .put("storage.balancer-interval", "5h")
                .buildOrThrow();

        BucketBalancerConfig expected = new BucketBalancerConfig()
                .setBalancerEnabled(false)
                .setBalancerInterval(new Duration(5, HOURS));

        assertFullMapping(properties, expected);
    }
}
