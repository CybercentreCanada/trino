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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.collect.ImmutableMap;
import io.trino.hdfs.azure.HiveAzureConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

public class TestIcebergHadoopCatalogConfig
{
    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.azure.abfs.oauth.client-id", "client-id")
                .put("hive.azure.abfs.oauth.endpoint", "https://login.microsoftonline.com/tenantid/oauth2/token")
                .put("hive.azure.abfs.oauth.secret", "foosecret")
                .buildOrThrow();

        HiveAzureConfig expected = new HiveAzureConfig()
                .setAbfsOAuthClientId("client-id")
                .setAbfsOAuthClientEndpoint("https://login.microsoftonline.com/tenantid/oauth2/token")
                .setAbfsOAuthClientSecret("foosecret");

        assertFullMapping(properties, expected);
    }
}
