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
import io.trino.filesystem.manager.FileSystemConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestIcebergHadoopCatalogFileSystemConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FileSystemConfig.class)
                .setHadoopEnabled(true)
                .setNativeAzureEnabled(false)
                .setNativeS3Enabled(true)
                .setNativeGcsEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-azure.enabled", "true")
                .put("fs.native-s3.enabled", "false")
                .put("fs.native-gcs.enabled", "false")
                .buildOrThrow();

        FileSystemConfig expected = new FileSystemConfig()
                .setHadoopEnabled(false)
                .setNativeAzureEnabled(true)
                .setNativeS3Enabled(false)
                .setNativeGcsEnabled(false);

        assertFullMapping(properties, expected);
    }
}
