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

import com.google.inject.Inject;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;

import static java.util.Objects.requireNonNull;

public class TrinoHadoopCatalogFactory
        implements TrinoCatalogFactory
{
    private final IcebergConfig config;
    private final CatalogName catalogName;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergTableOperationsProvider tableOperationsProvider;

    @Inject
    public TrinoHadoopCatalogFactory(
            CatalogName catalogName,
            IcebergConfig config,
            HdfsEnvironment hdfsEnvironment,
            IcebergTableOperationsProvider tableOperationsProvider)
    {
        this.config = requireNonNull(config, "config is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
    }

    @Override
    public TrinoCatalog create()
    {
        return new TrinoHadoopCatalog(catalogName, config, hdfsEnvironment, tableOperationsProvider);
    }
}
