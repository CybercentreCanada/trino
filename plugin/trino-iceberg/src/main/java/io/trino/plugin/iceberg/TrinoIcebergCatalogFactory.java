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
package io.trino.plugin.iceberg;

import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;

class TrinoIcebergCatalogFactory
{
    private final IcebergCatalogType catalogType;
    private final CatalogName catalogName;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveTableOperationsProvider tableOperationsProvider;

    @Inject
    public TrinoIcebergCatalogFactory(
            CatalogName catalogName,
            IcebergConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HiveTableOperationsProvider tableOperationsProvider)
    {
        this.catalogType = config.getCatalogType();
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.tableOperationsProvider = tableOperationsProvider;
    }

    public TrinoSessionCatalog create()
    {
        switch (catalogType) {
            case HIVE:
                return new TrinoHiveSessionCatalog(catalogName, metastore, hdfsEnvironment, typeManager, tableOperationsProvider);
            default:
                throw new TrinoException(CONFIGURATION_INVALID, "Unknown Trino Iceberg catalog type " + catalogType);
        }
    }
}
