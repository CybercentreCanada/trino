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
package io.trino.plugin.phoenix;

import org.apache.phoenix.util.SchemaUtil;

import javax.annotation.Nullable;

import static io.trino.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static org.apache.phoenix.query.QueryConstants.NULL_SCHEMA_NAME;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static @Nullable String getEscapedTableName(@Nullable String schema, String table)
    {
        return SchemaUtil.getEscapedTableName(toPhoenixSchemaName(schema), table);
    }

    public static @Nullable String toPhoenixSchemaName(@Nullable String trinoSchemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(trinoSchemaName) ? NULL_SCHEMA_NAME : trinoSchemaName;
    }

    public static @Nullable String toTrinoSchemaName(@Nullable String phoenixSchemaName)
    {
        return NULL_SCHEMA_NAME.equalsIgnoreCase(phoenixSchemaName) ? DEFAULT_SCHEMA : phoenixSchemaName;
    }
}
