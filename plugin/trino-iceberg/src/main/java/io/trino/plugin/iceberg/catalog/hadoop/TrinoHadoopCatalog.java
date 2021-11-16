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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.util.PropertyUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergConfig.convertToCatalogProperties;
import static io.trino.plugin.iceberg.IcebergUtil.quotedTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogUtil.loadCatalog;
import static org.apache.iceberg.TableMetadata.newTableMetadata;
import static org.apache.iceberg.Transactions.createTableTransaction;

public class TrinoHadoopCatalog
        implements TrinoCatalog
{
    private static final Map<String, String> EMPTY_SESSION_MAP = ImmutableMap.of();

    private final String catalogName;
    private final Map<String, String> catalogProperties;
    private final String warehouse;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergTableOperationsProvider tableOperationsProvider;
    private final Cache<String, Catalog> catalogCache;

    private final Map<SchemaTableName, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    public TrinoHadoopCatalog(CatalogName catalogName, IcebergConfig config, HdfsEnvironment hdfsEnvironment, IcebergTableOperationsProvider tableOperationsProvider)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toString();
        requireNonNull(config, "config is null");
        this.catalogProperties = convertToCatalogProperties(config);
        this.warehouse = requireNonNull(config.getCatalogWarehouse(), "warehouse is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        // FIXME: Maybe?
        //this.catalogCache = CacheBuilder.newBuilder().maximumSize(config.getCatalogCacheSize()).build();
        this.catalogCache = CacheBuilder.newBuilder().build();
    }

    /**
     * generate a unique string that represents the session information.
     * The string is used as the cache key, if the session key is the same, it means the same catalog can be reused.
     * The default behavior is to use {@link ConnectorSession#getQueryId()} as the cache key,
     * which means catalog is not shared across queries.
     * Implementations can override this method to use for example the authZ user information of the session instead.
     * @param session session
     * @return session cache key
     */
    public String getSessionCacheKey(ConnectorSession session)
    {
        return session.getQueryId();
    }

    /**
     * Convert a session to a properties map that is used to initialihze a new catalog
     * together with other catalog properties configured at connector level.
     * The default behavior is to return an empty map.
     * Implementations can override this method to pass session properties or session identity information to the catalog.
     * @param session session
     * @return catalog properties derived from session
     */
    public Map<String, String> getSessionProperties(ConnectorSession session)
    {
        return EMPTY_SESSION_MAP;
    }

    private Catalog createNewCatalog(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(catalogProperties);
        builder.putAll(getSessionProperties(session));
        Configuration hadoopConf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(warehouse));
        return loadCatalog(HadoopCatalog.class.getName(), catalogName, builder.build(), hadoopConf);
    }

    private Catalog getCatalog(ConnectorSession session)
    {
        try {
            return catalogCache.get(getSessionCacheKey(session), () -> createNewCatalog(session));
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Fail to create catalog for " + session, e);
        }
    }

    private SupportsNamespaces getNamespaceCatalog(ConnectorSession session)
    {
        Catalog catalog = getCatalog(session);
        if (catalog instanceof SupportsNamespaces) {
            return (SupportsNamespaces) catalog;
        }
        throw new TrinoException(NOT_SUPPORTED, "catalog " + HadoopCatalog.class.getName() + " does not support namespace operations");
    }

    public String getName(ConnectorSession session)
    {
        return getCatalog(session).name();
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return getNamespaceCatalog(session).listNamespaces().stream().map(Namespace::toString).collect(Collectors.toList());
    }

    @Override
    public boolean dropNamespace(ConnectorSession session, String namespace)
            throws NamespaceNotEmptyException
    {
        return getNamespaceCatalog(session).dropNamespace(Namespace.of(namespace));
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
            throws NoSuchNamespaceException
    {
        return getNamespaceCatalog(session).loadNamespaceMetadata(Namespace.of(namespace)).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + getName(session));
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        getNamespaceCatalog(session).createNamespace(Namespace.of(namespace), properties.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
            throws NoSuchNamespaceException
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + getName(session));
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + getName(session));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        List<Namespace> namespaces = namespace.isPresent() ? ImmutableList.of(Namespace.of(namespace.get())) : getNamespaceCatalog(session).listNamespaces();
        return namespaces.stream().flatMap(ns -> getCatalog(session).listTables(ns).stream().map(IcebergUtil::fromTableId)).collect(Collectors.toList());
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String location, Map<String, String> properties)
    {
        TableMetadata metadata = newTableMetadata(schema, partitionSpec, location, properties);
        TableOperations ops = tableOperationsProvider.createTableOperations(
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                Optional.of(session.getUser()),
                Optional.of(location));
        return createTableTransaction(schemaTableName.toString(), ops, metadata);
    }

    @Override
    public boolean dropTable(ConnectorSession session, SchemaTableName schemaTableName, boolean purgeData)
    {
        return getCatalog(session).dropTable(TableIdentifier.of(schemaTableName.getSchemaName(), schemaTableName.getTableName()), purgeData);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        getCatalog(session).renameTable(TableIdentifier.of(from.getSchemaName(), from.getTableName()), TableIdentifier.of(to.getSchemaName(), to.getTableName()));
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableMetadata metadata = tableMetadataCache.computeIfAbsent(
                schemaTableName,
                ignore -> {
                    TableOperations operations = tableOperationsProvider.createTableOperations(
                            session,
                            schemaTableName.getSchemaName(),
                            schemaTableName.getTableName(),
                            Optional.empty(),
                            Optional.empty());
                    return new BaseTable(operations, quotedTableName(schemaTableName)).operations().current();
                });

        IcebergTableOperations operations = tableOperationsProvider.createTableOperations(
                session,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                Optional.empty(),
                Optional.empty());
        operations.initializeFromMetadata(metadata);
        return new BaseTable(operations, quotedTableName(schemaTableName));
    }

    @Override
    public void updateTableComment(ConnectorSession session, SchemaTableName schemaTableName, Optional<String> comment)
    {
        UpdateProperties update = loadTable(session, schemaTableName).updateProperties();
        comment.ifPresentOrElse(c -> update.set(TABLE_COMMENT, c), () -> update.remove(TABLE_COMMENT));
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String dbLocationUri = PropertyUtil.propertyAsString(
                getNamespaceCatalog(session).loadNamespaceMetadata(Namespace.of(schemaTableName.getSchemaName())), "locationUri", null);

        if (dbLocationUri != null) {
            return String.format("%s/%s", dbLocationUri, schemaTableName.getTableName());
        }
        else {
            return String.format("%s/%s/%s", warehouse, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported by " + getName(session));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported by " + getName(session));
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported by " + getName(session));
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported by " + getName(session));
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported by " + getName(session));
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> namespace)
    {
        return null;
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewIdentifier)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return ImmutableList.of();
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + getName(session));
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by " + getName(session));
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported by " + getName(session));
    }
}
