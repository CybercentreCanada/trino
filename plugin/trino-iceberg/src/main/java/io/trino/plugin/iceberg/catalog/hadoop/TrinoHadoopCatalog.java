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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.cache.SafeCaches;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class TrinoHadoopCatalog
        extends AbstractTrinoCatalog
{
    private static final Map<String, String> EMPTY_SESSION_MAP = ImmutableMap.of();
    private static final Joiner SLASH = Joiner.on("/");
    private static final int PER_QUERY_CACHES_SIZE = 1000;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final Map<String, String> catalogProperties;
    private final Cache<String, Catalog> catalogCache;
    private final TrinoFileSystem trinoFileSystem;
    private final CatalogName catalogName;
    private final String warehouse;
    private final TrinoFileSystem trinoFileSystem;
    private final Cache<SchemaTableName, TableMetadata> tableMetadataCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(PER_QUERY_CACHES_SIZE)
            .build();

    public TrinoHadoopCatalog(
            CatalogName catalogName,
            TypeManager typeManager,
            ConnectorIdentity identity,
            IcebergTableOperationsProvider tableOperationsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            boolean useUniqueTableLocation,
            IcebergConfig icebergConfig)
    {
        super(catalogName, typeManager, tableOperationsProvider, fileSystemFactory, useUniqueTableLocation);
        this.catalogCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(icebergConfig.getCatalogCacheSize()));
        this.warehouse = requireNonNull(icebergConfig.getCatalogWarehouse(), "warehouse is null");
        this.trinoFileSystem = fileSystemFactory.create(identity);
        this.catalogName = catalogName;

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(icebergConfig.getCatalogWarehouse()),
                "Cannot initialize HadoopCatalog because warehousePath must not be null or empty");

    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        if (!namespace.equals(namespace.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            return false;
        }
        return listNamespaces(session).stream().filter(namespace::equals).findAny().orElse(null) != null;
    }

    private List<String> listNamespaces(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            return ImmutableList.of(namespace.get());
        }
        return listNamespaces(session);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        try {
            return trinoFileSystem.listDirectories(Location.of(warehouse)).stream().filter(this::isNamespace).map(Location::fileName).toList();
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "could not list namespaces", e);
        }
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        Location nsLocation = Location.of(SLASH.join(warehouse, namespace));

        if (!isNamespace(nsLocation) || namespace.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, "could not find namespace");
        }

        try {
            if (!trinoFileSystem.listDirectories(nsLocation).isEmpty() || trinoFileSystem.listFiles(nsLocation).hasNext()) {
                throw new TrinoException(SCHEMA_NOT_EMPTY,String.format("Namespace %s is not empty.", namespace));
            }

            trinoFileSystem.deleteDirectory(nsLocation);
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Namespace delete failed: %s", namespace), e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        Location nsLocation = Location.of(SLASH.join(warehouse, namespace));

        if (!isNamespace(nsLocation) || namespace.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, "could not find namespace");
        }

        return ImmutableMap.of("location", nsLocation.toString());
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        checkArgument(owner.getType() == PrincipalType.USER, "Owner type must be USER");
        checkArgument(owner.getName().equals(session.getUser().toLowerCase(ENGLISH)), "Explicit schema owner is not supported");

        checkArgument(
                !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
        if (!properties.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Cannot create namespace " + namespace + ": metadata is not supported");
        }

        Location nsLocation = Location.of(SLASH.join(warehouse, namespace));

        if (isNamespace(nsLocation)) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS,  String.format("Namespace already exists: %s", namespace));
        }

        try {
            trinoFileSystem.createDirectory(nsLocation);

        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Create namespace failed: %s", namespace), e);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        throw new TrinoException(NOT_SUPPORTED, "getNamespacePrincipal is not supported by " + catalogName);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setNamespacePrincipal is not supported by " + catalogName);
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameNamespace is not supported by " + catalogName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        Set<SchemaTableName> schemaTableNames = Sets.newHashSet();
        try {
            for (String ns : listNamespaces(session, namespace)) {
                Location nsLocation = Location.of(SLASH.join(warehouse, namespace));
                if (!isDirectory(nsLocation)) {
                    throw new TrinoException(SCHEMA_NOT_FOUND, "could not find namespace");
                }
                schemaTableNames.addAll(trinoFileSystem.listDirectories(nsLocation).stream()
                    .filter(this::isTableDir).map(tableLocation -> new SchemaTableName(ns, tableLocation.fileName())).toList());
            }
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, String.format("Failed to list tables under: %s", namespace),e);
        }

        return Lists.newArrayList(schemaTableNames);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> namespace)
    {
        // views and materialized views are currently not supported
        verify(listViews(session, namespace).isEmpty(), "Unexpected views support");
        verify(listMaterializedViews(session, namespace).isEmpty(), "Unexpected views support");
        return listTables(session, namespace).stream()
                .collect(toImmutableMap(identity(), ignore -> RelationType.TABLE));
    }

    @Override
    public Optional<Iterator<RelationColumnsMetadata>> streamRelationColumns(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Iterator<RelationCommentMetadata>> streamRelationComments(ConnectorSession session, Optional<String> namespace, UnaryOperator<Set<SchemaTableName>> relationFilter, Predicate<SchemaTableName> isRedirected)
    {
        return Optional.empty();
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return newCreateTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties, Optional<String> owner)
    {
        // Location cannot be specified for hadoop tables.
        return this.catalog.newCreateTableTransaction(
                toTableId(schemaTableName),
                schema,
                partitionSpec,
                null,
                properties);
    }

    @Override
    public Transaction newCreateOrReplaceTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return newCreateOrReplaceTableTransaction(
                session,
                schemaTableName,
                schema,
                partitionSpec,
                sortOrder,
                location,
                properties,
                Optional.of(session.getUser()));
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, TableMetadata tableMetadata)
    {
        this.catalog.registerTable(TableIdentifier.of(tableName.getSchemaName(), tableName.getTableName()), tableMetadata.metadataFileLocation());
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        dropTable(session, tableName);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        this.catalog.dropTable(toTableId(schemaTableName), true);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        this.catalog.dropTable(toTableId(schemaTableName), true);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        this.catalog.renameTable(toTableId(from), toTableId(to));
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return this.catalog.loadTable(toTableId(schemaTableName));
        }
        catch (NoSuchTableException e) {
            // Have to change exception types due to code relying on specific exception to be thrown.
            throw new TableNotFoundException(schemaTableName, e);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> tryGetColumnMetadata(ConnectorSession session, List<SchemaTableName> tables)
    {
        return null;
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = toTableId(schemaTableName);
        String dbLocationUri = PropertyUtil.propertyAsString(getNamespaceCatalog().loadNamespaceMetadata(tableIdentifier.namespace()), "locationUri", null);

        if (dbLocationUri != null) {
            return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
        }
        else {
            return String.format("%s/%s/%s", warehouse, schemaFromTableId(tableIdentifier), tableIdentifier.name());
        }
    }

    @Override
    protected void invalidateTableCache(SchemaTableName schemaTableName)
    {
        tableMetadataCache.invalidate(schemaTableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setTablePrincipal is not supported by " + catalogName);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new TrinoException(NOT_SUPPORTED, "createView is not supported by " + catalogName);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameView is not supported by " + catalogName);
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setViewPrincipal is not supported by " + catalogName);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropView is not supported by " + catalogName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return new ArrayList<>();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return new ArrayList<>();
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewComment is not supported by " + catalogName);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateViewColumnComment is not supported by " + catalogName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties,
            boolean replace,
            boolean ignoreExisting)
    {
        throw new TrinoException(NOT_SUPPORTED, "createMaterializedView is not supported by " + catalogName);
    }

    @Override
    public void updateMaterializedViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "updateMaterializedViewColumnComment is not supported by " + catalogName);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropMaterializedView is not supported by " + catalogName);
    }

    @Override
    public Optional<BaseTable> getMaterializedViewStorageTable(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.empty();
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        throw new TrinoException(NOT_SUPPORTED, "renameMaterializedView is not supported by " + catalogName);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName, String catalogName)
    {
        return Optional.empty();
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return Optional.empty();
    }

    public CatalogName getCatalogName() {
        return catalogName;
    }

    private boolean isDirectory(Location location)
    {
        try {
            Optional<Boolean> directoryExists = trinoFileSystem.directoryExists(location);

            if (directoryExists.isPresent()) {
                return directoryExists.get();
            }
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isDirectory(Location) could encountered an error ",e);
        }
        return false;
    }
    private boolean isTableDir(Location location)
    {
        try {
            Optional<Boolean> directoryExists = trinoFileSystem.directoryExists(location);
            if (directoryExists.isPresent()) {
                if (!directoryExists.get() || !location.path().contains("metadata")) {
                    return false;
                }
            }
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isTableDir(Location) could not determine whether directory exists",e);
        }

        try {
            FileIterator files = trinoFileSystem.listFiles(location);
            while (files.hasNext()) {
                if (files.next().toString().endsWith(".metadata.json")) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "isTableDir(Location) could not list files",e);
        }

        return false;
    }

    private boolean isNamespace(Location location)
    {
        return isDirectory(location) && !isTableDir(location);
    }
}
