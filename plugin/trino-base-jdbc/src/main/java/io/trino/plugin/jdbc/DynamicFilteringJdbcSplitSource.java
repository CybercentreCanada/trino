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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Attaches dynamic filter to {@link JdbcSplit} after {@link JdbcDynamicFilteringSplitManager}
 * has waited for the collection of dynamic filters.
 * This allows JDBC based connectors to avoid waiting for dynamic filters again on the worker node
 * in {@link JdbcRecordSetProvider}. The number of splits generated by JDBC based connectors are
 * typically small, therefore attaching dynamic filter here does not add significant overhead.
 * Waiting for dynamic filters in {@link JdbcDynamicFilteringSplitManager} is preferred over waiting
 * for them on the worker node in {@link JdbcRecordSetProvider} to allow connectors to take advantage of
 * dynamic filters during the splits generation phase.
 */
public class DynamicFilteringJdbcSplitSource
        implements ConnectorSplitSource
{
    private final ConnectorSplitSource delegateSplitSource;
    private final DynamicFilter dynamicFilter;
    private final JdbcTableHandle tableHandle;

    DynamicFilteringJdbcSplitSource(ConnectorSplitSource delegateSplitSource, DynamicFilter dynamicFilter, JdbcTableHandle tableHandle)
    {
        this.delegateSplitSource = requireNonNull(delegateSplitSource, "delegateSplitSource is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (!isEligibleForDynamicFilter(tableHandle)) {
            return delegateSplitSource.getNextBatch(maxSize);
        }
        return delegateSplitSource.getNextBatch(maxSize)
                .thenApply(batch -> {
                    TupleDomain<JdbcColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                            .transformKeys(JdbcColumnHandle.class::cast);
                    return new ConnectorSplitBatch(
                            batch.getSplits().stream()
                                    // attach dynamic filter constraint to JdbcSplit
                                    .map(split -> {
                                        JdbcSplit jdbcSplit = (JdbcSplit) split;
                                        // If split was a subclass of JdbcSplit, there would be additional information
                                        // that we would need to pass further on.
                                        verify(jdbcSplit.getClass() == JdbcSplit.class, "Unexpected split type %s", jdbcSplit);
                                        return jdbcSplit.withDynamicFilter(dynamicFilterPredicate);
                                    })
                                    .collect(toImmutableList()),
                            batch.isNoMoreSplits());
                });
    }

    @Override
    public void close()
    {
        delegateSplitSource.close();
    }

    @Override
    public boolean isFinished()
    {
        return delegateSplitSource.isFinished();
    }

    public static boolean isEligibleForDynamicFilter(JdbcTableHandle tableHandle)
    {
        // don't pushdown predicate through limit as it could reduce performance
        return tableHandle.getLimit().isEmpty();
    }
}
