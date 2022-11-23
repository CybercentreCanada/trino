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
package io.trino.plugin.kudu;

import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KuduPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final KuduClientSession clientSession;

    @Inject
    public KuduPageSinkProvider(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        requireNonNull(outputTableHandle, "outputTableHandle is null");
        checkArgument(outputTableHandle instanceof KuduOutputTableHandle, "outputTableHandle is not an instance of KuduOutputTableHandle");
        KuduOutputTableHandle handle = (KuduOutputTableHandle) outputTableHandle;

        return new KuduPageSink(session, clientSession, handle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        requireNonNull(insertTableHandle, "insertTableHandle is null");
        checkArgument(insertTableHandle instanceof KuduInsertTableHandle, "insertTableHandle is not an instance of KuduInsertTableHandle");
        KuduInsertTableHandle handle = (KuduInsertTableHandle) insertTableHandle;

        return new KuduPageSink(session, clientSession, handle);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle)
    {
        return new KuduPageSink(session, clientSession, (KuduMergeTableHandle) mergeHandle);
    }
}
