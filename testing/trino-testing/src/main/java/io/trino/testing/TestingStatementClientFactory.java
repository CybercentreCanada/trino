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
package io.trino.testing;

import io.trino.Session;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import okhttp3.OkHttpClient;

import java.util.Optional;

import static io.trino.client.StatementClientFactory.newStatementClient;

public interface TestingStatementClientFactory
{
    TestingStatementClientFactory DEFAULT_STATEMENT_FACTORY = new TestingStatementClientFactory() {};

    default StatementClient create(OkHttpClient httpClient, Session session, ClientSession clientSession, String query)
    {
        return newStatementClient(httpClient, clientSession, query, Optional.of(session.getClientCapabilities()));
    }
}
