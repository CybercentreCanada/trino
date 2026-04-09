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
package io.trino.spi.security;

import io.trino.spi.QueryId;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SystemSecurityContext
{
    private final Identity identity;
    private final QueryId queryId;
    private final Instant queryStart;
    private final Optional<String> source;

    public SystemSecurityContext(Identity identity, QueryId queryId, Instant queryStart)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryStart = requireNonNull(queryStart, "queryStart is null");
        this.source = Optional.empty();
    }

    public SystemSecurityContext(Identity identity, QueryId queryId, Instant queryStart, Optional<String> source)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryStart = requireNonNull(queryStart, "queryStart is null");
        this.source = requireNonNull(source, "source is null");
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Instant getQueryStart()
    {
        return queryStart;
    }

    public Optional<String> getSource()
    {
        return source;
    }
}
