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
package io.trino.plugin.queryeventlistener.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.eventlistener.QueryCreatedEvent;

public class CreatedEvent
{
    private static final String KEY_EVENT_TYPE = "eventType";
    private static final String KEY_QUERY_ID = "queryId";
    private static final String KEY_CREATE_TIME = "createTime";
    private static final String KEY_USER = "user";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_CATALOG = "catalog";
    private static final String KEY_SQL = "sql";
    private static final String KEY_USER_AGENT = "userAgent";

    @JsonProperty(KEY_EVENT_TYPE)
    private final String eventType = "QueryCreate";

    @JsonProperty(KEY_QUERY_ID)
    private final String queryId;

    @JsonProperty(KEY_CREATE_TIME)
    private final String createTime;

    @JsonProperty(KEY_USER)
    private final String user;

    @JsonProperty(KEY_SCHEMA)
    private final String schema;

    @JsonProperty(KEY_CATALOG)
    private final String catalog;

    @JsonProperty(KEY_SQL)
    private final String sql;

    @JsonProperty(KEY_USER_AGENT)
    private final String userAgent;

    public CreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
        this.queryId = queryCreatedEvent.getMetadata().getQueryId();
        this.createTime = queryCreatedEvent.getCreateTime().toString();
        this.user = queryCreatedEvent.getContext().getUser();
        this.schema = queryCreatedEvent.getContext().getSchema().orElse(null);
        this.catalog = queryCreatedEvent.getContext().getCatalog().orElse(null);
        this.sql = queryCreatedEvent.getMetadata().getQuery();
        this.userAgent = queryCreatedEvent.getContext().getUserAgent().orElse(null);
    }

    @JsonCreator
    private CreatedEvent(
            @JsonProperty(KEY_EVENT_TYPE) String eventType,
            @JsonProperty(KEY_QUERY_ID) String queryId,
            @JsonProperty(KEY_CREATE_TIME) String createTime,
            @JsonProperty(KEY_USER) String user,
            @JsonProperty(KEY_SCHEMA) String schema,
            @JsonProperty(KEY_CATALOG) String catalog,
            @JsonProperty(KEY_SQL) String sql,
            @JsonProperty(KEY_USER_AGENT) String userAgent)
    {
        this.queryId = queryId;
        this.createTime = createTime;
        this.user = user;
        this.schema = schema;
        this.catalog = catalog;
        this.sql = sql;
        this.userAgent = userAgent;
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getCreateTime()
    {
        return createTime;
    }

    public String getUser()
    {
        return user;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSql()
    {
        return sql;
    }

    public String getUserAgent()
    {
        return userAgent;
    }

    @Override
    public String toString()
    {
        return "CreateEventJson{" +
                "eventType='" + eventType + '\'' +
                ", queryId='" + queryId + '\'' +
                ", createTime='" + createTime + '\'' +
                ", user='" + user + '\'' +
                ", schema='" + schema + '\'' +
                ", catalog='" + catalog + '\'' +
                ", sql='" + sql + '\'' +
                ", userAgent='" + userAgent + '\'' +
                '}';
    }
}
