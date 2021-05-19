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
import io.trino.spi.eventlistener.QueryCompletedEvent;

public class CompletedEvent
{
    private static final String KEY_EVENT_TYPE = "eventType";
    private static final String KEY_QUERY_ID = "queryId";
    private static final String KEY_CREATE_TIME = "createTime";
    private static final String KEY_QUEUED_TIME = "queuedTime";
    private static final String KEY_WALL_TIME = "wallTime";
    private static final String KEY_CPU_TIME = "cpuTime";
    private static final String KEY_USER = "user";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_CATALOG = "catalog";
    private static final String KEY_RECORDS = "records";
    private static final String KEY_COMPLETED = "completed";
    private static final String KEY_SQL = "sql";
    private static final String KEY_USER_AGENT = "userAgent";

    @JsonProperty(KEY_EVENT_TYPE)
    private final String eventType = "QueryCompleted";

    @JsonProperty(KEY_QUERY_ID)
    private final String queryId;

    @JsonProperty(KEY_CREATE_TIME)
    private final String createTime;

    @JsonProperty(KEY_QUEUED_TIME)
    private final String queuedTime;

    @JsonProperty(KEY_WALL_TIME)
    private final String wallTime;

    @JsonProperty(KEY_CPU_TIME)
    private final String cpuTime;

    @JsonProperty(KEY_USER)
    private final String user;

    @JsonProperty(KEY_SCHEMA)
    private final String schema;

    @JsonProperty(KEY_CATALOG)
    private final String catalog;

    @JsonProperty(KEY_RECORDS)
    private final long records;

    @JsonProperty(KEY_COMPLETED)
    private final boolean completed;

    @JsonProperty(KEY_SQL)
    private final String sql;

    @JsonProperty(KEY_USER_AGENT)
    private final String userAgent;

    public CompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        this.queryId = queryCompletedEvent.getMetadata().getQueryId();
        this.createTime = queryCompletedEvent.getCreateTime().toString();
        this.queuedTime = queryCompletedEvent.getStatistics().getQueuedTime().toString();
        this.wallTime = queryCompletedEvent.getStatistics().getWallTime().toString();
        this.cpuTime = queryCompletedEvent.getStatistics().getCpuTime().toString();
        this.user = queryCompletedEvent.getContext().getUser();
        this.schema = queryCompletedEvent.getContext().getSchema().orElse(null);
        this.catalog = queryCompletedEvent.getContext().getCatalog().orElse(null);
        this.records = queryCompletedEvent.getStatistics().getTotalRows();
        this.completed = queryCompletedEvent.getStatistics().isComplete();
        this.sql = queryCompletedEvent.getMetadata().getQuery();
        this.userAgent = queryCompletedEvent.getContext().getUserAgent().orElse(null);
    }

    @JsonCreator
    private CompletedEvent(
            @JsonProperty(KEY_EVENT_TYPE) String eventType,
            @JsonProperty(KEY_QUERY_ID) String queryId,
            @JsonProperty(KEY_CREATE_TIME) String createTime,
            @JsonProperty(KEY_QUEUED_TIME) String queuedTime,
            @JsonProperty(KEY_WALL_TIME) String wallTime,
            @JsonProperty(KEY_CPU_TIME) String cpuTime,
            @JsonProperty(KEY_USER) String user,
            @JsonProperty(KEY_SCHEMA) String schema,
            @JsonProperty(KEY_CATALOG) String catalog,
            @JsonProperty(KEY_RECORDS) Long records,
            @JsonProperty(KEY_COMPLETED) Boolean completed,
            @JsonProperty(KEY_SQL) String sql,
            @JsonProperty(KEY_USER_AGENT) String userAgent)
    {
        this.queryId = queryId;
        this.createTime = createTime;
        this.queuedTime = queuedTime;
        this.wallTime = wallTime;
        this.cpuTime = cpuTime;
        this.user = user;
        this.schema = schema;
        this.catalog = catalog;
        this.records = (records == null) ? -1L : records;
        this.completed = completed != null && completed;
        this.sql = sql;
        this.userAgent = userAgent;
    }

    @JsonProperty("eventType")
    public String getEventType()
    {
        return eventType;
    }

    @JsonProperty("queryId")
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty("createTime")
    public String getCreateTime()
    {
        return createTime;
    }

    @JsonProperty("queuedTime")
    public String getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty("wallTime")
    public String getWallTime()
    {
        return wallTime;
    }

    @JsonProperty("cpuTime")
    public String getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty("user")
    public String getUser()
    {
        return user;
    }

    @JsonProperty("schema")
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty("catalog")
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty("records")
    public long getRecords()
    {
        return records;
    }

    @JsonProperty("completed")
    public boolean isCompleted()
    {
        return completed;
    }

    @JsonProperty("sql")
    public String getSql()
    {
        return sql;
    }

    @JsonProperty("userAgent")
    public String getUserAgent()
    {
        return userAgent;
    }

    @Override
    public String toString()
    {
        return "CompletedEventJson{" +
                "eventType='" + eventType + '\'' +
                ", queryId='" + queryId + '\'' +
                ", createTime='" + createTime + '\'' +
                ", queuedTime='" + queuedTime + '\'' +
                ", wallTime='" + wallTime + '\'' +
                ", cpuTime='" + cpuTime + '\'' +
                ", user='" + user + '\'' +
                ", schema='" + schema + '\'' +
                ", catalog='" + catalog + '\'' +
                ", records=" + records +
                ", completed=" + completed +
                ", sql='" + sql + '\'' +
                ", userAgent='" + userAgent + '\'' +
                '}';
    }
}
