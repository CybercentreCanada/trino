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
package io.trino.plugin.snowflake;

import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public final class TestingSnowflakeServer
{
    public static final String TEST_URL = requireNonNull(System.getProperty("snowflake.test.server.url"), "snowflake.test.server.url is not set");
    public static final String TEST_USER = requireNonNull(System.getProperty("snowflake.test.server.user"), "snowflake.test.server.user is not set");
    public static final String TEST_PASSWORD = requireNonNull(System.getProperty("snowflake.test.server.password"), "snowflake.test.server.password is not set");
    public static final String TEST_DATABASE = requireNonNull(System.getProperty("snowflake.test.server.database"), "snowflake.test.server.database is not set");
    public static final String TEST_WAREHOUSE = requireNonNull(System.getProperty("snowflake.test.server.warehouse"), "snowflake.test.server.warehouse is not set");
    public static final String TEST_ROLE = requireNonNull(System.getProperty("snowflake.test.server.role"), "snowflake.test.server.role is not set");
    public static final String TEST_SCHEMA = "tpch";

    private TestingSnowflakeServer() {}

    public static void execute(@Language("SQL") String sql)
    {
        execute(TEST_URL, getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", TEST_USER);
        properties.setProperty("password", TEST_PASSWORD);
        properties.setProperty("db", TEST_DATABASE);
        properties.setProperty("schema", TEST_SCHEMA);
        properties.setProperty("warehouse", TEST_WAREHOUSE);
        properties.setProperty("role", TEST_ROLE);
        return properties;
    }
}
