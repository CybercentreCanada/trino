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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Supplies Iceberg REST auth properties for Dremio's OAuth2 AuthManager.
 */
public class DremioSecurityProperties
        implements SecurityProperties
{
    private static final String AUTH_TYPE = "rest.auth.type";

    private static final String OAUTH2_PREFIX = "rest.auth.oauth2.";
    private static final String ISSUER_URL = OAUTH2_PREFIX + "issuer-url";
    private static final String TOKEN_ENDPOINT = OAUTH2_PREFIX + "token-endpoint";
    private static final String GRANT_TYPE = OAUTH2_PREFIX + "grant-type";
    private static final String CLIENT_AUTH = OAUTH2_PREFIX + "client-auth";
    private static final String CLIENT_ID = OAUTH2_PREFIX + "client-id";
    private static final String CLIENT_SECRET = OAUTH2_PREFIX + "client-secret";
    private static final String SCOPE = OAUTH2_PREFIX + "scope";
    private static final String TOKEN = OAUTH2_PREFIX + "token";
    private static final String TOKEN_REFRESH_ENABLED = OAUTH2_PREFIX + "token-refresh.enabled";
    private static final String HTTP_CLIENT_TYPE = OAUTH2_PREFIX + "http.client-type";
    private static final String SYSTEM_SESSION_CACHE_TIMEOUT = OAUTH2_PREFIX + "system.session-cache-timeout";
    private static final String SMALLRYE_CONFIG_LOCATIONS = "smallrye.config.locations";

    private final Map<String, String> properties;

    @Inject
    public DremioSecurityProperties(DremioSecurityConfig config)
    {
        requireNonNull(config, "config is null");

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.put(AUTH_TYPE, "com.dremio.iceberg.authmgr.oauth2.OAuth2Manager");
        config.getToken().ifPresent(token -> builder.put(TOKEN, token));
        config.getScope().ifPresent(scope -> builder.put(SCOPE, scope));
        config.getIssuerUrl().ifPresent(uri -> builder.put(ISSUER_URL, uri.toString()));
        config.getTokenEndpoint().ifPresent(uri -> builder.put(TOKEN_ENDPOINT, uri.toString()));
        builder.put(TOKEN_REFRESH_ENABLED, String.valueOf(config.isTokenRefreshEnabled()));
        builder.put(GRANT_TYPE, config.getGrantType());
        builder.put(CLIENT_AUTH, config.getClientAuthMethod());
        config.getClientId().ifPresent(value -> builder.put(CLIENT_ID, value));
        config.getClientSecret().ifPresent(value -> builder.put(CLIENT_SECRET, value));
        config.getHttpClientType().ifPresent(value -> builder.put(HTTP_CLIENT_TYPE, value));
        config.getSessionCacheTimeout().ifPresent(value -> builder.put(SYSTEM_SESSION_CACHE_TIMEOUT, value));
        config.getSmallRyeConfigLocations().ifPresent(value -> builder.put(SMALLRYE_CONFIG_LOCATIONS, value));

        this.properties = builder.buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
