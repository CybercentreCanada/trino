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
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class DremioSecurityConfig
{
    private Optional<URI> issuerUrl = Optional.empty();               // rest.auth.oauth2.issuer-url
    private Optional<URI> tokenEndpoint = Optional.empty();           // rest.auth.oauth2.token-endpoint
    private String grantType = "client_credentials";                  // rest.auth.oauth2.grant-type
    private String clientAuthMethod = "client_secret_basic";          // rest.auth.oauth2.client-auth
    private Optional<String> clientId = Optional.empty();             // rest.auth.oauth2.client-id
    private Optional<String> clientSecret = Optional.empty();         // rest.auth.oauth2.client-secret
    private Optional<String> scope = Optional.empty();                // rest.auth.oauth2.scope
    private Optional<String> token = Optional.empty();                // rest.auth.oauth2.token
    private boolean tokenRefreshEnabled = true;                       // rest.auth.oauth2.token-refresh.enabled
    private Optional<String> httpClientType = Optional.empty();       // rest.auth.oauth2.http.client-type
    private Optional<String> sessionCacheTimeout = Optional.empty();  // rest.auth.oauth2.system.session-cache-timeout
    private Map<String, String> extraParams = Map.of();               // rest.auth.oauth2.extra-params.*
    private Optional<String> smallRyeConfigLocations = Optional.empty();

    @Config("iceberg.rest-catalog.oauth2.issuer-url")
    @ConfigDescription("OAuth2 issuer URL")
    public DremioSecurityConfig setIssuerUrl(URI issuerUrl)
    {
        this.issuerUrl = Optional.ofNullable(issuerUrl);
        return this;
    }

    public Optional<URI> getIssuerUrl()
    {
        return issuerUrl;
    }

    @Config("iceberg.rest-catalog.oauth2.token-endpoint")
    @ConfigDescription("OAuth2 token endpoint")
    public DremioSecurityConfig setTokenEndpoint(URI tokenEndpoint)
    {
        this.tokenEndpoint = Optional.ofNullable(tokenEndpoint);
        return this;
    }

    public Optional<URI> getTokenEndpoint()
    {
        return tokenEndpoint;
    }

    @Config("iceberg.rest-catalog.oauth2.grant-type")
    @ConfigDescription("OAuth2 grant type (e.g., client_credentials, authorization_code, device_code, password, urn:ietf:params:oauth:grant-type:token-exchange)")
    public DremioSecurityConfig setGrantType(String grantType)
    {
        this.grantType = grantType;
        return this;
    }

    public String getGrantType()
    {
        return grantType;
    }

    @Config("iceberg.rest-catalog.oauth2.client-auth")
    @ConfigDescription("OAuth2 client authentication method (none, client_secret_basic, client_secret_post, client_secret_jwt, private_key_jwt)")
    public DremioSecurityConfig setClientAuthMethod(String clientAuthMethod)
    {
        this.clientAuthMethod = clientAuthMethod;
        return this;
    }

    public String getClientAuthMethod()
    {
        return clientAuthMethod;
    }

    @Config("iceberg.rest-catalog.oauth2.client-id")
    @ConfigDescription("OAuth2 client ID")
    public DremioSecurityConfig setClientId(String clientId)
    {
        this.clientId = Optional.ofNullable(clientId);
        return this;
    }

    public Optional<String> getClientId()
    {
        return clientId;
    }

    @Config("iceberg.rest-catalog.oauth2.client-secret")
    @ConfigDescription("OAuth2 client secret")
    @ConfigSecuritySensitive
    public DremioSecurityConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = Optional.ofNullable(clientSecret);
        return this;
    }

    public Optional<String> getClientSecret()
    {
        return clientSecret;
    }

    @Config("iceberg.rest-catalog.oauth2.scope")
    @ConfigDescription("OAuth2 scope (space-separated)")
    public DremioSecurityConfig setScope(String scope)
    {
        this.scope = Optional.ofNullable(scope);
        return this;
    }

    public Optional<String> getScope()
    {
        return scope;
    }

    @Config("iceberg.rest-catalog.oauth2.token")
    @ConfigDescription("Pre-issued access token to use instead of fetching an initial token")
    @ConfigSecuritySensitive
    public DremioSecurityConfig setToken(String token)
    {
        this.token = Optional.ofNullable(token);
        return this;
    }

    public Optional<String> getToken()
    {
        return token;
    }

    @Config("iceberg.rest-catalog.oauth2.token-refresh-enabled")
    @ConfigDescription("Enable automatic token refresh")
    public DremioSecurityConfig setTokenRefreshEnabled(boolean tokenRefreshEnabled)
    {
        this.tokenRefreshEnabled = tokenRefreshEnabled;
        return this;
    }

    public boolean isTokenRefreshEnabled()
    {
        return tokenRefreshEnabled;
    }

    @Config("iceberg.rest-catalog.oauth2.http.client-type")
    @ConfigDescription("HTTP client type for OAuth2 (DEFAULT or APACHE)")
    public DremioSecurityConfig setHttpClientType(String httpClientType)
    {
        this.httpClientType = Optional.ofNullable(httpClientType);
        return this;
    }

    public Optional<String> getHttpClientType()
    {
        return httpClientType;
    }

    @Config("iceberg.rest-catalog.oauth2.system.session-cache-timeout")
    @ConfigDescription("Session cache idle timeout for the OAuth2 manager (ISO-8601 duration, e.g., PT30S, PT1H)")
    public DremioSecurityConfig setSessionCacheTimeout(String timeout)
    {
        this.sessionCacheTimeout = Optional.ofNullable(timeout);
        return this;
    }

    public Optional<String> getSessionCacheTimeout()
    {
        return sessionCacheTimeout;
    }

    @Config("iceberg.rest-catalog.oauth2.extra-params")
    @ConfigDescription("Additional OAuth2 token request parameters as a comma-separated list of key=value pairs")
    public DremioSecurityConfig setExtraParams(String extraParams)
    {
        this.extraParams = extraParams == null ? Map.of() : parseExtraParams(extraParams);
        return this;
    }

    public Map<String, String> getExtraParams()
    {
        return extraParams;
    }

    private static Map<String, String> parseExtraParams(String raw)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String entry : raw.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int idx = trimmed.indexOf('=');
            if (idx <= 0 || idx == trimmed.length() - 1) {
                // ignore malformed entries
                continue;
            }
            String key = trimmed.substring(0, idx).trim();
            String value = trimmed.substring(idx + 1).trim();
            if (!key.isEmpty() && !value.isEmpty()) {
                builder.put(key, value);
            }
        }
        return builder.buildOrThrow();
    }

    @Config("iceberg.rest-catalog.smallrye-config-locations")
    @ConfigDescription("Comma-separated list of SmallRye config locations for advanced OAuth2 settings")
    public DremioSecurityConfig setSmallRyeConfigLocations(String locations)
    {
        this.smallRyeConfigLocations = Optional.ofNullable(locations);
        return this;
    }

    public Optional<String> getSmallRyeConfigLocations()
    {
        return smallRyeConfigLocations;
    }
}
