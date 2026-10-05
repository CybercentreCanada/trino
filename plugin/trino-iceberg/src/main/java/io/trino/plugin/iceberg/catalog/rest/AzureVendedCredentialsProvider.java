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
import io.airlift.log.Logger;
import org.apache.iceberg.rest.credentials.Credential;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iceberg.azure.AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.azure.AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_PREFIX;

final class AzureVendedCredentialsProvider
        extends AbstractIcebergRestVendedCredentialsProvider<AzureVendedCredentials>
{
    private static final Logger log = Logger.get(AzureVendedCredentialsProvider.class);

    AzureVendedCredentialsProvider(Map<String, String> catalogProperties, Map<String, String> fileIoProperties)
    {
        super(catalogProperties,
                fileIoProperties,
                parseBoolean(fileIoProperties, ADLS_REFRESH_CREDENTIALS_ENABLED, true),
                Optional.ofNullable(fileIoProperties.get(ADLS_REFRESH_CREDENTIALS_ENDPOINT)),
                createVendedCredentials(fileIoProperties));
        log.debug(
                "Azure vended credentials: refreshEnabled=%s, endpointPresent=%s, expiryPresent=%s",
                parseBoolean(fileIoProperties, ADLS_REFRESH_CREDENTIALS_ENABLED, true),
                fileIoProperties.containsKey(ADLS_REFRESH_CREDENTIALS_ENDPOINT),
                fileIoProperties.keySet().stream()
                        .anyMatch(key -> key.startsWith(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX)));
    }

    @Override
    protected AzureVendedCredentials applyRefreshedCredentials(List<Credential> credentials)
    {
        return parseAzureVendedCredentials(credentials.stream()
                .flatMap(credential -> credential.config().entrySet().stream())
                .collect(toImmutableList()));
    }

    public static AzureVendedCredentials createVendedCredentials(Map<String, String> fileIoProperties)
    {
        return parseAzureVendedCredentials(fileIoProperties.entrySet());
    }

    private static AzureVendedCredentials parseAzureVendedCredentials(Iterable<Entry<String, String>> properties)
    {
        ImmutableMap.Builder<String, String> sasTokensBuilder = ImmutableMap.builder();
        Instant earliest = null;
        for (Entry<String, String> entry : properties) {
            if (entry.getKey().startsWith(ADLS_SAS_TOKEN_PREFIX)) {
                String account = entry.getKey().substring(ADLS_SAS_TOKEN_PREFIX.length());
                sasTokensBuilder.put(account, entry.getValue());
            }
            if (entry.getKey().startsWith(ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX)) {
                Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(entry.getValue()));
                if (earliest == null || expiresAt.isBefore(earliest)) {
                    earliest = expiresAt;
                }
            }
        }
        Optional<Instant> expirationTime = Optional.ofNullable(earliest);
        log.debug("Azure vended SAS token expires at: %s", expirationTime.map(Instant::toString).orElse("not provided"));
        return new AzureVendedCredentials(sasTokensBuilder.buildOrThrow(), Optional.ofNullable(earliest));
    }
}
