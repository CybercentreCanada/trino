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
package io.trino.filesystem.azure;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import java.util.logging.Level;
import java.util.logging.Logger;

public class AzureFileSystemModule
        extends AbstractConfigurationAwareModule
{
    private static final Logger okhttpLogger = Logger.getLogger("okhttp3.OkHttpClient");

    @Override
    protected void setup(Binder binder)
    {
        // Set OkHttpClient logging level to FINE for detailed connection information
        okhttpLogger.setLevel(Level.FINE);

        binder.bind(AzureFileSystemFactory.class).in(Scopes.SINGLETON);
        Module module = switch (buildConfigObject(AzureFileSystemConfig.class).getAuthType()) {
            case ACCESS_KEY -> new AzureAuthAccessKeyModule();
            case OAUTH -> new AzureAuthOAuthModule();
            case DEFAULT -> new AzureAuthDefaultModule();
        };
        install(module);
    }
}
