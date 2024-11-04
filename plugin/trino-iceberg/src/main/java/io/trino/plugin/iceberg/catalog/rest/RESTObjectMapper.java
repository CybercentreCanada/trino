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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.iceberg.rest.RESTSerializers;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;

class RESTObjectMapper
{
    private static final JsonFactory FACTORY = jsonFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
    private static volatile boolean isInitialized;

    private RESTObjectMapper() {}

    static ObjectMapper mapper()
    {
        if (!isInitialized) {
            synchronized (RESTObjectMapper.class) {
                if (!isInitialized) {
                    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
                    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    MAPPER.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
                    RESTSerializers.registerAll(MAPPER);
                    isInitialized = true;
                }
            }
        }

        return MAPPER;
    }
}
