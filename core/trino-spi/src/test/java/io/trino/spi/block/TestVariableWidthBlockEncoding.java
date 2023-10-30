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
package io.trino.spi.block;

import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestVariableWidthBlockEncoding
        extends BaseBlockEncodingTest<String>
{
    @Override
    protected Type getType()
    {
        return VARCHAR;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, String value)
    {
        VARCHAR.writeString(blockBuilder, value);
    }

    @Override
    protected String randomValue(Random random)
    {
        char[] value = new char[random.nextInt(16)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @Test
    public void testUnicode()
    {
        roundTrip(
                "\u0000",
                "Ní hé lá na gaoithe lá na scolb",
                "لولا اختلاف النظر، لبارت السلع",
                "△△▿▿◁▷◁▷BA",
                "Something in ASCII, latin ÿ, some İ and I, geometry ▦ and finally an emoji \uD83D\uDE0D");
    }
}
