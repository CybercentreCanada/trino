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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyStackedNot;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyStackedNot
{
    @Test
    void test()
    {
        assertThat(optimize(
                not(PLANNER_CONTEXT.getMetadata(), not(PLANNER_CONTEXT.getMetadata(), new Reference(BOOLEAN, "a")))))
                .isEqualTo(Optional.of(new Reference(BOOLEAN, "a")));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyStackedNot().apply(expression, testSession(), ImmutableMap.of());
    }
}
