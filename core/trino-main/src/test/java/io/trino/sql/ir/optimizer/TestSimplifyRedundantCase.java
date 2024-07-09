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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.SimplifyRedundantCase;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IDENTICAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyRedundantCase
{
    @Test
    void test()
    {
        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "x"), TRUE)),
                        FALSE)))
                .isEqualTo(Optional.of(new Comparison(IDENTICAL, new Reference(BOOLEAN, "x"), TRUE)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "x"), FALSE)),
                        TRUE)))
                .isEqualTo(Optional.of(not(PLANNER_CONTEXT.getMetadata(), new Comparison(IDENTICAL, new Reference(BOOLEAN, "x"), TRUE))));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)), TRUE)),
                        FALSE)))
                .isEqualTo(Optional.of(new Comparison(IDENTICAL, new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)), TRUE)));

        assertThat(optimize(
                new Case(
                        ImmutableList.of(new WhenClause(new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)), FALSE)),
                        TRUE)))
                .isEqualTo(Optional.of(not(PLANNER_CONTEXT.getMetadata(), new Comparison(IDENTICAL, new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)), TRUE))));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyRedundantCase(PLANNER_CONTEXT).apply(expression, testSession(), ImmutableMap.of());
    }
}
