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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public abstract class PatternQuantifier
        extends Node
{
    private final boolean greedy;

    protected PatternQuantifier(NodeLocation location, boolean greedy)
    {
        super(location);
        this.greedy = greedy;
    }

    public boolean isGreedy()
    {
        return greedy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternQuantifier(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PatternQuantifier o = (PatternQuantifier) obj;
        return greedy == o.greedy;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(greedy);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        PatternQuantifier otherNode = (PatternQuantifier) other;
        return greedy == otherNode.greedy;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("greedy", greedy)
                .toString();
    }
}
