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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.util.Objects;

public final class MemoryColumnHandle
        implements ColumnHandle
{
    private final int columnIndex;
    private final TypeSignature typeSignature;
    private transient Type typeCache;

    @JsonCreator
    public MemoryColumnHandle(@JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("typeSignature") TypeSignature typeSignature)
    {
        this.columnIndex = columnIndex;
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    public Type getType(TypeManager typeManager)
    {
        if (typeCache == null) {
            typeCache = typeManager.getType(getTypeSignature());
        }
        return typeCache;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIndex);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MemoryColumnHandle other = (MemoryColumnHandle) obj;
        return Objects.equals(this.columnIndex, other.columnIndex);
    }

    @Override
    public String toString()
    {
        return Integer.toString(columnIndex);
    }
}
