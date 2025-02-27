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
import com.google.common.annotations.VisibleForTesting;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.memory.MemoryTableProperties.SPILL_COMPRESSION_DEFAULT_VALUE;
import static java.util.Objects.requireNonNull;

public final class MemoryWriteTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    private final long table;
    private final boolean compressionEnabled;
    private final Set<Long> activeTableIds;
    private final List<ColumnInfo> columns;
    private final List<SortingColumn> sortedBy;
    private final List<String> indexColumns;
    private final String schemaName;
    private final String tableName;
    private final int splitsPerNode;

    @JsonCreator
    public MemoryWriteTableHandle(
            @JsonProperty("table") long table,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("compressEnabled") boolean compressionEnabled,
            @JsonProperty("activeTableIds") Set<Long> activeTableIds,
            @JsonProperty("columns") List<ColumnInfo> columns,
            @JsonProperty("sortedBy") List<SortingColumn> sortedBy,
            @JsonProperty("indexColumns") List<String> indexColumns,
            @JsonProperty("splitsPerNode") int splitsPerNode)
    {
        this.table = table;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.compressionEnabled = compressionEnabled;
        this.splitsPerNode = splitsPerNode;
        this.activeTableIds = requireNonNull(activeTableIds, "activeTableIds is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.sortedBy = requireNonNull(sortedBy, "sortedBy is null");
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
    }

    @VisibleForTesting
    MemoryWriteTableHandle(long table, Set<Long> activeTableIds)
    {
        this(table, "", "", SPILL_COMPRESSION_DEFAULT_VALUE, activeTableIds, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), 1);
    }

    @JsonProperty
    public long getTable()
    {
        return table;
    }

    @JsonProperty
    public boolean isCompressionEnabled()
    {
        return compressionEnabled;
    }

    @JsonProperty
    public Set<Long> getActiveTableIds()
    {
        return activeTableIds;
    }

    @JsonProperty
    public List<SortingColumn> getSortedBy()
    {
        return sortedBy;
    }

    @JsonProperty
    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getIndexColumns()
    {
        return indexColumns;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("activeTableIds", activeTableIds)
                .add("sortedBy", sortedBy)
                .add("indexColumns", indexColumns)
                .toString();
    }
}
