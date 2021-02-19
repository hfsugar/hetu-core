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
package io.prestosql.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.window.WindowFunctionSupplier;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.MapBlockEncoding;
import io.prestosql.spi.block.RowBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleMapBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorCapabilities;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SubQueryApplicationResult;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.sql.SqlQueryWriter;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.type.InternalTypeManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.type.DoubleType.DOUBLE;

public abstract class AbstractMockMetadata
        implements Metadata
{
    public static Metadata dummyMetadata()
    {
        return new AbstractMockMetadata() {};
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Hetu supports pushing sub-query with join down to the connector.
     * This method decides if the sub-query can be pushed down to the connector based on the connector.
     *
     * @param session Presto session
     * @param tableHandle a table used in the sub-query (if the sub query has more than one tables, use a random table from the sub-query)
     * @param subQuery the actual sub-query to be pushed down
     * @param types Presto types of intermediate symbols
     * @return optional SubQueryApplicationResult which has the new TableHandle if the connector supports this feature
     */
    @Override
    public Optional<SubQueryApplicationResult<TableHandle>> applySubQuery(Session session, TableHandle tableHandle, String subQuery, Map<String, Type> types)
    {
        return Optional.empty();
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> getTableHandleForStatisticsCollection(Session session, QualifiedObjectName tableName, Map<String, Object> analyzeProperties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableLayoutResult> getLayout(Session session, TableHandle tableHandle, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, Constraint constraint)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, QualifiedObjectName newTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<NewTableLayout> getInsertLayout(Session session, TableHandle target)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupQuery(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, boolean isOverwrite)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsMetadataDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CatalogName> getCatalogHandle(Session session, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, CatalogName> getCatalogNames(Session session)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<QualifiedObjectName, ConnectorViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean usesLegacyTableLayouts(Session session, TableHandle table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        return Optional.empty();
    }

    @Override
    public Optional<TableHandle> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        return Optional.empty();
    }

    //
    // Roles and Grants
    //

    @Override
    public void createRole(Session session, String role, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(Session session, String role, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, PrestoPrincipal principal, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, String catalog, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }

    //
    // Types
    //

    @Override
    public Type getType(TypeSignature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void verifyComparableOrderableContract()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Type> getTypes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        throw new UnsupportedOperationException();
    }

    //
    // Functions
    //

    @Override
    public void addFunctions(List<? extends SqlFunction> functions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInvokerProvider getFunctionInvokerProvider()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Signature resolveFunction(QualifiedName name, List<TypeSignatureProvider> parameterTypes)
    {
        String nameSuffix = name.getSuffix();
        if (nameSuffix.equals("rand") && parameterTypes.isEmpty()) {
            return new Signature(nameSuffix, FunctionKind.SCALAR, DOUBLE.getTypeSignature(), ImmutableList.of());
        }
        throw new PrestoException(FUNCTION_NOT_FOUND, name + "(" + Joiner.on(", ").join(parameterTypes) + ")");
    }

    @Override
    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcedureRegistry getProcedureRegistry()
    {
        throw new UnsupportedOperationException();
    }

    //
    // Blocks
    //

    @Override
    public BlockEncoding getBlockEncoding(String encodingName)
    {
        if (encodingName.equals(VariableWidthBlockEncoding.NAME)) {
            return new VariableWidthBlockEncoding();
        }
        if (encodingName.equals(ByteArrayBlockEncoding.NAME)) {
            return new ByteArrayBlockEncoding();
        }
        if (encodingName.equals(ShortArrayBlockEncoding.NAME)) {
            return new ShortArrayBlockEncoding();
        }
        if (encodingName.equals(IntArrayBlockEncoding.NAME)) {
            return new IntArrayBlockEncoding();
        }
        if (encodingName.equals(LongArrayBlockEncoding.NAME)) {
            return new LongArrayBlockEncoding();
        }
        if (encodingName.equals(Int128ArrayBlockEncoding.NAME)) {
            return new Int128ArrayBlockEncoding();
        }
        if (encodingName.equals(DictionaryBlockEncoding.NAME)) {
            return new DictionaryBlockEncoding();
        }
        if (encodingName.equals(MapBlockEncoding.NAME)) {
            return new MapBlockEncoding(new InternalTypeManager(this));
        }
        if (encodingName.equals(SingleMapBlockEncoding.NAME)) {
            return new SingleMapBlockEncoding(new InternalTypeManager(this));
        }
        if (encodingName.equals(RowBlockEncoding.NAME)) {
            return new RowBlockEncoding();
        }
        if (encodingName.equals(SingleRowBlockEncoding.NAME)) {
            return new SingleRowBlockEncoding();
        }
        if (encodingName.equals(RunLengthBlockEncoding.NAME)) {
            return new RunLengthBlockEncoding();
        }
        if (encodingName.equals(ArrayBlockEncoding.NAME)) {
            return new ArrayBlockEncoding();
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        return new InternalBlockEncodingSerde(this);
    }

    //
    // Properties
    //

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaPropertyManager getSchemaPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnPropertyManager getColumnPropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AnalyzePropertyManager getAnalyzePropertyManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    /**
     * Hetu's sub-query push down expects supporting connectors to provide a {@link SqlQueryWriter}
     * to write SQL queries for the respective databases.
     *
     * @return the optional SQL query writer which can write database specific SQL queries
     */
    @Override
    public Optional<SqlQueryWriter> getSqlQueryWriter(Session session, TableHandle tableHandle)
    {
        return Optional.empty();
    }

    /**
     * Hetu can only cache execution plans for supported connectors.
     * This method overrides {@link ConnectorMetadata} returns true to indicate
     * execution plan caching is enabled for testing connectors.
     *
     * @param session Presto session
     * @param handle Connector specific table handle
     */
    @Override
    public boolean isExecutionPlanCacheSupported(Session session, TableHandle handle)
    {
        return true;
    }

    /**
     * Hetu can only create index for supported connectors.
     *
     * @param session Presto session
     * @param tableName Connector specific tableName
     */
    public boolean isHeuristicIndexSupported(Session session, QualifiedObjectName tableName)
    {
        return true;
    }
}
