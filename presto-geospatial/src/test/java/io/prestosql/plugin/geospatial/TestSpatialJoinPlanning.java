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
package io.prestosql.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuFileSystemClientPlugin;
import io.hetu.core.metastore.HetuMetastorePlugin;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.geospatial.KdbTree;
import io.prestosql.geospatial.KdbTreeUtils;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.plugin.memory.MemoryConnectorFactory;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.SystemSessionProperties.SPATIAL_PARTITIONING_TABLE_NAME;
import static io.prestosql.geospatial.KdbTree.Node.newLeaf;
import static io.prestosql.spi.StandardErrorCode.INVALID_SPATIAL_PARTITIONING;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.unnest;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSpatialJoinPlanning
        extends BasePlanTest
{
    private static final String KDB_TREE_JSON = KdbTreeUtils.toJson(new KdbTree(newLeaf(new Rectangle(0, 0, 10, 10), 0)));

    public TestSpatialJoinPlanning()
    {
        super(() -> createQueryRunner());
    }

    private static LocalQueryRunner createQueryRunner()
            throws IOException
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default")
                .build());
        queryRunner.installPlugin(new HetuFileSystemClientPlugin());
        queryRunner.installPlugin(new HetuMetastorePlugin());
        queryRunner.installPlugin(new GeoPlugin());
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        TempFolder folder = new TempFolder().create();
        Runtime.getRuntime().addShutdownHook(new Thread(folder::close));
        HashMap<String, String> metastoreConfig = new HashMap<>();
        metastoreConfig.put("hetu.metastore.type", "hetufilesystem");
        metastoreConfig.put("hetu.metastore.hetufilesystem.profile-name", "default");
        metastoreConfig.put("hetu.metastore.hetufilesystem.path", folder.newFolder("metastore").getAbsolutePath());
        queryRunner.loadMetastore(metastoreConfig);
        queryRunner.createCatalog("memory", new MemoryConnectorFactory(),
                ImmutableMap.of("memory.spill-path", folder.newFolder("memory-connector").getAbsolutePath(),
                        "memory.splits-per-node", "1"));

        queryRunner.execute(format("CREATE TABLE kdb_tree AS SELECT '%s' AS v", KDB_TREE_JSON));
        queryRunner.execute("CREATE TABLE points (lng, lat, name) AS (VALUES (2.1e0, 2.1e0, 'x'))");
        queryRunner.execute("CREATE TABLE polygons (wkt, name) AS (VALUES ('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 'a'))");
        return queryRunner;
    }

    @Test
    public void testSpatialJoinContains()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)"), "length", expression("length(name)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))"), "length_2", expression("length(name_2)")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_contains(st_geometryfromtext, st_point)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_point)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_geometryfromtext)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))))));
    }

    @Test
    public void testSpatialJoinWithin()
    {
        // broadcast
        assertPlan("SELECT points.name, polygons.name " +
                        "FROM points, polygons " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_within(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // Verify that projections generated by the ExtractSpatialJoins rule
        // get merged with other projections under the join
        assertPlan("SELECT * " +
                        "FROM (SELECT length(name), * FROM points), (SELECT length(name), * FROM polygons) " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialJoin("st_within(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)"), "length", expression("length(name)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))"), "length_2", expression("length(name_2)")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_2", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Within(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_within(st_geometryfromtext, st_point)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_point)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_geometryfromtext)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))))));
    }

    @Test
    public void testInvalidKdbTree()
    {
        // table doesn't exist
        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("non_existent_table"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Table not found: memory.default.non_existent_table");

        // empty table
        getQueryRunner().execute("CREATE TABLE empty_table AS SELECT 'a' AS v WHERE false");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("empty_table"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.empty_table, but got none");

        // invalid JSON
        getQueryRunner().execute("CREATE TABLE invalid_kdb_tree AS SELECT 'invalid-json' AS v");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("invalid_kdb_tree"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Invalid JSON string for KDB tree: .*");

        // more than one row
        getQueryRunner().execute(format("CREATE TABLE too_many_rows AS SELECT * FROM (VALUES '%s', '%s') AS t(v)", KDB_TREE_JSON, KDB_TREE_JSON));

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_rows"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected exactly one row for table memory.default.too_many_rows, but found 2 rows");

        // more than one column
        getQueryRunner().execute("CREATE TABLE too_many_columns AS SELECT '%s' as c1, 100 as c2");

        assertInvalidSpatialPartitioning(
                withSpatialPartitioning("too_many_columns"),
                "SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                "Expected single column for table memory.default.too_many_columns, but found 2 columns");
    }

    private void assertInvalidSpatialPartitioning(Session session, String sql, String expectedMessageRegExp)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        try {
            queryRunner.inTransaction(session, transactionSession -> {
                queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
                return null;
            });
            fail(format("Expected query to fail: %s", sql));
        }
        catch (PrestoException ex) {
            assertEquals(ex.getErrorCode(), INVALID_SPATIAL_PARTITIONING.toErrorCode());
            if (!nullToEmpty(ex.getMessage()).matches(expectedMessageRegExp)) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", ex.getMessage(), expectedMessageRegExp, sql), ex);
            }
        }
    }

    @Test
    public void testSpatialJoinIntersects()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                anyTree(
                        spatialJoin("st_intersects(geometry_a, geometry_b)",
                                project(ImmutableMap.of("geometry_a", expression("ST_GeometryFromText(cast(wkt_a as varchar))")),
                                        tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("geometry_b", expression("ST_GeometryFromText(cast(wkt_b as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name")))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                withSpatialPartitioning("default.kdb_tree"),
                anyTree(
                        spatialJoin("st_intersects(geometry_a, geometry_b)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), geometry_a)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("geometry_a", expression("ST_GeometryFromText(cast(wkt_a as varchar))")),
                                                                tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name")))))),
                                anyTree(
                                        project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), geometry_b)", KDB_TREE_JSON))),
                                                project(ImmutableMap.of("geometry_b", expression("ST_GeometryFromText(cast(wkt_b as varchar))")),
                                                        tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testDistanceQuery()
    {
        // broadcast
        assertPlan("SELECT b.name, a.name " +
                        "FROM (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS a (lng, lat, name), (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 3.1",
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius",
                                project(ImmutableMap.of("st_point_a", expression("ST_Point(cast(a_lng as double), cast(a_lat as double))")),
                                        anyTree(
                                                values(ImmutableMap.of("a_lng", 0, "a_lat", 1)))),
                                anyTree(
                                        project(ImmutableMap.of("st_point_b", expression("ST_Point(cast(b_lng as double), cast(b_lat as double))"), "radius", expression("3.1e0")),
                                                anyTree(
                                                        values(ImmutableMap.of("b_lng", 0, "b_lat", 1))))))));

        assertPlan("SELECT b.name, a.name " +
                        "FROM (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS a (lng, lat, name), (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 300 / (cos(radians(b.lat)) * 111321)",
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius",
                                project(ImmutableMap.of("st_point_a", expression("ST_Point(cast(a_lng as double), cast(a_lat as double))")),
                                        anyTree(
                                                values(ImmutableMap.of("a_lng", 0, "a_lat", 1)))),
                                anyTree(
                                        project(ImmutableMap.of("st_point_b", expression("ST_Point(cast(b_lng as double), cast(b_lat as double))"), "radius", expression("3e2 / (cos(radians(cast(b_lat as double))) * 111.321e3)")),
                                                anyTree(
                                                        values(ImmutableMap.of("b_lng", 0, "b_lat", 1))))))));

        // distributed
        assertDistributedPlan("SELECT b.name, a.name " +
                        "FROM (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS a (lng, lat, name), (VALUES (2.1, 2.1, 'x'), (3.1, 4.1, 'y')) AS b (lng, lat, name) " +
                        "WHERE ST_Distance(ST_Point(a.lng, a.lat), ST_Point(b.lng, b.lat)) <= 3.1",
                withSpatialPartitioning("memory.default.kdb_tree"),
                anyTree(
                        spatialJoin("st_distance(st_point_a, st_point_b) <= radius", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_point_a)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("st_point_a", expression("ST_Point(cast(a_lng as double), cast(a_lat as double))")),
                                                                anyTree(
                                                                        values(ImmutableMap.of("a_lng", 0, "a_lat", 1))))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("partitions", expression(format("spatial_partitions(cast('%s' as kdbtree), st_point_b, 3.1e0)", KDB_TREE_JSON)), "radius", expression("3.1e0")),
                                                        project(ImmutableMap.of("st_point_b", expression("ST_Point(cast(b_lng as double), cast(b_lat as double))")),
                                                                anyTree(
                                                                        values(ImmutableMap.of("b_lng", 0, "b_lat", 1))))))))));
    }

    @Test
    public void testNotContains()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE NOT ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        filter("NOT ST_Contains(ST_GeometryFromText(cast(wkt as varchar)), ST_Point(lng, lat))",
                                join(JoinNode.Type.INNER, emptyList(),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name")),
                                        anyTree(
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));
    }

    @Test
    public void testNotIntersects()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM (VALUES (IF(rand() >= 0, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'), 'a')) AS a (wkt, name), (VALUES (IF(rand() >= 0, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'), 'a')) AS b (wkt, name) " +
                        "WHERE NOT ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                anyTree(
                        filter("NOT ST_Intersects(ST_GeometryFromText(cast(wkt_a as varchar)), ST_GeometryFromText(cast(wkt_b as varchar)))",
                                join(JoinNode.Type.INNER, emptyList(),
                                        anyTree(
                                                values(ImmutableMap.of("wkt_a", 0, "name_a", 1))),
                                        values(ImmutableMap.of("wkt_b", 0, "name_b", 1))))));
    }

    @Test
    public void testContainsWithEquiClause()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a, polygons b " +
                        "WHERE a.name = b.name AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        join(JoinNode.Type.INNER, ImmutableList.of(equiJoinClause("name_a", "name_b")),
                                Optional.of("ST_Contains(ST_GeometryFromText(cast(wkt as varchar)), ST_Point(lng, lat))"),
                                anyTree(
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name"))))));
    }

    @Test
    public void testIntersectsWithEquiClause()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM polygons a, polygons b " +
                        "WHERE a.name = b.name AND ST_Intersects(ST_GeometryFromText(a.wkt), ST_GeometryFromText(b.wkt))",
                anyTree(
                        join(JoinNode.Type.INNER, ImmutableList.of(equiJoinClause("name_a", "name_b")),
                                Optional.of("ST_Intersects(ST_GeometryFromText(cast(wkt_a as varchar)), ST_GeometryFromText(cast(wkt_B as varchar)))"),
                                anyTree(
                                        tableScan("polygons", ImmutableMap.of("wkt_a", "wkt", "name_a", "name"))),
                                anyTree(
                                        tableScan("polygons", ImmutableMap.of("wkt_b", "wkt", "name_b", "name"))))));
    }

    @Test
    public void testSpatialLeftJoins()
    {
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                anyTree(
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND a.name <> b.name",
                anyTree(
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point) AND name_a <> name_b",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // non-deterministic extra join predicate
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) AND rand() < 0.5",
                anyTree(
                        spatialLeftJoin("st_contains(st_geometryfromtext, st_point) AND rand() < 5e-1",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                        tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                anyTree(
                                        project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name")))))));

        // filter over join
        assertPlan("SELECT b.name, a.name " +
                        "FROM points a LEFT JOIN polygons b " +
                        "   ON ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat)) " +
                        "WHERE concat(a.name, b.name) is null",
                anyTree(
                        filter("concat(cast(name_a as varchar), cast(name_b as varchar)) is null",
                                spatialLeftJoin("st_contains(st_geometryfromtext, st_point)",
                                        project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")),
                                                tableScan("points", ImmutableMap.of("lng", "lng", "lat", "lat", "name_a", "name"))),
                                        anyTree(
                                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(cast(wkt as varchar))")),
                                                        tableScan("polygons", ImmutableMap.of("wkt", "wkt", "name_b", "name"))))))));
    }

    @Test
    public void testDistributedSpatialJoinOverUnion()
    {
        // union on the left side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) a, tpch.tiny.customer b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_contains(g1, g3)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p1", expression(format("spatial_partitions(cast('%s' as kdbtree), g1)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g1", expression("ST_GeometryFromText(cast(name_a1 as varchar))")),
                                                                tableScan("region", ImmutableMap.of("name_a1", "name")))),
                                                project(ImmutableMap.of("p2", expression(format("spatial_partitions(cast('%s' as kdbtree), g2)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g2", expression("ST_GeometryFromText(cast(name_a2 as varchar))")),
                                                                tableScan("nation", ImmutableMap.of("name_a2", "name"))))))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p3", expression(format("spatial_partitions(cast('%s' as kdbtree), g3)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g3", expression("ST_GeometryFromText(cast(name_b as varchar))")),
                                                                tableScan("customer", ImmutableMap.of("name_b", "name")))))))));

        // union on the right side
        assertDistributedPlan("SELECT a.name, b.name " +
                        "FROM tpch.tiny.customer a, (SELECT name FROM tpch.tiny.region UNION ALL SELECT name FROM tpch.tiny.nation) b " +
                        "WHERE ST_Contains(ST_GeometryFromText(a.name), ST_GeometryFromText(b.name))",
                withSpatialPartitioning("kdb_tree"),
                anyTree(
                        spatialJoin("st_contains(g1, g2)", Optional.of(KDB_TREE_JSON),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("p1", expression(format("spatial_partitions(cast('%s' as kdbtree), g1)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g1", expression("ST_GeometryFromText(cast(name_a as varchar))")),
                                                                tableScan("customer", ImmutableMap.of("name_a", "name")))))),
                                anyTree(
                                        unnest(exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                                project(ImmutableMap.of("p2", expression(format("spatial_partitions(cast('%s' as kdbtree), g2)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g2", expression("ST_GeometryFromText(cast(name_b1 as varchar))")),
                                                                tableScan("region", ImmutableMap.of("name_b1", "name")))),
                                                project(ImmutableMap.of("p3", expression(format("spatial_partitions(cast('%s' as kdbtree), g3)", KDB_TREE_JSON))),
                                                        project(ImmutableMap.of("g3", expression("ST_GeometryFromText(cast(name_b2 as varchar))")),
                                                                tableScan("nation", ImmutableMap.of("name_b2", "name"))))))))));
    }

    private Session withSpatialPartitioning(String tableName)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, tableName)
                .build();
    }
}
