/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
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
package com.dremio.exec.planner.sql;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.google.common.collect.Sets;

public class TestSQLAlterTableMetadataRefresh {
  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Test
  public void testAlterTableRefreshMetadataAllFiles() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR ALL FILES";
    final  SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final  SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getAllFilesRefresh().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllPartitions() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR ALL PARTITIONS";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final  SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getAllPartitionsRefresh().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllFilesLazyUpdate() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR ALL FILES LAZY UPDATE";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getAllFilesRefresh().booleanValue());
    Assert.assertFalse(sqlRefreshTable.getForceUpdate().booleanValue());
  }

  @Test
  public void testAlterTableRefreshMetadataAllPartitionsLazyUpdate() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR ALL PARTITIONS LAZY UPDATE";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getAllPartitionsRefresh().booleanValue());
    Assert.assertFalse(sqlRefreshTable.getForceUpdate().booleanValue());
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForFilesEmptyList() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR FILES ()";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForPartitionsEmptyList() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS ()";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test
  public void testAlterTableRefreshMetadataForFiles_OneFile() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR FILES ('file1.json')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getFileRefresh().booleanValue());
    Assert.assertArrayEquals(new String[]{"file1.json"}, sqlRefreshTable.getFileNames().toArray(new String[0]));
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForFiles_WrongQuotes() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR FILES (\"file1.json\")";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test
  public void testAlterTableRefreshMetadataForFiles_SpecialCharacters() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR FILES ('file1''%$#&*.json')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getFileRefresh().booleanValue());
    Assert.assertArrayEquals(new String[]{"file1'%$#&*.json"}, sqlRefreshTable.getFileNames().toArray(new String[0]));
  }

  @Test
  public void testAlterTableRefreshMetadataForPartitions_OnePartitionKey() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS (\"year\" = '2021')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getPartitionRefresh().booleanValue());
    Assert.assertEquals(1, sqlRefreshTable.getPartitionList().size());

    final SqlNodeList pair = (SqlNodeList) sqlRefreshTable.getPartitionList().get(0);
    Assert.assertEquals(2, pair.size());
    Assert.assertEquals("year", ((SqlIdentifier) pair.get(0)).getSimple());
    Assert.assertEquals("2021", ((SqlLiteral) pair.get(1)).getValueAs(String.class));
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForPartitions_WrongQuotesKey() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS ('year' = '2021')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test(expected = UserException.class)
  public void testAlterTableRefreshMetadataForPartitions_WrongQuotesValue() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS (\"year\" = \"2021\")";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
  }

  @Test
  public void testAlterTableRefreshMetadataForPartitions_SpecialCharacters() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS (\"year\" = '2021''%$#&*')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getPartitionRefresh().booleanValue());
    Assert.assertEquals(1, sqlRefreshTable.getPartitionList().size());

    final SqlNodeList pair = (SqlNodeList) sqlRefreshTable.getPartitionList().get(0);
    Assert.assertEquals(2, pair.size());
    Assert.assertEquals("year", ((SqlIdentifier) pair.get(0)).getSimple());
    Assert.assertEquals("2021'%$#&*", ((SqlLiteral) pair.get(1)).getValueAs(String.class));
  }

  @Test
  public void testAlterTableRefreshMetadataForFiles_MultipleFiles() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR FILES ('file1.json', 'file2.json')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getFileRefresh().booleanValue());
    Assert.assertArrayEquals(new String[]{"file1.json", "file2.json"}, sqlRefreshTable.getFileNames().toArray(new String[0]));
  }

  @Test
  public void testAlterTableRefreshMetadataForPartitions_MultiplePartitionKeys() {
    final String sql = "ALTER TABLE tbl REFRESH METADATA FOR PARTITIONS (\"year\" = '2021', \"month\" = 'Jan')";
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.OTHER)));

    final SqlRefreshTable sqlRefreshTable = (SqlRefreshTable) sqlNode;
    Assert.assertTrue(sqlRefreshTable.getPartitionRefresh().booleanValue());
    Assert.assertEquals(2, sqlRefreshTable.getPartitionList().size());

    final SqlNodeList pair1 = (SqlNodeList) sqlRefreshTable.getPartitionList().get(0);
    Assert.assertEquals(2, pair1.size());
    Assert.assertEquals("year", ((SqlIdentifier) pair1.get(0)).getSimple());
    Assert.assertEquals("2021", ((SqlLiteral) pair1.get(1)).getValueAs(String.class));

    final SqlNodeList pair2 = (SqlNodeList) sqlRefreshTable.getPartitionList().get(1);
    Assert.assertEquals(2, pair2.size());
    Assert.assertEquals("month", ((SqlIdentifier) pair2.get(0)).getSimple());
    Assert.assertEquals("Jan", ((SqlLiteral) pair2.get(1)).getValueAs(String.class));
  }
}
