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
package com.dremio.sabot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.Filter;
import com.dremio.exec.physical.config.Project;
import com.dremio.options.OptionValue;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.filter.FilterOperator;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.project.ProjectorStats.Metric;
import com.dremio.sabot.op.spi.SingleInputOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;

/*
 * Run the same test (simple expression) on both gandiva and java, and compare the perf.
 * Ignoring test by default, since it can take very long to run. And, requires gandiva support.
 */
@Ignore
public class TestGandivaPerf extends BaseTestOperator {
  private final String PREFER_JAVA = "java";
  private final String PREFER_GANDIVA = "gandiva";
  private final int numThreads = 1;
  private final int batchSize = 16383;

  class RunWithPreference<T extends SingleInputOperator> implements Callable<OperatorStats> {
    TpchTable table;
    double scale;
    PhysicalOperator operator;
    Class<T> clazz;

    RunWithPreference(TpchTable table, double scale, PhysicalOperator operator, Class<T> clazz) {
      this.table = table;
      this.scale = scale;
      this.operator = operator;
      this.clazz = clazz;
    }

    @Override
    public OperatorStats call() throws Exception {
      return runSingle(operator, clazz, table, scale, batchSize);
    }
  }

  /*
   * Returns total evaluation time.
   */
  <T extends SingleInputOperator>
  long runOne(String preference, String expr, TpchTable table, double scale,
              PhysicalOperator operator, Class<T> clazz) throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(numThreads);

    testContext.getOptions().setOption(OptionValue.createString(
      OptionValue.OptionType.SYSTEM,
      ExecConstants.QUERY_EXEC_OPTION_KEY,
      preference));

    List<Future<OperatorStats>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      Future<OperatorStats> ret = service.submit(new RunWithPreference(table, scale, operator, clazz));
      futures.add(ret);
    }

    long totalEvalTime = 0;
    long javaCodegenEvalTime = 0;
    long gandivaCodegenEvalTime = 0;
    for (Future<OperatorStats> future : futures) {
      OperatorStats stats = future.get();
      javaCodegenEvalTime += stats.getLongStat(Metric.JAVA_EVALUATE_TIME);
      gandivaCodegenEvalTime += stats.getLongStat(Metric.GANDIVA_EVALUATE_TIME);
    }
    totalEvalTime = javaCodegenEvalTime + gandivaCodegenEvalTime;
    System.out.println("evaluate time with pref " + preference + " for [" + expr + "] is " +
      " [" +
      " eval  : " + (javaCodegenEvalTime + gandivaCodegenEvalTime) + "ms " +
      " javaCodeGen : " + javaCodegenEvalTime + "ms " +
      " gandivaCodeGen : " + gandivaCodegenEvalTime + "ms " +
      "]");
    return totalEvalTime;
  }

  /*
   * Returns delta of evaluation time as a %.
   */
  <T extends SingleInputOperator>
  int runBoth(String expr, TpchTable table, double scale, PhysicalOperator operator, Class clazz)
    throws Exception {

    long javaTime = runOne(PREFER_JAVA, expr, table, scale, operator, clazz);
    long gandivaTime = runOne(PREFER_GANDIVA, expr, table, scale, operator, clazz);

    int deltaPcnt = (int)(((javaTime - gandivaTime) * 100) / javaTime);

    System.out.println("Gandiva execution time: " + gandivaTime + " ms");
    System.out.println("Java execution time: " + javaTime + " ms");
    System.out.println("Delta for [" + expr + "] is " + deltaPcnt + "%");
    return deltaPcnt;
  }

  private int compareProject(TpchTable table, double scale, String expr) throws Exception {
    Project project = new Project(PROPS, null, Arrays.asList(n(expr, "res")));
    return runBoth(expr, table, scale, project, ProjectOperator.class);
  }

  private int compareFilter(TpchTable table, double scale, String expr) throws Exception {
    Filter filter = new Filter(PROPS, null, parseExpr(expr), 1f);
    return runBoth(expr, table, scale, filter, FilterOperator.class);
  }

  @Test
  public void testProjectAdd() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "c_custkey + c_nationkey");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectLike() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "like(c_name, '%PROMO%')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectBeginsWith() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "like(c_name, 'PROMO%')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractYear() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "extractYear(castDATE(c_acctbal))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastDate() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "castDATE(c_date)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastTimestamp() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "castTIMESTAMP(c_time)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectToDate() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "to_date(c_date, 'YYYY-MM-DD', 1)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectConcat() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "concat(c_name, c_mktsegment, c_comment)");
    Assert.assertTrue(delta > 0);
    delta = compareProject(TpchTable.CUSTOMER, 6, "concat(c_name, c_mktsegment, c_name, c_address, c_comment, c_phone)");
    Assert.assertTrue(delta > 0);
    delta = compareProject(TpchTable.CUSTOMER, 6, "concat(c_phone, c_name, c_comment, c_mktsegment, c_name, " +
      "c_address, c_comment, c_phone, c_mktsegment, c_address)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSha1Numeric() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "hashsha1(c_custkey)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSha1Varchar() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "hashsha1(c_name)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSha256Numeric() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "hashSHA256(c_nationkey)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSha256Varchar() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "hashSHA256(c_name)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectLastday() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "last_day(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractDate() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "year(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
    delta = compareProject(TpchTable.CUSTOMER, 6, "month(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractDate2() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "weekofyear(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
    delta = compareProject(TpchTable.CUSTOMER, 6, "day(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractDate3() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 3, "hour(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
    delta = compareProject(TpchTable.CUSTOMER, 3, "minute(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectExtractDate4() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "second(cast(c_date as DATE))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSinh() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "sinh(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectTan() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "tan(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCosh() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "cosh(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCos() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "cos(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSin() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "sin(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastVarchar() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "cast(c_acctbal as VARCHAR(200))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastIntFromBigint() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "castINT(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectLeft() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "left(c_address, 10)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectRight() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "right(c_address, 10)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectRpad() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "rpad(c_address, 200, 'xxx')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectLpad() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "lpad(c_address, 200, 'xxx')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectStrpos() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "strpos(c_address, 'e')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectConvertTo() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "convert_to(c_name, 'UTF8')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectConvertFrom() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "convert_from(convert_to(c_name, 'UTF8'), 'UTF8')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectToTimeFromSeconds() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 4, "to_time(c_custkey)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectToTimestampFromSeconds() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 4, "to_timestamp(c_custkey)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastVarbinaryFromNumeric() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "cast(c_custkey as VARBINARY(200))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectCastVarbinaryFromVarchar() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "cast(c_name as VARBINARY(10500))");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectBin() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "bin(c_custkey)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectBase64() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "base64(c_name)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectAscii() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "ascii(c_name)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectSpace() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "space(c_acctbal)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectBytesSubstring() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "byte_substr(cast(c_custkey as VARBINARY(200)), 2, 5)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectUpper() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "upper(c_comment)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjecLower() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "lower(c_comment)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjecInitcap() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "initcap(c_comment)");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testProjectRegexpReplace() throws Exception {
    int delta = compareProject(TpchTable.CUSTOMER, 6, "regexp_replace(c_comment, 'a', 'b')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testFilterSimple() throws Exception {
    int delta = compareFilter(TpchTable.CUSTOMER, 6, "c_custkey < c_nationkey");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testFilterLike() throws Exception {
    int delta = compareFilter(TpchTable.CUSTOMER, 6, "like(c_name, '%PROMO%')");
    Assert.assertTrue(delta > 0);
  }

  @Test
  public void testFilterILike() throws Exception {
    int delta = compareFilter(TpchTable.CUSTOMER, 6, "ilike(c_name, '%PROMO%')");
    Assert.assertTrue(delta > 0);
  }
}
