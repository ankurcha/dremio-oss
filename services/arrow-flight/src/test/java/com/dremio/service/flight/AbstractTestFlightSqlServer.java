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

package com.dremio.service.flight;

import static java.util.Arrays.asList;

import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.stream.IntStream;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.service.catalog.TableType;
import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProviderConditions;
import com.dremio.exec.proto.UserProtos;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public abstract class AbstractTestFlightSqlServer extends AbstractTestFlightServer {

  private final ExecutionMode executionMode;

  enum ExecutionMode {
    STATEMENT,
    PREPARED_STATEMENT
  }

  @Parameterized.Parameters(name = "ExecutionMode: {0}")
  public static Collection<ExecutionMode> parameters() {
    return asList(ExecutionMode.STATEMENT, ExecutionMode.PREPARED_STATEMENT);
  }

  public AbstractTestFlightSqlServer(ExecutionMode executionMode) {
    this.executionMode = executionMode;
  }

  private FlightInfo executeStatement(String query) {
    final FlightSqlClient client = getFlightClientWrapper().getSqlClient();
    return client.execute(query, getCallOptions());
  }

  private FlightInfo executePreparedStatement(String query) throws SQLException {
    final FlightSqlClient client = getFlightClientWrapper().getSqlClient();
    final FlightSqlClient.PreparedStatement preparedStatement = client.prepare(query, getCallOptions());
    return preparedStatement.execute(getCallOptions());
  }

  @Override
  public FlightInfo getFlightInfo(String query) throws SQLException{
    switch (executionMode) {
      case STATEMENT: return executeStatement(query);
      case PREPARED_STATEMENT: return executePreparedStatement(query);
    }

    throw new IllegalStateException();
  }
}
