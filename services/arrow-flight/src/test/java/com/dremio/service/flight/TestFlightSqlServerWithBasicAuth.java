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

import java.sql.SQLException;
import java.util.Collections;
import java.util.stream.IntStream;

import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.service.flight.impl.FlightWorkManager;
import com.google.common.collect.ImmutableList;

/**
 * Test FlightServer with basic authentication using FlightSql producer.
 */
public class TestFlightSqlServerWithBasicAuth extends AbstractTestFlightServer {
  @BeforeClass
  public static void setup() throws Exception {
    setupBaseFlightQueryTest(
      false,
      true,
      "flight.endpoint.port",
      FlightWorkManager.RunQueryResponseHandlerFactory.DEFAULT,
      DremioFlightService.FLIGHT_LEGACY_AUTH_MODE);
  }

  @Override
  protected String getAuthMode() {
    return DremioFlightService.FLIGHT_LEGACY_AUTH_MODE;
  }

  @Override
  public FlightInfo getFlightInfo(String query) throws SQLException {
    final FlightClientUtils.FlightClientWrapper clientWrapper = getFlightClientWrapper();

    final FlightSqlClient.PreparedStatement preparedStatement = clientWrapper.getSqlClient().prepare(query);
    return preparedStatement.execute();
  }

  @Test
  public void testGetTablesWithoutFiltering() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByCatalogPattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables("DREMIO", null, null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringBySchemaPattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, "INFORMATION_SCHEMA", null,
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByTablePattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, "COLUMNS",
      null, false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesFilteringByTableTypePattern() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      Collections.singletonList("SYSTEM_TABLE"), false);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      Assert.assertTrue(root.getRowCount() > 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGetTablesTypes() {
    FlightSqlClient flightSqlClient = getFlightClientWrapper().getSqlClient();
    FlightInfo flightInfo = flightSqlClient.getTableTypes();
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
      Assert.assertTrue(stream.next());
      VectorSchemaRoot root = stream.getRoot();
      final int rowCount = root.getRowCount();
      Assert.assertEquals(rowCount, 3);

      final ImmutableList<Text> item = ImmutableList.of(new Text("TABLE"), new Text("VIEW"), new Text("SYSTEM_TABLE"));

      final IntStream range = IntStream.range(0, rowCount);
      range.forEach(i -> Assert.assertEquals(root.getVector(0).getObject(i), item.get(i)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
