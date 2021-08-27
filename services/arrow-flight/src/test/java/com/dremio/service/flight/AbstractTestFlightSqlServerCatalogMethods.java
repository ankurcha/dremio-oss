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

import java.util.Collections;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProviderConditions;
import com.dremio.exec.proto.UserProtos;
import com.dremio.service.catalog.TableType;
import com.google.common.collect.ImmutableList;

/**
 * Base class for Flight SQL catalog methods tests.
 */
public abstract class AbstractTestFlightSqlServerCatalogMethods extends BaseFlightQueryTest {

  private FlightSqlClient flightSqlClient;

  @Before
  public void setUp() {
    flightSqlClient = getFlightClientWrapper().getSqlClient();
  }

  abstract CallOption[] getCallOptions();

  @Test
  public void testGetCatalogs() throws Exception {
    final CallOption[] callOptions = getCallOptions();

    final FlightInfo flightInfo = flightSqlClient.getCatalogs(callOptions);
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(), callOptions)) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();
      Assert.assertEquals(1, root.getRowCount());

      final String catalogName = ((VarCharVector) root.getVector("catalog_name")).getObject(0).toString();
      Assert.assertEquals("DREMIO", catalogName);
    }
  }

  @Test
  public void testGetTablesWithoutFiltering() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      null, false, getCallOptions());
    try (
      final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
        getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }

  @Test
  public void testGetTablesFilteringByCatalogPattern() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables("DREMIO", null, null,
      null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }

  @Test
  public void testGetTablesFilteringBySchemaPattern() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, "INFORMATION_SCHEMA",
      null, null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 5);
    }
  }

  @Test
  public void testGetTablesFilteringByTablePattern() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, null, "COLUMNS",
      null, false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 1);
    }
  }

  @Test
  public void testGetTablesFilteringByTableTypePattern() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      Collections.singletonList("SYSTEM_TABLE"), false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 28);
    }
  }

  @Test
  public void testGetTablesFilteringByMultiTableTypes() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      ImmutableList.of("TABLE", "VIEW"), false, getCallOptions());
    try (FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      Assert.assertEquals(root.getRowCount(), 0);
    }
  }

  @Test
  public void testGetTablesTypes() throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getTableTypes(getCallOptions());
    try (
      final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
        getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      final ImmutableList<TableType> tableTypes =
        ImmutableList.of(TableType.TABLE, TableType.SYSTEM_TABLE, TableType.VIEW);
      final int counter = tableTypes.size();

      final IntStream range = IntStream.range(0, counter);

      Assert.assertEquals(root.getRowCount(), counter);
      range.forEach(i -> {
        Assert.assertEquals(root.getVector(0).getObject(i).toString(), tableTypes.get(i).toString());
      });
    }
  }

  private void testGetSchemas(String catalog, String schemaPattern, boolean expectNonEmptyResult) throws Exception {
    final FlightInfo flightInfo = flightSqlClient.getSchemas(catalog, schemaPattern, getCallOptions());
    try (
      final FlightStream stream = flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
        getCallOptions())) {
      Assert.assertTrue(stream.next());
      final VectorSchemaRoot root = stream.getRoot();

      final Predicate<String> catalogNamePredicate = MetadataProviderConditions.getCatalogNamePredicate(
        catalog != null ? UserProtos.LikeFilter.newBuilder().setPattern(catalog).build() : null);
      final Pattern schemaFilterPattern =
        schemaPattern != null ? Pattern.compile(RegexpUtil.sqlToRegexLike(schemaPattern)) :
          Pattern.compile(".*");

      final VarCharVector catalogNameVector = (VarCharVector) root.getVector("catalog_name");
      final VarCharVector schemaNameVector = (VarCharVector) root.getVector("schema_name");

      for (int i = 0; i < root.getRowCount(); i++) {
        final String catalogName = catalogNameVector.getObject(i).toString();
        final String schemaName = schemaNameVector.getObject(i).toString();

        Assert.assertTrue(catalogNamePredicate.test(catalogName));
        Assert.assertTrue(schemaFilterPattern.matcher(schemaName).matches());
      }

      if (expectNonEmptyResult) {
        Assert.assertTrue(root.getRowCount() > 0);
      }
    }
  }

  @Test
  public void testGetSchemasWithNoFilter() throws Exception {
    testGetSchemas(null, null, true);
  }

  @Test
  public void testGetSchemasWithBothFilters() throws Exception {
    testGetSchemas("DREMIO", "INFORMATION_SCHEMA", true);
  }

  @Test
  public void testGetSchemasWithCatalog() throws Exception {
    testGetSchemas("DREMIO", null, true);
  }

  @Test
  public void testGetSchemasWithSchemaFilterPattern() throws Exception {
    testGetSchemas(null, "sys", true);
  }

  @Test
  public void testGetSchemasWithNonMatchingSchemaFilter() throws Exception {
    testGetSchemas(null, "NON_EXISTING_SCHEMA", false);
  }

  @Test
  public void testGetSchemasWithNonMatchingCatalog() throws Exception {
    testGetSchemas("NON_EXISTING_CATALOG", null, false);
  }

  /**
   * Retrieve a flightStream with schemas.
   *
   * @return a flight stream.
   */
  private FlightStream getSchemasFlightStream() {
    final FlightInfo flightInfo = flightSqlClient.getSchemas(null, null, getCallOptions());
    return flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions());
  }

  @Test
  public void testGetSchemasClosingBeforeStreamIsRetrieved() throws Exception {
    final FlightStream stream = getSchemasFlightStream();

    stream.close();
  }

  @Test
  public void testGetSchemasClosingAfterStreamIsRetrieved() throws Exception {
    final FlightStream stream = getSchemasFlightStream();
    //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
    while (stream.next()) {
      // Draining the stream before closing.
    }
    //CHECKSTYLE:ON EmptyStatement|EmptyBlock

    stream.close();
  }

  @Test
  public void testGetSchemasCancelingBeforeStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getSchemasFlightStream()) {
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
    }
  }

  @Test
  public void testGetSchemasCancelingAfterStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getSchemasFlightStream()) {
      //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
      while(stream.next()) {
        // Draining the stream before cancellation.
      }
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
      stream.getRoot().clear();
    }
  }


  /**
   * Retrieve a flightStream with tables info.
   *
   * @return a flight stream.
   */
  private FlightStream getTablesFlightStream() {
    final FlightInfo flightInfo = flightSqlClient.getTables(null, null, null,
      null, false, getCallOptions());
    return flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions());
  }

  @Test
  public void testGetTablesClosingBeforeStreamIsRetrieved() throws Exception {
    final FlightStream stream = getTablesFlightStream();

    stream.close();
  }

  @Test
  public void testGetTablesClosingAfterStreamIsRetrieved() throws Exception {
    final FlightStream stream = getTablesFlightStream();
    //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
    while (stream.next()) {
      // Draining the stream before closing.
    }
    //CHECKSTYLE:ON EmptyStatement|EmptyBlock

    stream.close();
  }

  @Test
  public void testGetTablesCancelingBeforeStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getTablesFlightStream()) {
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
    }
  }

  @Test
  public void testGetTablesCancelingAfterStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getTablesFlightStream()) {
      //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
      while(stream.next()) {
        // Draining the stream before cancellation.

      }
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
      stream.getRoot().clear();
    }
  }

  /**
   * Retrieve a stream with table types.
   *
   * @return a flight stream.
   */
  private FlightStream getTableTypesFlightStream() {
    final FlightInfo flightInfo = flightSqlClient.getTableTypes(getCallOptions());
    return flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions());
  }

  @Test
  public void testGetTablesTypesClosingBeforeStreamIsRetrieved() throws Exception {
    final FlightStream stream = getTableTypesFlightStream();

    stream.close();
  }

  @Test
  public void testGetTablesTypesClosingAfterStreamIsRetrieved() throws Exception {
    final FlightStream stream = getTableTypesFlightStream();
    //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
    while (stream.next()) {
      // Draining the stream before closing.
    }
    //CHECKSTYLE:ON EmptyStatement|EmptyBlock

    stream.close();
  }

  @Test
  public void testGetTablesTypesCancelingBeforeStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getTableTypesFlightStream()) {
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
    }
  }

  @Test
  public void testGetTablesTypesCancelingAfterStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getTableTypesFlightStream()) {
      //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
      while (stream.next()) {
        // Draining the stream before cancellation.

      }
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
      stream.getRoot().clear();
    }
  }

  @Test
  public void testGetCatalogsClosingBeforeStreamIsRetrieved() throws Exception {
    final FlightStream stream = getCatalogsFlightStream();

    stream.close();
  }

  /**
   * Retrieve a flight a stream with catalologs info.
   *
   * @return flight stream.
   */
  private FlightStream getCatalogsFlightStream() {
    final FlightInfo flightInfo = flightSqlClient.getCatalogs(getCallOptions());
    return flightSqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket(),
      getCallOptions());
  }

  @Test
  public void testGetCatalogsClosingAfterStreamIsRetrieved() throws Exception {
    final FlightStream stream = getCatalogsFlightStream();
    //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
    while (stream.next()) {
      // Draining the stream before closing.
    }
    //CHECKSTYLE:ON EmptyStatement|EmptyBlock

    stream.close();
  }

  @Test
  public void testGetCatalogsCancelingBeforeStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getCatalogsFlightStream()) {
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
    }
  }

  @Test
  public void testGetCatalogsCancelingAfterStreamIsRetrieved() throws Exception {
    try (final FlightStream stream = getCatalogsFlightStream()) {
      //CHECKSTYLE:OFF EmptyStatement|EmptyBlock
      while (stream.next()) {
        // Draining the stream before cancellation.

      }
      stream.cancel("Metadata retrieved canceled", new Exception("Testing query data retrieval cancellation."));
      stream.getRoot().clear();
    }
  }
}
