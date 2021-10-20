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

import static com.google.protobuf.Any.pack;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.createBitmaskFromEnums;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.SqlInfoProvider;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedGroupBy;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightSqlProducer {

  private static final SqlInfoProvider sqlInfoProvider = new SqlInfoProvider();
  private final FlightWorkManager flightWorkManager;
  private final Location location;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;
  private final Cache<UserProtos.PreparedStatementHandle, FlightPreparedStatement> flightPreparedStatementCache;

  public DremioFlightProducer(Location location, DremioFlightSessionsManager sessionsManager,
                              Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                              BufferAllocator allocator,
                              RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.location = location;
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
    flightPreparedStatementCache = CacheBuilder.newBuilder()
      .maximumSize(1024)
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .build();
    setUoSqlInfoProvider();
  }

  private void setUoSqlInfoProvider() {
    // TODO Rebase branch to get SqlInfo.SQL_SUPPORTS_CONVERT and SqlSupportsConvert
//    sqlInfoProvider.setIntToIntListMapProvider(SqlInfo.forNumber(517), ImmutableMap.builder()
//      .put(
//        SqlSupportsConvert.SQL_CONVERT_BIT_VALUE,
//        Arrays.asList(
//          SqlSupportsConvert.SQL_CONVERT_INTEGER_VALUE,
//          SqlSupportsConvert.SQL_CONVERT_BIGINT_VALUE))
//      .build());
    final String MOCK_DATABASE_PRODUCT_NAME = "Test Server Name";
    final String MOCK_DATABASE_PRODUCT_VERSION = "v0.0.1-alpha";
    final String MOCK_IDENTIFIER_QUOTE_STRING = "\"";
    final boolean MOCK_IS_READ_ONLY = true;
    final String MOCK_SQL_KEYWORDS = "ADD, ADD CONSTRAINT, ALTER, ALTER TABLE, ANY, USER, TABLE";
    final String MOCK_NUMERIC_FUNCTIONS = "ABS(), ACOS(), ASIN(), ATAN(), CEIL(), CEILING(), COT()";
    final String MOCK_STRING_FUNCTIONS = "ASCII, CHAR, CHARINDEX, CONCAT, CONCAT_WS, FORMAT, LEFT";
    final String MOCK_SYSTEM_FUNCTIONS = "CAST, CONVERT, CHOOSE, ISNULL, IS_NUMERIC, IIF, TRY_CAST";
    final String MOCK_TIME_DATE_FUNCTIONS = "GETDATE(), DATEPART(), DATEADD(), DATEDIFF()";
    final String MOCK_SEARCH_STRING_ESCAPE = "\\";
    final String MOCK_EXTRA_NAME_CHARACTERS = "";
    final boolean MOCK_SUPPORTS_COLUMN_ALIASING = true;
    final boolean MOCK_NULL_PLUS_NULL_IS_NULL = true;
    final boolean MOCK_SUPPORTS_TABLE_CORRELATION_NAMES = true;
    final boolean MOCK_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES = false;
    final boolean MOCK_EXPRESSIONS_IN_ORDER_BY = true;
    final boolean MOCK_SUPPORTS_ORDER_BY_UNRELATED = true;
    final boolean MOCK_SUPPORTS_LIKE_ESCAPE_CLAUSE = true;
    final boolean MOCK_NON_NULLABLE_COLUMNS = true;
    final String MOCK_SCHEMA_TERM = "schema";
    final String MOCK_PROCEDURE_TERM = "procedure";
    final String MOCK_CATALOG_TERM = "catalog";
    final boolean MOCK_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = true;
    final boolean MOCK_CATALOG_AT_START = false;
    final boolean MOCK_SELECT_FOR_UPDATE_SUPPORTED = false;
    final boolean MOCK_STORED_PROCEDURES_SUPPORTED = false;
    final int MOCK_SUPPORTED_SUBQUERIES = 1;
    final boolean MOCK_CORRELATED_SUBQUERIES_SUPPORTED = true;
    final int MOCK_MAX_BINARY_LITERAL_LENGTH = 0;
    final int MOCK_MAX_CHAR_LITERAL_LENGTH = 0;
    final int MOCK_MAX_COLUMN_NAME_LENGTH = 1024;
    final int MOCK_MAX_COLUMNS_IN_GROUP_BY = 0;
    final int MOCK_MAX_COLUMNS_IN_INDEX = 0;
    final int MOCK_MAX_COLUMNS_IN_ORDER_BY = 0;
    final int MOCK_MAX_COLUMNS_IN_SELECT = 0;
    final int MOCK_MAX_CONNECTIONS = 0;
    final int MOCK_MAX_CURSOR_NAME_LENGTH = 1024;
    final int MOCK_MAX_INDEX_LENGTH = 0;
    final int MOCK_SCHEMA_NAME_LENGTH = 1024;
    final int MOCK_MAX_PROCEDURE_NAME_LENGTH = 0;
    final int MOCK_MAX_CATALOG_NAME_LENGTH = 1024;
    final int MOCK_MAX_ROW_SIZE = 0;
    final boolean MOCK_MAX_ROW_SIZE_INCLUDES_BLOBS = false;
    final int MOCK_MAX_STATEMENT_LENGTH = 0;
    final int MOCK_MAX_STATEMENTS = 0;
    final int MOCK_MAX_TABLE_NAME_LENGTH = 1024;
    final int MOCK_MAX_TABLES_IN_SELECT = 0;
    final int MOCK_MAX_USERNAME_LENGTH = 1024;
    final int MOCK_DEFAULT_TRANSACTION_ISOLATION = 0;
    final boolean MOCK_TRANSACTIONS_SUPPORTED = false;
    final boolean MOCK_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT = true;
    final boolean MOCK_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED = false;
    final boolean MOCK_BATCH_UPDATES_SUPPORTED = true;
    final boolean MOCK_SAVEPOINTS_SUPPORTED = false;
    final boolean MOCK_NAMED_PARAMETERS_SUPPORTED = false;
    final boolean MOCK_LOCATORS_UPDATE_COPY = true;
    final boolean MOCK_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED = false;

    sqlInfoProvider.setStringProvider(SqlInfo.FLIGHT_SQL_SERVER_NAME, MOCK_DATABASE_PRODUCT_NAME);
    sqlInfoProvider.setStringProvider(SqlInfo.FLIGHT_SQL_SERVER_VERSION, MOCK_DATABASE_PRODUCT_VERSION);
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR, MOCK_IDENTIFIER_QUOTE_STRING);
    sqlInfoProvider.setBooleanProvider(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, MOCK_IS_READ_ONLY);
    sqlInfoProvider.setStringListProvider(SqlInfo.SQL_KEYWORDS, MOCK_SQL_KEYWORDS.split("\\s*,\\s*"));
    sqlInfoProvider.setStringListProvider(SqlInfo.SQL_NUMERIC_FUNCTIONS, MOCK_NUMERIC_FUNCTIONS.split("\\s*,\\s*"));
    sqlInfoProvider.setStringListProvider(SqlInfo.SQL_STRING_FUNCTIONS, MOCK_STRING_FUNCTIONS.split("\\s*,\\s*"));
    sqlInfoProvider.setStringListProvider(SqlInfo.SQL_SYSTEM_FUNCTIONS, MOCK_SYSTEM_FUNCTIONS.split("\\s*,\\s*"));
    sqlInfoProvider.setStringListProvider(SqlInfo.SQL_DATETIME_FUNCTIONS,
      MOCK_TIME_DATE_FUNCTIONS.split("\\s*,\\s*"));
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_SEARCH_STRING_ESCAPE, MOCK_SEARCH_STRING_ESCAPE);
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_EXTRA_NAME_CHARACTERS, MOCK_EXTRA_NAME_CHARACTERS);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING, MOCK_SUPPORTS_COLUMN_ALIASING);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL, MOCK_NULL_PLUS_NULL_IS_NULL);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES,
      MOCK_SUPPORTS_TABLE_CORRELATION_NAMES);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES,
      MOCK_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY, MOCK_EXPRESSIONS_IN_ORDER_BY);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED, MOCK_SUPPORTS_ORDER_BY_UNRELATED);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_GROUP_BY,
      (int) createBitmaskFromEnums(SqlSupportedGroupBy.SQL_GROUP_BY_UNRELATED));
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE, MOCK_SUPPORTS_LIKE_ESCAPE_CLAUSE);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS, MOCK_NON_NULLABLE_COLUMNS);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_GRAMMAR,
      (int) (createBitmaskFromEnums(SupportedSqlGrammar.SQL_CORE_GRAMMAR, SupportedSqlGrammar.SQL_MINIMUM_GRAMMAR)));
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
      (int) (createBitmaskFromEnums(SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL,
        SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL)));
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY,
      MOCK_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY);
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_SCHEMA_TERM, MOCK_SCHEMA_TERM);
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_CATALOG_TERM, MOCK_CATALOG_TERM);
    sqlInfoProvider.setStringProvider(SqlInfo.SQL_PROCEDURE_TERM, MOCK_PROCEDURE_TERM);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_CATALOG_AT_START, MOCK_CATALOG_AT_START);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
      (int) (createBitmaskFromEnums(SqlSupportedElementActions.SQL_ELEMENT_IN_PROCEDURE_CALLS,
        SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS)));
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS,
      (int) createBitmaskFromEnums(SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS));
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
      (int) createBitmaskFromEnums(SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE));
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED, MOCK_SELECT_FOR_UPDATE_SUPPORTED);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED, MOCK_STORED_PROCEDURES_SUPPORTED);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED, MOCK_SUPPORTED_SUBQUERIES);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED,
      MOCK_CORRELATED_SUBQUERIES_SUPPORTED);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_UNIONS,
      (int) createBitmaskFromEnums(SqlSupportedUnions.SQL_UNION_ALL));
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH, MOCK_MAX_BINARY_LITERAL_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH, MOCK_MAX_CHAR_LITERAL_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH, MOCK_MAX_COLUMN_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY, MOCK_MAX_COLUMNS_IN_GROUP_BY);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_INDEX, MOCK_MAX_COLUMNS_IN_INDEX);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY, MOCK_MAX_COLUMNS_IN_ORDER_BY);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_COLUMNS_IN_SELECT, MOCK_MAX_COLUMNS_IN_SELECT);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_CONNECTIONS, MOCK_MAX_CONNECTIONS);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH, MOCK_MAX_CURSOR_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_INDEX_LENGTH, MOCK_MAX_INDEX_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_SCHEMA_NAME_LENGTH, MOCK_SCHEMA_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH, MOCK_MAX_PROCEDURE_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH, MOCK_MAX_CATALOG_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_ROW_SIZE, MOCK_MAX_ROW_SIZE);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS, MOCK_MAX_ROW_SIZE_INCLUDES_BLOBS);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_STATEMENT_LENGTH, MOCK_MAX_STATEMENT_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_STATEMENTS, MOCK_MAX_STATEMENTS);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_TABLE_NAME_LENGTH, MOCK_MAX_TABLE_NAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_TABLES_IN_SELECT, MOCK_MAX_TABLES_IN_SELECT);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_MAX_USERNAME_LENGTH, MOCK_MAX_USERNAME_LENGTH);
    sqlInfoProvider.setBitIntProvider(SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION,
      MOCK_DEFAULT_TRANSACTION_ISOLATION);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_TRANSACTIONS_SUPPORTED, MOCK_TRANSACTIONS_SUPPORTED);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
      (int) (createBitmaskFromEnums(SqlTransactionIsolationLevel.SQL_TRANSACTION_SERIALIZABLE,
        SqlTransactionIsolationLevel.SQL_TRANSACTION_READ_COMMITTED)));
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT,
      MOCK_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED,
      MOCK_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED);
    sqlInfoProvider.setIntProvider(SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES,
      (int) (createBitmaskFromEnums(SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY,
        SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE)));
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_BATCH_UPDATES_SUPPORTED, MOCK_BATCH_UPDATES_SUPPORTED);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_SAVEPOINTS_SUPPORTED, MOCK_SAVEPOINTS_SUPPORTED);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED, MOCK_NAMED_PARAMETERS_SUPPORTED);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_LOCATORS_UPDATE_COPY, MOCK_LOCATORS_UPDATE_COPY);
    sqlInfoProvider.setBooleanProvider(SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED,
      MOCK_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED);
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    if (isFlightSqlTicket(ticket)) {
      FlightSqlProducer.super.getStream(callContext, ticket, serverStreamListener);
      return;
    }

    getStreamLegacy(callContext, ticket, serverStreamListener);
  }

  private void getStreamLegacy(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {

    final TicketContent.PreparedStatementTicket preparedStatementTicket;
    try {
      preparedStatementTicket = TicketContent.PreparedStatementTicket.parseFrom(ticket.getBytes());
    } catch (InvalidProtocolBufferException ex) {
      final RuntimeException error =
        CallStatus.INVALID_ARGUMENT.withCause(ex).withDescription("Invalid ticket used in getStream")
          .toRuntimeException();
      serverStreamListener.error(error);
      throw error;
    }

    UserProtos.PreparedStatementHandle preparedStatementHandle = preparedStatementTicket.getHandle();

    runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery commandPreparedStatementQuery,
                                         CallContext callContext,
                                         ServerStreamListener serverStreamListener) {
    UserProtos.PreparedStatementHandle preparedStatementHandle;
    try {
      preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    // Check if given PreparedStatement is cached
    FlightPreparedStatement preparedStatement = flightPreparedStatementCache.getIfPresent(preparedStatementHandle);
    if (preparedStatement == null) {
      throw CallStatus.NOT_FOUND.withDescription("PreparedStatement not found.").toRuntimeException();
    }

    runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listFlights is unimplemented").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    if (isFlightSqlCommand(flightDescriptor)) {
      return FlightSqlProducer.super.getFlightInfo(callContext, flightDescriptor);
    }

    return getFlightInfoLegacy(callContext, flightDescriptor);
  }

  private FlightInfo getFlightInfoLegacy(CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(flightDescriptor, callContext::isCancelled, session);

    flightPreparedStatementCache.put(flightPreparedStatement.getServerHandle(), flightPreparedStatement);

    return flightPreparedStatement.getFlightInfo(location);
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserProtos.PreparedStatementHandle preparedStatementHandle;

    try {
      preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(commandPreparedStatementQuery.getPreparedStatementHandle());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }

    FlightPreparedStatement preparedStatement = flightPreparedStatementCache.getIfPresent(preparedStatementHandle);
    if (preparedStatement == null) {
      throw CallStatus.NOT_FOUND.withDescription("PreparedStatement not found.").toRuntimeException();
    }

    Schema schema = preparedStatement.getSchema();
    return getFlightInfoForFlightSqlCommands(commandPreparedStatementQuery, flightDescriptor, schema);
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream,
                            StreamListener<PutResult> streamListener) {
    if (isFlightSqlCommand(flightStream.getDescriptor())) {
      return FlightSqlProducer.super.acceptPut(callContext, flightStream, streamListener);
    }

    throw CallStatus.UNIMPLEMENTED.withDescription("acceptPut is unimplemented").toRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    if (isFlightSqlAction(action)) {
      FlightSqlProducer.super.doAction(callContext, action, streamListener);
      return;
    }

    throw CallStatus.UNIMPLEMENTED.withDescription("doAction is unimplemented").toRuntimeException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listActions is unimplemented").toRuntimeException();
  }

  @Override
  public void createPreparedStatement(
    ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {
    final String query = actionCreatePreparedStatementRequest.getQuery();

    final UserSession session = getUserSessionFromCallContext(callContext);
    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(query, callContext::isCancelled, session);

    flightPreparedStatementCache.put(flightPreparedStatement.getServerHandle(), flightPreparedStatement);

    final ActionCreatePreparedStatementResult action = flightPreparedStatement.createAction();

    streamListener.onNext(new Result(pack(action).toByteArray()));
    streamListener.onCompleted();

  }

  @Override
  public void closePreparedStatement(
    ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> listener) {
    try {
      UserProtos.PreparedStatementHandle preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(actionClosePreparedStatementRequest.getPreparedStatementHandle());

      flightPreparedStatementCache.invalidate(preparedStatementHandle);
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid PreparedStatementHandle").toRuntimeException();
    }
  }

  @Override
  public FlightInfo getFlightInfoStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    final FlightPreparedStatement flightPreparedStatement = flightWorkManager
      .createPreparedStatement(commandStatementQuery.getQuery(), callContext::isCancelled, session);

    flightPreparedStatementCache.put(flightPreparedStatement.getServerHandle(), flightPreparedStatement);

    final TicketStatementQuery ticket =
      TicketStatementQuery.newBuilder()
        .setStatementHandle(flightPreparedStatement.getServerHandle().toByteString())
        .build();

    final Schema schema = flightPreparedStatement.getSchema();
    return getFlightInfoForFlightSqlCommands(ticket, flightDescriptor, schema);
  }

  @Override
  public SchemaResult getSchemaStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final FlightInfo info = this.getFlightInfo(callContext, flightDescriptor);
    return new SchemaResult(info.getSchema());
  }

  @Override
  public void getStreamStatement(TicketStatementQuery ticketStatementQuery,
                                 CallContext callContext,
                                 ServerStreamListener serverStreamListener) {
    try {
      final UserProtos.PreparedStatementHandle preparedStatementHandle =
        UserProtos.PreparedStatementHandle.parseFrom(ticketStatementQuery.getStatementHandle());

      runPreparedStatement(callContext, serverStreamListener, preparedStatementHandle);
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INTERNAL.toRuntimeException();
    }
  }

  @Override
  public Runnable acceptPutStatement(
    CommandStatementUpdate commandStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Statement not supported.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
    CommandPreparedStatementUpdate commandPreparedStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement with parameter binding not supported.")
      .toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("PreparedStatement with parameter binding not supported.")
      .toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    return new FlightInfo(
      Schemas.GET_SQL_INFO_SCHEMA,
      flightDescriptor,
      Collections.singletonList(new FlightEndpoint(new Ticket(Any.pack(commandGetSqlInfo).toByteArray()))),
      -1, -1);
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                               CallContext callContext,
                               ServerStreamListener serverStreamListener) {
    sqlInfoProvider.send(commandGetSqlInfo.getInfoList(), serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(
    CommandGetCatalogs commandGetCatalogs, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    final Schema catalogsSchema = Schemas.GET_CATALOGS_SCHEMA;
    return getFlightInfoForFlightSqlCommands(commandGetCatalogs, flightDescriptor, catalogsSchema);
  }

  @Override
  public void getStreamCatalogs(CallContext callContext,
                                ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    flightWorkManager.getCatalogs(serverStreamListener, allocator, callContext::isCancelled, session);
  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetSchemas commandGetSchemas,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_SCHEMAS_SCHEMA;
    return getFlightInfoForFlightSqlCommands(commandGetSchemas, flightDescriptor, schema);
  }

  @Override
  public void getStreamSchemas(CommandGetSchemas commandGetSchemas,
                               CallContext callContext,
                               ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    String catalog = commandGetSchemas.hasCatalog() ? commandGetSchemas.getCatalog() : null;
    String schemaFilterPattern =
      commandGetSchemas.hasSchemaFilterPattern() ? commandGetSchemas.getSchemaFilterPattern() : null;

    flightWorkManager.getSchemas(
      catalog, schemaFilterPattern, serverStreamListener, allocator, callContext::isCancelled, session);
  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables commandGetTables,
                                        CallContext callContext,
                                        FlightDescriptor flightDescriptor) {
    final Schema schema;
    if (commandGetTables.getIncludeSchema()) {
      schema = Schemas.GET_TABLES_SCHEMA;
    } else {
      schema = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
    }

    return getFlightInfoForFlightSqlCommands(commandGetTables, flightDescriptor, schema);
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables,
                              CallContext callContext,
                              ServerStreamListener serverStreamListener) {
    final UserSession session = getUserSessionFromCallContext(callContext);

    flightWorkManager.runGetTables(commandGetTables, serverStreamListener, callContext::isCancelled,
      allocator, session);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(
    CommandGetTableTypes commandGetTableTypes, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_TABLE_TYPES_SCHEMA;

    return getFlightInfoForFlightSqlCommands(commandGetTableTypes, flightDescriptor, schema);
  }

  @Override
  public void getStreamTableTypes(CallContext callContext,
                                  ServerStreamListener serverStreamListener) {
    flightWorkManager.runGetTablesTypes(serverStreamListener,
      allocator);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
    CommandGetPrimaryKeys commandGetPrimaryKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_PRIMARY_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys,
                                   CallContext callContext,
                                   ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetPrimaryKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_IMPORTED_AND_EXPORTED_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetExportedKeys not supported.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_IMPORTED_AND_EXPORTED_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public void getStreamImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext,
    ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetImportedKeys not supported.").toRuntimeException();
  }

  @Override
  public void close() throws Exception {

  }

  private void runPreparedStatement(CallContext callContext,
                                    ServerStreamListener serverStreamListener,
                                    UserProtos.PreparedStatementHandle preparedStatementHandle) {
    final UserSession session = getUserSessionFromCallContext(callContext);
    flightWorkManager.runPreparedStatement(preparedStatementHandle, serverStreamListener, allocator, session);
  }

  /**
   * Helper method to retrieve CallHeaders from the CallContext.
   *
   * @param callContext the CallContext to retrieve headers from.
   * @return CallHeaders retrieved from provided CallContext.
   */
  private CallHeaders retrieveHeadersFromCallContext(CallContext callContext) {
    return callContext.getMiddleware(FlightConstants.HEADER_KEY).headers();
  }

  private UserSession getUserSessionFromCallContext(CallContext callContext) {
    final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
    return sessionsManager.getUserSession(callContext.peerIdentity(), headers);
  }

  private <T extends Message> FlightInfo getFlightInfoForFlightSqlCommands(
    T command, FlightDescriptor flightDescriptor, Schema schema) {
    final Ticket ticket = new Ticket(pack(command).toByteArray());

    final FlightEndpoint flightEndpoint = new FlightEndpoint(ticket, location);
    return new FlightInfo(schema, flightDescriptor, ImmutableList.of(flightEndpoint), -1, -1);
  }

  private boolean isFlightSqlCommand(Any command) {
    return command.is(CommandStatementQuery.class) || command.is(CommandPreparedStatementQuery.class) ||
      command.is(CommandGetCatalogs.class) || command.is(CommandGetSchemas.class) ||
      command.is(CommandGetTables.class) || command.is(CommandGetTableTypes.class) ||
      command.is(CommandGetSqlInfo.class) || command.is(CommandGetPrimaryKeys.class) ||
      command.is(CommandGetExportedKeys.class) || command.is(CommandGetImportedKeys.class) ||
      command.is(TicketStatementQuery.class);
  }

  private boolean isFlightSqlCommand(byte[] bytes) {
    try {
      Any command = Any.parseFrom(bytes);
      return isFlightSqlCommand(command);
    } catch (InvalidProtocolBufferException e) {
      return false;
    }
  }

  private boolean isFlightSqlCommand(FlightDescriptor flightDescriptor) {
    return isFlightSqlCommand(flightDescriptor.getCommand());
  }

  private boolean isFlightSqlTicket(Ticket ticket) {
    // The byte array on ticket is a serialized FlightSqlCommand
    return isFlightSqlCommand(ticket.getBytes());
  }

  private boolean isFlightSqlAction(Action action) {
    String actionType = action.getType();
    return FlightSqlUtils.FLIGHT_SQL_ACTIONS.stream().anyMatch(action2 -> action2.getType().equals(actionType));
  }
}
