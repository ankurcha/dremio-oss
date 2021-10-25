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
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions.SQL_ELEMENT_IN_PROCEDURE_CALLS;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedGroupBy.SQL_GROUP_BY_UNRELATED;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions.SQL_UNION_ALL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_BIGINT_VALUE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_BIT_VALUE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert.SQL_CONVERT_INTEGER_VALUE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel.SQL_TRANSACTION_READ_COMMITTED;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel.SQL_TRANSACTION_SERIALIZABLE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar.SQL_CORE_GRAMMAR;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar.SQL_MINIMUM_GRAMMAR;
import static org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;

import java.util.Arrays;
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
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
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

  private final SqlInfoBuilder sqlInfoBuilder;
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
    sqlInfoBuilder = getSqlInfoBuilder();
  }

  private SqlInfoBuilder getSqlInfoBuilder() {
    final String DATABASE_PRODUCT_NAME = "Test Server Name";
    final String DATABASE_PRODUCT_VERSION = "v0.0.1-alpha";
    final String IDENTIFIER_QUOTE_STRING = "\"";
    final boolean IS_READ_ONLY = true;
    final String SQL_KEYWORDS = "ADD, ADD CONSTRAINT, ALTER, ALTER TABLE, ANY, USER, TABLE";
    final String NUMERIC_FUNCTIONS = "ABS(), ACOS(), ASIN(), ATAN(), CEIL(), CEILING(), COT()";
    final String STRING_FUNCTIONS = "ASCII, CHAR, CHARINDEX, CONCAT, CONCAT_WS, FORMAT, LEFT";
    final String SYSTEM_FUNCTIONS = "CAST, CONVERT, CHOOSE, ISNULL, IS_NUMERIC, IIF, TRY_CAST";
    final String TIME_DATE_FUNCTIONS = "GETDATE(), DATEPART(), DATEADD(), DATEDIFF()";
    final String SEARCH_STRING_ESCAPE = "\\";
    final String EXTRA_NAME_CHARACTERS = "";
    final boolean SUPPORTS_COLUMN_ALIASING = true;
    final boolean NULL_PLUS_NULL_IS_NULL = true;
    final boolean SUPPORTS_TABLE_CORRELATION_NAMES = true;
    final boolean SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES = false;
    final boolean EXPRESSIONS_IN_ORDER_BY = true;
    final boolean SUPPORTS_ORDER_BY_UNRELATED = true;
    final boolean SUPPORTS_LIKE_ESCAPE_CLAUSE = true;
    final boolean NON_NULLABLE_COLUMNS = true;
    final String SCHEMA_TERM = "schema";
    final String PROCEDURE_TERM = "procedure";
    final String CATALOG_TERM = "catalog";
    final boolean SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = true;
    final boolean CATALOG_AT_START = false;
    final boolean SELECT_FOR_UPDATE_SUPPORTED = false;
    final boolean STORED_PROCEDURES_SUPPORTED = false;
    final int SUPPORTED_SUBQUERIES = 1;
    final boolean CORRELATED_SUBQUERIES_SUPPORTED = true;
    final int MAX_BINARY_LITERAL_LENGTH = 0;
    final int MAX_CHAR_LITERAL_LENGTH = 0;
    final int MAX_COLUMN_NAME_LENGTH = 1024;
    final int MAX_COLUMNS_IN_GROUP_BY = 0;
    final int MAX_COLUMNS_IN_INDEX = 0;
    final int MAX_COLUMNS_IN_ORDER_BY = 0;
    final int MAX_COLUMNS_IN_SELECT = 0;
    final int MAX_CONNECTIONS = 0;
    final int MAX_CURSOR_NAME_LENGTH = 1024;
    final int MAX_INDEX_LENGTH = 0;
    final int SCHEMA_NAME_LENGTH = 1024;
    final int MAX_PROCEDURE_NAME_LENGTH = 0;
    final int MAX_CATALOG_NAME_LENGTH = 1024;
    final int MAX_ROW_SIZE = 0;
    final boolean MAX_ROW_SIZE_INCLUDES_BLOBS = false;
    final int MAX_STATEMENT_LENGTH = 0;
    final int MAX_STATEMENTS = 0;
    final int MAX_TABLE_NAME_LENGTH = 1024;
    final int MAX_TABLES_IN_SELECT = 0;
    final int MAX_USERNAME_LENGTH = 1024;
    final int DEFAULT_TRANSACTION_ISOLATION = 0;
    final boolean TRANSACTIONS_SUPPORTED = false;
    final boolean DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT = true;
    final boolean DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED = false;
    final boolean BATCH_UPDATES_SUPPORTED = true;
    final boolean SAVEPOINTS_SUPPORTED = false;
    final boolean NAMED_PARAMETERS_SUPPORTED = false;
    final boolean LOCATORS_UPDATE_COPY = true;
    final boolean STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED = false;

    return sqlInfoBuilder
      .withFlightSqlServerName(DATABASE_PRODUCT_NAME)
      .withFlightSqlServerVersion(DATABASE_PRODUCT_VERSION)
      .withSqlIdentifierQuoteChar(IDENTIFIER_QUOTE_STRING)
      .withFlightSqlServerReadOnly(IS_READ_ONLY)
      .withSqlKeywords(SQL_KEYWORDS.split("\\s*,\\s*"))
      .withSqlNumericFunctions(NUMERIC_FUNCTIONS.split("\\s*,\\s*"))
      .withSqlStringFunctions(STRING_FUNCTIONS.split("\\s*,\\s*"))
      .withSqlSystemFunctions(SYSTEM_FUNCTIONS.split("\\s*,\\s*"))
      .withSqlDatetimeFunctions(TIME_DATE_FUNCTIONS.split("\\s*,\\s*"))
      .withSqlSearchStringEscape(SEARCH_STRING_ESCAPE)
      .withSqlExtraNameCharacters(EXTRA_NAME_CHARACTERS)
      .withSqlSupportsColumnAliasing(SUPPORTS_COLUMN_ALIASING)
      .withSqlNullPlusNullIsNull(NULL_PLUS_NULL_IS_NULL)
      .withSqlSupportsTableCorrelationNames(SUPPORTS_TABLE_CORRELATION_NAMES)
      .withSqlSupportsDifferentTableCorrelationNames(SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES)
      .withSqlSupportsExpressionsInOrderBy(EXPRESSIONS_IN_ORDER_BY)
      .withSqlSupportsOrderByUnrelated(SUPPORTS_ORDER_BY_UNRELATED)
      .withSqlSupportedGroupBy(SQL_GROUP_BY_UNRELATED)
      .withSqlSupportsLikeEscapeClause(SUPPORTS_LIKE_ESCAPE_CLAUSE)
      .withSqlSupportsNonNullableColumns(NON_NULLABLE_COLUMNS)
      .withSqlSupportedGrammar(SQL_CORE_GRAMMAR, SQL_MINIMUM_GRAMMAR)
      .withSqlAnsi92SupportedLevel(ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL)
      .withSqlSupportsIntegrityEnhancementFacility(SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY)
      .withSqlSchemaTerm(SCHEMA_TERM)
      .withSqlCatalogTerm(CATALOG_TERM)
      .withSqlProcedureTerm(PROCEDURE_TERM)
      .withSqlCatalogAtStart(CATALOG_AT_START)
      .withSqlSchemasSupportedActions(SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS)
      .withSqlCatalogsSupportedActions(SQL_ELEMENT_IN_INDEX_DEFINITIONS)
      .withSqlSupportedPositionedCommands(SQL_POSITIONED_DELETE)
      .withSqlSelectForUpdateSupported(SELECT_FOR_UPDATE_SUPPORTED)
      .withSqlStoredProceduresSupported(STORED_PROCEDURES_SUPPORTED)
      .withSqlSubQueriesSupported(SUPPORTED_SUBQUERIES)
      .withSqlCorrelatedSubqueriesSupported(CORRELATED_SUBQUERIES_SUPPORTED)
      .withSqlSupportedUnions(SQL_UNION_ALL)
      .withSqlMaxBinaryLiteralLength(MAX_BINARY_LITERAL_LENGTH)
      .withSqlMaxCharLiteralLength(MAX_CHAR_LITERAL_LENGTH)
      .withSqlMaxColumnNameLength(MAX_COLUMN_NAME_LENGTH)
      .withSqlMaxColumnsInGroupBy(MAX_COLUMNS_IN_GROUP_BY)
      .withSqlMaxColumnsInIndex(MAX_COLUMNS_IN_INDEX)
      .withSqlMaxColumnsInOrderBy(MAX_COLUMNS_IN_ORDER_BY)
      .withSqlMaxColumnsInSelect(MAX_COLUMNS_IN_SELECT)
      .withSqlMaxConnections(MAX_CONNECTIONS)
      .withSqlMaxCursorNameLength(MAX_CURSOR_NAME_LENGTH)
      .withSqlMaxIndexLength(MAX_INDEX_LENGTH)
      .withSqlSchemaNameLength(SCHEMA_NAME_LENGTH)
      .withSqlMaxProcedureNameLength(MAX_PROCEDURE_NAME_LENGTH)
      .withSqlMaxCatalogNameLength(MAX_CATALOG_NAME_LENGTH)
      .withSqlMaxRowSize(MAX_ROW_SIZE)
      .withSqlMaxRowSizeIncludesBlobs(MAX_ROW_SIZE_INCLUDES_BLOBS)
      .withSqlMaxStatementLength(MAX_STATEMENT_LENGTH)
      .withSqlMaxStatements(MAX_STATEMENTS)
      .withSqlMaxTableNameLength(MAX_TABLE_NAME_LENGTH)
      .withSqlMaxTablesInSelect(MAX_TABLES_IN_SELECT)
      .withSqlMaxUsernameLength(MAX_USERNAME_LENGTH)
      .withSqlDefaultTransactionIsolation(DEFAULT_TRANSACTION_ISOLATION)
      .withSqlTransactionsSupported(TRANSACTIONS_SUPPORTED)
      .withSqlSupportedTransactionsIsolationLevels(SQL_TRANSACTION_SERIALIZABLE, SQL_TRANSACTION_READ_COMMITTED)
      .withSqlDataDefinitionCausesTransactionCommit(DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT)
      .withSqlDataDefinitionsInTransactionsIgnored(DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED)
      .withSqlSupportedResultSetTypes(SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE)
      .withSqlBatchUpdatesSupported(BATCH_UPDATES_SUPPORTED)
      .withSqlSavepointsSupported(SAVEPOINTS_SUPPORTED)
      .withSqlNamedParametersSupported(NAMED_PARAMETERS_SUPPORTED)
      .withSqlLocatorsUpdateCopy(LOCATORS_UPDATE_COPY)
      .withSqlStoredFunctionsUsingCallSyntaxSupported(STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED)
      .withSqlSupportsConvert(Collections.singletonMap(
        SQL_CONVERT_BIT_VALUE, Arrays.asList(SQL_CONVERT_INTEGER_VALUE, SQL_CONVERT_BIGINT_VALUE)));
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
    sqlInfoBuilder.send(commandGetSqlInfo.getInfoList(), serverStreamListener);
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
    final Schema schema = Schemas.GET_EXPORTED_KEYS_SCHEMA;
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
    final Schema schema = Schemas.GET_IMPORTED_KEYS_SCHEMA;
    return new FlightInfo(schema, flightDescriptor, Collections.emptyList(), -1, -1);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(
    FlightSql.CommandGetCrossReference commandGetCrossReference,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    final Schema schema = Schemas.GET_CROSS_REFERENCE_SCHEMA;
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
  public void getStreamCrossReference(
    FlightSql.CommandGetCrossReference commandGetCrossReference,
    CallContext callContext, ServerStreamListener serverStreamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("CommandGetCrossReference not supported.").toRuntimeException();
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
