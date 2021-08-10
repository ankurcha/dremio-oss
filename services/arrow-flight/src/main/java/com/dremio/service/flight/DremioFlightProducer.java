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

import static org.apache.arrow.flight.sql.impl.FlightSql.*;

import javax.inject.Provider;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightPreparedStatement;
import com.dremio.service.flight.impl.FlightWorkManager;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A FlightProducer implementation which exposes Dremio's catalog and produces results from SQL queries.
 */
public class DremioFlightProducer implements FlightSqlProducer {
  private final FlightWorkManager flightWorkManager;
  private final Location location;
  private final DremioFlightSessionsManager sessionsManager;
  private final BufferAllocator allocator;

  public DremioFlightProducer(Location location, DremioFlightSessionsManager sessionsManager,
                              Provider<UserWorker> workerProvider, Provider<OptionManager> optionManagerProvider,
                              BufferAllocator allocator, RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) {
    this.location = location;
    this.sessionsManager = sessionsManager;
    this.allocator = allocator;

    flightWorkManager = new FlightWorkManager(workerProvider, optionManagerProvider, runQueryResponseHandlerFactory);
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    try {
      final CallHeaders headers = retrieveHeadersFromCallContext(callContext);
      final UserSession session = sessionsManager.getUserSession(callContext.peerIdentity(), headers);
      final TicketContent.PreparedStatementTicket preparedStatementTicket = TicketContent.PreparedStatementTicket.parseFrom(ticket.getBytes());

      flightWorkManager.runPreparedStatement(preparedStatementTicket, serverStreamListener, allocator, session);
    } catch (InvalidProtocolBufferException ex) {
      final RuntimeException error = CallStatus.INVALID_ARGUMENT.withCause(ex).withDescription("Invalid ticket used in getStream").toRuntimeException();
      serverStreamListener.error(error);
      throw error;
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listFlights is unimplemented").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    return FlightSqlProducer.super.getFlightInfo(context, descriptor);
  }

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    return FlightSqlProducer.super.getSchema(context, descriptor);
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("acceptPut is unimplemented").toRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("doAction is unimplemented").toRuntimeException();
  }

  @Override
  public void createPreparedStatement(
    ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {

  }

  @Override
  public void closePreparedStatement(
    ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
    CallContext callContext,
    StreamListener<Result> streamListener) {

  }

  @Override
  public FlightInfo getFlightInfoStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaStatement(
    CommandStatementQuery commandStatementQuery,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public void getStreamStatement(CommandStatementQuery commandStatementQuery,
                                 CallContext callContext, Ticket ticket,
                                 ServerStreamListener serverStreamListener) {

  }

  @Override
  public void getStreamPreparedStatement(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {

  }

  @Override
  public Runnable acceptPutStatement(
    CommandStatementUpdate commandStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    return null;
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
    CommandPreparedStatementUpdate commandPreparedStatementUpdate,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    return null;
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
    CommandPreparedStatementQuery commandPreparedStatementQuery,
    CallContext callContext, FlightStream flightStream,
    StreamListener<PutResult> streamListener) {
    return null;
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaSqlInfo() {
    return FlightSqlProducer.super.getSchemaSqlInfo();
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo commandGetSqlInfo,
                               CallContext callContext, Ticket ticket,
                               ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoCatalogs(
    CommandGetCatalogs commandGetCatalogs, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaCatalogs() {
    return FlightSqlProducer.super.getSchemaCatalogs();
  }

  @Override
  public void getStreamCatalogs(CallContext callContext, Ticket ticket,
                                ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetSchemas commandGetSchemas,
                                         CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaSchemas() {
    return FlightSqlProducer.super.getSchemaSchemas();
  }

  @Override
  public void getStreamSchemas(CommandGetSchemas commandGetSchemas,
                               CallContext callContext, Ticket ticket,
                               ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables commandGetTables,
                                        CallContext callContext,
                                        FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaTables() {
    return FlightSqlProducer.super.getSchemaTables();
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables,
                              CallContext callContext, Ticket ticket,
                              ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoTableTypes(
    CommandGetTableTypes commandGetTableTypes, CallContext callContext,
    FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaTableTypes() {
    return FlightSqlProducer.super.getSchemaTableTypes();
  }

  @Override
  public void getStreamTableTypes(CallContext callContext, Ticket ticket,
                                  ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(
    CommandGetPrimaryKeys commandGetPrimaryKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaPrimaryKeys() {
    return FlightSqlProducer.super.getSchemaPrimaryKeys();
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys,
                                   CallContext callContext, Ticket ticket,
                                   ServerStreamListener serverStreamListener) {

  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, FlightDescriptor flightDescriptor) {
    return null;
  }

  @Override
  public SchemaResult getSchemaForImportedAndExportedKeys() {
    return FlightSqlProducer.super.getSchemaForImportedAndExportedKeys();
  }

  @Override
  public void getStreamExportedKeys(
    CommandGetExportedKeys commandGetExportedKeys,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {

  }

  @Override
  public void getStreamImportedKeys(
    CommandGetImportedKeys commandGetImportedKeys,
    CallContext callContext, Ticket ticket,
    ServerStreamListener serverStreamListener) {

  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("listActions is unimplemented").toRuntimeException();
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

  @Override
  public void close() throws Exception {

  }
}
