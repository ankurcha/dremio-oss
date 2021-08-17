package com.dremio.service.flight.impl;

import java.util.stream.IntStream;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;

public class GetTableTypesResponseHandler implements UserResponseHandler {

  private final BufferAllocator allocator;
  private final FlightProducer.ServerStreamListener listener;

  public GetTableTypesResponseHandler(BufferAllocator allocator, FlightProducer.ServerStreamListener listener) {
    this.allocator = allocator;
    this.listener = listener;
  }

  @Override
  public void sendData(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener,
                       QueryWritableBatch result) {
  }

  @Override
  public void completed(UserResult result) {
    final UserProtos.GetTablesTypesResp tableTypesResp = result.unwrap(UserProtos.GetTablesTypesResp.class);

    try (VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA,
      allocator)) {
      listener.start(root);

      root.allocateNew();

      final VarCharVector tableTypeVector = (VarCharVector) root.getVector("table_type");

      final int tableTypesCount = tableTypesResp.getTableTypesCount();
      final IntStream range = IntStream.range(0, tableTypesCount);

      range.forEach(i -> {
        final UserProtos.TableTypesMetadata tableTypes = tableTypesResp.getTableTypes(i);;
        tableTypeVector.setSafe(i, new Text(tableTypes.getTableType()));
      });

      root.setRowCount(tableTypesCount);
      listener.putNext();
      listener.completed();
    }
  }
}
