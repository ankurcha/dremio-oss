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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.store.RecordReader.COL_IDS;
import static com.dremio.exec.store.RecordReader.DATAFILE_PATH;
import static com.dremio.exec.store.RecordReader.SPLIT_INFORMATION;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Arrays;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.service.namespace.file.proto.FileConfig;

public class DataProcessorTestUtil {

  static TableFunctionContext getTableFunctionContext(DataProcessorType datafileProcessorType) throws Exception {
    TableFunctionContext functionContext = mock(TableFunctionContext.class);
    BatchSchema batchSchema = getBatchSchema(datafileProcessorType);
    BatchSchema spyBatchSchema = spy(batchSchema);
    when(functionContext.getFullSchema()).thenReturn(spyBatchSchema);
    when(functionContext.getColumns()).thenReturn(Arrays.asList(SchemaPath.getSimplePath(SPLIT_INFORMATION),
      SchemaPath.getSimplePath(COL_IDS), SchemaPath.getSimplePath(DATAFILE_PATH)));
    when(functionContext.getTableSchema()).thenReturn(getBatchSchema(datafileProcessorType));
    FileConfig fc = new FileConfig();
    fc.setLocation("/test");
    when(functionContext.getFormatSettings()).thenReturn(fc);
    when(spyBatchSchema.maskAndReorder(anyList())).thenReturn(spyBatchSchema);
    return functionContext;
  }

  static DataFile getDatafile(String path, long size) {
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath(path)
      .withFileSizeInBytes(size)
      .withRecordCount(9)
      .build();
    return dataFile;
  }

  static VarCharVector getDataFileVec(VectorAccessible vectorWrappers) {
    TypedFieldId typedFieldId = vectorWrappers.getSchema().getFieldId(SchemaPath.getSimplePath(DATAFILE_PATH));
    Field field = vectorWrappers.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    return (VarCharVector) vectorWrappers.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
  }

  static VarBinaryVector getSplitVec(VectorAccessible vectorWrappers) {
    TypedFieldId typedFieldId = vectorWrappers.getSchema().getFieldId(SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION));
    Field field = vectorWrappers.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    return (VarBinaryVector) vectorWrappers.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
  }

  static String extractDataFilePath(VarCharVector datafileV, int idx) throws IOException, ClassNotFoundException {
    return datafileV.getObject(idx).toString();
  }

  static BatchSchema getOutputSchemaForSplitGen() {
    return RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;
  }

  static BatchSchema getOutputSchemaForDataPathGen() {
    return RecordReader.MANIFEST_SCAN_TABLE_FUNCTION_SCHEMA;
  }

  static BatchSchema getBatchSchema(DataProcessorType type) throws Exception {
    switch (type) {
      case SPLIT_GEN:
        return getOutputSchemaForSplitGen();
      case DATAFILE_PATH_GEN:
        return getOutputSchemaForDataPathGen();
      default:
        throw new Exception("Not a valid type");
    }
  }

  static SplitAndPartitionInfo extractSplit(VarBinaryVector splits, int idx) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(splits.get(idx));
         ObjectInput in = new ObjectInputStream(bis)) {
      return (SplitAndPartitionInfo) in.readObject();
    }
  }

  enum DataProcessorType {
    SPLIT_GEN,
    DATAFILE_PATH_GEN
  }

}
