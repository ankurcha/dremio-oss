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
package com.dremio.exec.store.iceberg.nessie;

import java.util.Arrays;
import java.util.List;

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import com.dremio.common.exceptions.UserException;
import com.dremio.service.nessie.NessieConfig;
import com.dremio.service.nessieapi.Contents;
import com.dremio.service.nessieapi.ContentsKey;
import com.dremio.service.nessieapi.GetContentsRequest;
import com.dremio.service.nessieapi.GetReferenceByNameRequest;
import com.dremio.service.nessieapi.Reference;
import com.dremio.service.nessieapi.SetContentsRequest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Iceberg nessie table operations
 */
class IcebergNessieTableOperations extends BaseMetastoreTableOperations {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergNessieTableOperations.class);
    private final FileIO fileIO;
    private final NessieGrpcClient client;
    private final IcebergNessieTableIdentifier nessieTableIdentifier;
    private Reference reference;
    public IcebergNessieTableOperations(NessieGrpcClient client, FileIO fileIO, IcebergNessieTableIdentifier nessieTableIdentifier) {
        this.fileIO = fileIO;
        this.client = client;
        this.nessieTableIdentifier = nessieTableIdentifier;
        this.reference = null;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    protected void doRefresh() {
        this.reference = getBranchRef();
        String metadataLocation = null;
        try {
            Contents contents = client.getContentsApi().getContents(
                    GetContentsRequest.newBuilder()
                    .setRef(reference.getBranch().getName())
                    .setContentsKey(
                        ContentsKey.newBuilder().addAllElements(
                            getNessieKey(nessieTableIdentifier.getTableIdentifier())))
                    .build()
            );
            if (contents != null && contents.hasIcebergTable()) {
                metadataLocation = contents.getIcebergTable().getMetadataLocation();
            }
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw UserException.dataReadError(sre).buildSilently();
            }
        }
        refreshFromMetadataLocation(metadataLocation, 2);
    }

    private List<String> getNessieKey(TableIdentifier tableIdentifier) {
        return Arrays.asList(
                tableIdentifier.namespace().toString(),
                tableIdentifier.name());
    }

    private Reference getBranchRef() {
        return client.getTreeApi()
                .getReferenceByName(GetReferenceByNameRequest
                        .newBuilder()
                        .setRefName(NessieConfig.NESSIE_DEFAULT_BRANCH)
                        .build());
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

        boolean threw = true;
        try {
            client.getContentsApi().setContents(
                SetContentsRequest
                    .newBuilder()
                    .setBranch(reference.getBranch().getName())
                    .setHash(reference.getBranch().getHash())
                    .setMessage("Replaced message")
                    .setContentsKey(ContentsKey.newBuilder().addAllElements(
                            getNessieKey(nessieTableIdentifier.getTableIdentifier())))
                    .setContents(Contents.newBuilder()
                            .setType(Contents.Type.ICEBERG_TABLE)
                            .setIcebergTable(Contents.IcebergTable.newBuilder()
                                    .setMetadataLocation(newMetadataLocation).build()))
                    .build()
            );
            threw = false;
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == Status.Code.ABORTED) {
                throw new CommitFailedException(sre, "Commit failed: Reference hash is out of date. " +
                        "Update the reference %s and try again", reference.getBranch().getHash());
            } else {
                throw UserException.dataReadError(sre).buildSilently();
            }
        }
        finally {
            if (threw) {
                io().deleteFile(newMetadataLocation);
            }
        }
    }
}
