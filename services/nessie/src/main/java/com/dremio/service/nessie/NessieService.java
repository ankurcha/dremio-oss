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
package com.dremio.service.nessie;

import java.util.List;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.rest.ConfigResource;
import org.projectnessie.services.rest.ContentsResource;
import org.projectnessie.services.rest.TreeResource;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.memory.InMemoryVersionStore;

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.Service;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;

import io.grpc.BindableService;

/**
 * Class that embeds Nessie into Dremio coordinator.
 */
public class NessieService implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NessieService.class);

  private final Provider<KVStoreProvider> kvStoreProvider;
  private final NessieConfig serverConfig;
  private final Supplier<VersionStore<Contents, CommitMeta>> versionStoreSupplier;
  private final TreeApiService treeApiService;
  private final ContentsApiService contentsApiService;
  private final ConfigApiService configApiService;
  private final int kvStoreMaxCommitRetries;

  public NessieService(Provider<KVStoreProvider> kvStoreProvider,
                       boolean inMemoryBackend,
                       int kvStoreMaxCommitRetries) {
    this.kvStoreProvider = kvStoreProvider;
    this.serverConfig = new NessieConfig();
    this.kvStoreMaxCommitRetries = kvStoreMaxCommitRetries;

    this.versionStoreSupplier = Suppliers.memoize(() -> getVersionStore(inMemoryBackend));
    this.treeApiService = new TreeApiService(Suppliers.memoize(() -> new TreeResource(serverConfig, null, versionStoreSupplier.get())));
    this.contentsApiService = new ContentsApiService(Suppliers.memoize(() ->
        new ContentsResource(serverConfig, null, versionStoreSupplier.get()))
    );
    this.configApiService = new ConfigApiService(Suppliers.memoize(() -> new ConfigResource(serverConfig)));
  }

  public List<BindableService> getGrpcServices() {
    return Lists.newArrayList(treeApiService, contentsApiService, configApiService);
  }

  @Override
  @SuppressWarnings("ReturnValueIgnored")
  public void start() throws Exception {
    logger.info("Starting Nessie gRPC Services.");

    // Get the version store here so it is ready before the first request is received
    versionStoreSupplier.get();

    logger.info("Started Nessie gRPC Services.");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping Nessie gRPC Service: Nothing to do");
  }

  private final VersionStore<Contents, CommitMeta> getVersionStore(boolean inMemoryBackend) {
    final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    if (inMemoryBackend) {
      return getInMemoryVersionStore(worker);
    } else {
      return getPersistingVersionStore(worker);
    }
  }

  private final VersionStore<Contents, CommitMeta> getPersistingVersionStore(TableCommitMetaStoreWorker worker) {
    return NessieKVVersionStore.builder()
      .metadataSerializer(worker.getMetadataSerializer())
      .valueSerializer(worker.getValueSerializer())
      .namedReferences(kvStoreProvider.get().getStore(NessieRefKVStoreBuilder.class))
      .commits(kvStoreProvider.get().getStore(NessieCommitKVStoreBuilder.class))
      .defaultBranchName(serverConfig.getDefaultBranch())
      .maxCommitRetries(kvStoreMaxCommitRetries)
      .build();
  }

  private final VersionStore<Contents, CommitMeta> getInMemoryVersionStore(TableCommitMetaStoreWorker worker) {
    return InMemoryVersionStore.<Contents, CommitMeta>builder()
      .metadataSerializer(worker.getMetadataSerializer())
      .valueSerializer(worker.getValueSerializer())
      .build();
  }

}
