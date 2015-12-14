/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.throwingproviders.ThrowingProviderBinder;
import com.thinkaurelius.titan.core.TitanGraph;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.discovery.HiveLineageService;
import org.apache.atlas.discovery.LineageService;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.graph.TitanGraphProvider;
import org.apache.atlas.repository.typestore.GraphBackedTypeStore;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.services.DefaultMetadataService;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.types.TypeSystem;

/**
 * Guice module for Repository module.
 */
public class RepositoryMetadataModule extends com.google.inject.AbstractModule {

    @Override
    protected void configure() {
        // special wiring for Titan Graph



        ThrowingProviderBinder.create(binder()).bind(GraphProvider.class, TitanGraph.class).to(TitanGraphProvider.class)
                .asEagerSingleton();

        // allow for dynamic binding of the metadata repo & graph service
        // bind the MetadataRepositoryService interface to an implementation
        bind(MetadataRepository.class).to(GraphBackedMetadataRepository.class).asEagerSingleton();

        bind(TypeSystem.class).in(Singleton.class);

        // bind the ITypeStore interface to an implementation
        bind(ITypeStore.class).to(GraphBackedTypeStore.class).asEagerSingleton();

        Multibinder<TypesChangeListener> typesChangeListenerBinder =
                Multibinder.newSetBinder(binder(), TypesChangeListener.class);
        typesChangeListenerBinder.addBinding().to(GraphBackedSearchIndexer.class);

        // bind the MetadataService interface to an implementation
        bind(MetadataService.class).to(DefaultMetadataService.class).asEagerSingleton();

        // bind the DiscoveryService interface to an implementation
        bind(DiscoveryService.class).to(GraphBackedDiscoveryService.class).asEagerSingleton();

        bind(LineageService.class).to(HiveLineageService.class).asEagerSingleton();

        MethodInterceptor interceptor = new GraphTransactionInterceptor();
        requestInjection(interceptor);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(GraphTransaction.class), interceptor);
    }

}
