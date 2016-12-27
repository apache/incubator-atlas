/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.typesystem.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class GraphTransactionInterceptor implements MethodInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(GraphTransactionInterceptor.class);

    private static final ThreadLocal<List<PostTransactionHook>> postTransactionHooks = new ThreadLocal<>();

    private AtlasGraph graph;

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        
        if (graph == null) {
            graph = AtlasGraphProvider.getGraphInstance();
        }

        boolean isSuccess = false;

        try {
            try {
                Object response = invocation.proceed();
                graph.commit();
                isSuccess = true;
                LOG.info("graph commit");
                return response;
            } catch (Throwable t) {
                if (logException(t)) {
                    LOG.error("graph rollback due to exception ", t);
                } else {
                    LOG.error("graph rollback due to exception {}:{}", t.getClass().getSimpleName(), t.getMessage());
                }
                graph.rollback();
                throw t;
            }
        } finally {
            List<PostTransactionHook> trxHooks = postTransactionHooks.get();

            if (trxHooks != null) {
                postTransactionHooks.remove();

                for (PostTransactionHook trxHook : trxHooks) {
                    try {
                        trxHook.onComplete(isSuccess);
                    } catch (Throwable t) {
                        LOG.error("postTransactionHook failed", t);
                    }
                }
            }
        }
    }

    boolean logException(Throwable t) {
        return !(t instanceof NotFoundException) &&
                ((t instanceof AtlasBaseException) &&
                        ((AtlasBaseException) t).getAtlasErrorCode().getHttpCode() != Response.Status.NOT_FOUND);
    }

    public static abstract class PostTransactionHook {
        protected PostTransactionHook() {
            List<PostTransactionHook> trxHooks = postTransactionHooks.get();

            if (trxHooks == null) {
                trxHooks = new ArrayList<>();
                postTransactionHooks.set(trxHooks);
            }

            trxHooks.add(this);
        }

        public abstract void onComplete(boolean isSuccess);
    }
}
