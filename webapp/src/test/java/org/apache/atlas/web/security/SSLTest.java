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

package org.apache.atlas.web.security;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.web.service.SecureEmbeddedServer;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.atlas.security.SecurityProperties.KEYSTORE_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.SERVER_CERT_PASSWORD_KEY;
import static org.apache.atlas.security.SecurityProperties.TRUSTSTORE_PASSWORD_KEY;

public class SSLTest extends BaseSSLAndKerberosTest {
    private AtlasClient atlasClient;
    private Path jksPath;
    private String providerUrl;
    private TestSecureEmbeddedServer secureEmbeddedServer;
    private String originalConf;

    class TestSecureEmbeddedServer extends SecureEmbeddedServer {

        public TestSecureEmbeddedServer(int port, String path) throws IOException {
            super(port, path);
        }

        public Server getServer() {
            return server;
        }

        @Override
        protected WebAppContext getWebAppContext(String path) {
            WebAppContext application = new WebAppContext(path, "/");
            application.setDescriptor(System.getProperty("projectBaseDir") + "/webapp/src/test/webapp/WEB-INF/web.xml");
            application.setClassLoader(Thread.currentThread().getContextClassLoader());
            return application;
        }
    }

    @BeforeClass
    public void setUp() throws Exception {
        jksPath = new Path(Files.createTempDirectory("tempproviders").toString(), "test.jks");
        providerUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file/" + jksPath.toUri();

        setupCredentials();
        final PropertiesConfiguration configuration = getSSLConfiguration(providerUrl);
        String persistDir = writeConfiguration(configuration);
        persistSSLClientConfiguration((org.apache.commons.configuration.Configuration) configuration);

        originalConf = System.getProperty("atlas.conf");
        System.setProperty("atlas.conf", persistDir);

        atlasClient = new AtlasClient(new String[]{DGI_URL},new String[]{"admin","admin"}) {
            @Override
            protected PropertiesConfiguration getClientProperties() {
                return configuration;
            }
        };

        secureEmbeddedServer = new TestSecureEmbeddedServer(21443, getWarPath()) {
            @Override
            public PropertiesConfiguration getConfiguration() {
                return configuration;
            }
        };
        secureEmbeddedServer.getServer().start();
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (secureEmbeddedServer != null) {
            secureEmbeddedServer.getServer().stop();
        }

        if (originalConf != null) {
            System.setProperty("atlas.conf", originalConf);
        }
    }

    protected void setupCredentials() throws Exception {
        Configuration conf = new Configuration(false);

        File file = new File(jksPath.toUri().getPath());
        file.delete();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerUrl);

        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);

        // create new aliases
        try {

            char[] storepass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(KEYSTORE_PASSWORD_KEY, storepass);

            char[] trustpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(TRUSTSTORE_PASSWORD_KEY, trustpass);

            char[] trustpass2 = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry("ssl.client.truststore.password", trustpass2);

            char[] certpass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
            provider.createCredentialEntry(SERVER_CERT_PASSWORD_KEY, certpass);

            // write out so that it can be found in checks
            provider.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testService() throws Exception {
        atlasClient.listTypes();
   }
}
