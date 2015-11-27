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
import org.apache.atlas.AtlasException;
import org.apache.atlas.web.TestUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

public class SSLAndKerberosTest extends BaseSSLAndKerberosTest {
    public static final String TEST_USER_JAAS_SECTION = "TestUser";
    public static final String TESTUSER = "testuser";
    public static final String TESTPASS = "testpass";

    private static final String DGI_URL = "https://localhost:21443/";
    private AtlasClient dgiCLient;
    private TestSecureEmbeddedServer secureEmbeddedServer;
    private Subject subject;
    private String originalConf;

    @BeforeClass
    public void setUp() throws Exception {
        jksPath = new Path(Files.createTempDirectory("tempproviders").toString(), "test.jks");
        providerUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file/" + jksPath.toUri();

        String persistDir = TestUtils.getTempDirectory();

        setupKDCAndPrincipals();
        setupCredentials();

        // client will actually only leverage subset of these properties
        final PropertiesConfiguration configuration = getSSLConfiguration(providerUrl);
        configuration.setProperty("atlas.http.authentication.type", "kerberos");
        TestUtils.writeConfiguration(configuration, persistDir + File.separator + "client.properties");

        String confLocation = System.getProperty("atlas.conf");
        URL url;
        if (confLocation == null) {
            url = SSLAndKerberosTest.class.getResource("/application.properties");
        } else {
            url = new File(confLocation, "application.properties").toURI().toURL();
        }
        configuration.load(url);
        configuration.setProperty(TLS_ENABLED, true);
        configuration.setProperty("atlas.http.authentication.enabled", "true");
        configuration.setProperty("atlas.http.authentication.kerberos.principal", "HTTP/localhost@" + kdc.getRealm());
        configuration.setProperty("atlas.http.authentication.kerberos.keytab", httpKeytabFile.getAbsolutePath());
        configuration.setProperty("atlas.http.authentication.kerberos.name.rules",
                "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//\nDEFAULT");

        TestUtils.writeConfiguration(configuration, persistDir + File.separator + "application.properties");

        subject = loginTestUser();
        UserGroupInformation.loginUserFromSubject(subject);
        UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
            "testUser",
            UserGroupInformation.getLoginUser());

        dgiCLient = proxyUser.doAs(new PrivilegedExceptionAction<AtlasClient>() {
            @Override
            public AtlasClient run() throws Exception {
                return new AtlasClient(DGI_URL) {
                    @Override
                    protected PropertiesConfiguration getClientProperties() throws AtlasException {
                        return configuration;
                    }
                };
            }
        });

        // save original setting
        originalConf = System.getProperty("atlas.conf");
        System.setProperty("atlas.conf", persistDir);
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

        if (kdc != null) {
            kdc.stop();
        }

        if (originalConf != null) {
            System.setProperty("atlas.conf", originalConf);
        }
    }

    protected Subject loginTestUser() throws LoginException, IOException {
        LoginContext lc = new LoginContext(TEST_USER_JAAS_SECTION, new CallbackHandler() {

            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (int i = 0; i < callbacks.length; i++) {
                    if (callbacks[i] instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback) callbacks[i];
                        passwordCallback.setPassword(TESTPASS.toCharArray());
                    }
                    if (callbacks[i] instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback) callbacks[i];
                        nameCallback.setName(TESTUSER);
                    }
                }
            }
        });
        // attempt authentication
        lc.login();
        return lc.getSubject();
    }

    @Test
    public void testService() throws Exception {
        dgiCLient.listTypes();
    }

}
