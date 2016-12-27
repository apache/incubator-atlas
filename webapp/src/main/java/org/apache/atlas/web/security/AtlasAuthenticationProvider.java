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

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;

@Component
public class AtlasAuthenticationProvider extends
        AtlasAbstractAuthenticationProvider {
    private static final Logger LOG = LoggerFactory
            .getLogger(AtlasAuthenticationProvider.class);

    private boolean fileAuthenticationMethodEnabled = true;
    private String ldapType = "NONE";
    public static final String FILE_AUTH_METHOD = "atlas.authentication.method.file";
    public static final String LDAP_AUTH_METHOD = "atlas.authentication.method.ldap";
    public static final String LDAP_TYPE = "atlas.authentication.method.ldap.type";



    private boolean ssoEnabled = false;

    @Autowired
    AtlasLdapAuthenticationProvider ldapAuthenticationProvider;

    @Autowired
    AtlasFileAuthenticationProvider fileAuthenticationProvider;

    @Autowired
    AtlasADAuthenticationProvider adAuthenticationProvider;

    @PostConstruct
    void setAuthenticationMethod() {
        try {
            Configuration configuration = ApplicationProperties.get();

            this.fileAuthenticationMethodEnabled = configuration.getBoolean(
                    FILE_AUTH_METHOD, true);

            boolean ldapAuthenticationEnabled = configuration.getBoolean(LDAP_AUTH_METHOD, false);

            if (ldapAuthenticationEnabled) {
                this.ldapType = configuration.getString(LDAP_TYPE, "NONE");
            } else {
                this.ldapType = "NONE";
            }
        } catch (Exception e) {
            LOG.error("Error while getting atlas.login.method application properties", e);
        }
    }

    @Override
    public Authentication authenticate(Authentication authentication)
            throws AuthenticationException {

        if(ssoEnabled){
            if (authentication != null){
                authentication = getSSOAuthentication(authentication);
                if(authentication!=null && authentication.isAuthenticated()){
                    return authentication;
                }
            }
        } else {

            if (ldapType.equalsIgnoreCase("LDAP")) {
                try {
                    authentication = ldapAuthenticationProvider.authenticate(authentication);
                } catch (Exception ex) {
                    LOG.error("Error while LDAP authentication", ex);
                }
            } else if (ldapType.equalsIgnoreCase("AD")) {
                try {
                    authentication = adAuthenticationProvider.authenticate(authentication);
                } catch (Exception ex) {
                    LOG.error("Error while AD authentication", ex);
                }
            }
        }

        if (authentication != null) {
            if (authentication.isAuthenticated()) {
                return authentication;
            } else if (fileAuthenticationMethodEnabled) {  // If the LDAP/AD authentication fails try the local filebased login method
                authentication = fileAuthenticationProvider.authenticate(authentication);

                if (authentication != null && authentication.isAuthenticated()) {
                    return authentication;
                }
            }
        }

        LOG.error("Authentication failed.");
        throw new AtlasAuthenticationException("Authentication failed.");
    }

    public boolean isSsoEnabled() {
        return ssoEnabled;
    }

    public void setSsoEnabled(boolean ssoEnabled) {
        this.ssoEnabled = ssoEnabled;
    }

    private Authentication getSSOAuthentication(Authentication authentication) throws AuthenticationException{
        return authentication;
    }
}
