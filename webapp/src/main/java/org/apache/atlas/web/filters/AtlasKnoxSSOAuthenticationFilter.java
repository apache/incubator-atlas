
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.atlas.web.filters;

import com.google.inject.Inject;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.web.security.AtlasAuthenticationProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Date;
import java.util.List;


public class AtlasKnoxSSOAuthenticationFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasKnoxSSOAuthenticationFilter.class);

    public static final String BROWSER_USERAGENT = "atlas.sso.knox.browser.useragent";
    public static final String JWT_AUTH_PROVIDER_URL = "atlas.sso.knox.providerurl";
    public static final String JWT_PUBLIC_KEY = "atlas.sso.knox.publicKey";
    public static final String JWT_COOKIE_NAME = "atlas.sso.knox.cookiename";
    public static final String JWT_ORIGINAL_URL_QUERY_PARAM = "atlas.sso.knox.query.param.originalurl";
    public static final String JWT_COOKIE_NAME_DEFAULT = "hadoop-jwt";
    public static final String JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT = "originalUrl";

    private SSOAuthenticationProperties jwtProperties;

    private String originalUrlQueryParam = "originalUrl";
    private String authenticationProviderUrl = null;
    private RSAPublicKey publicKey = null;
    private String cookieName = "hadoop-jwt";
    private Configuration configuration = null;
    private boolean ssoEnabled = false;
    private JWSVerifier verifier = null;

    @Inject
    public AtlasKnoxSSOAuthenticationFilter() {
        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.error("Error while getting application properties", e);
        }
        ssoEnabled = configuration.getBoolean("atlas.sso.knox.enabled", false);
        jwtProperties = loadJwtProperties();
        setJwtProperties();
    }

    public AtlasKnoxSSOAuthenticationFilter(
            SSOAuthenticationProperties jwtProperties) {
        this.jwtProperties = jwtProperties;
        setJwtProperties();
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    /*
     * doFilter of AtlasKnoxSSOAuthenticationFilter is the first in the filter list so in this it check for the request
     * if the request is from browser and sso is enabled then it process the request against knox sso
     * else if it's ssoenable and the request is with local login string then it show's the appropriate msg
     * else if ssoenable is false then it contiunes with further filters as it was before sso
     */
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {

        if (!ssoEnabled) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Knox doFilter {}", httpRequest.getRequestURI());
        }

        if (httpRequest.getSession() != null && httpRequest.getSession().getAttribute("locallogin") != null) {
            servletRequest.setAttribute("ssoEnabled", false);
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if (!isWebUserAgent(httpRequest.getHeader("User-Agent")) || jwtProperties == null || isAuthenticated()) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Knox ssoEnabled  {} {}", ssoEnabled, httpRequest.getRequestURI());
        }
        //if jwt properties are loaded and is current not authenticated then it will go for sso authentication
        //Note : Need to remove !isAuthenticated() after knoxsso solve the bug from cross-origin script
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        String serializedJWT = getJWTFromCookie(httpRequest);
        // if we get the hadoop-jwt token from the cookies then will process it further
        if (serializedJWT != null) {
            SignedJWT jwtToken = null;
            try {
                jwtToken = SignedJWT.parse(serializedJWT);
                boolean valid = validateToken(jwtToken);
                //if the public key provide is correct and also token is not expired the process token
                if (valid) {
                    String userName = jwtToken.getJWTClaimsSet().getSubject();
                    LOG.info("SSO login user : {} ", userName);
                    //if we get the userName from the token then log into atlas using the same user
                    if (userName != null && !userName.trim().isEmpty()) {
                        List<GrantedAuthority> grantedAuths = AtlasAuthenticationProvider.getAuthoritiesFromUGI(userName);
                        final UserDetails principal = new User(userName, "", grantedAuths);
                        final Authentication finalAuthentication = new UsernamePasswordAuthenticationToken(principal, "", grantedAuths);
                        WebAuthenticationDetails webDetails = new WebAuthenticationDetails(httpRequest);
                        ((AbstractAuthenticationToken) finalAuthentication).setDetails(webDetails);
                        AtlasAuthenticationProvider authenticationProvider = new AtlasAuthenticationProvider();
                        authenticationProvider.setSsoEnabled(ssoEnabled);
                        Authentication authentication = authenticationProvider.authenticate(finalAuthentication);
                        SecurityContextHolder.getContext().setAuthentication(authentication);
                    }

                    filterChain.doFilter(servletRequest, httpServletResponse);
                } else {  // if the token is not valid then redirect to knox sso
                    String ssourl = constructLoginURL(httpRequest);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SSO URL ={} invalid", ssourl);
                    }
                    httpServletResponse.sendRedirect(ssourl);
                }
            } catch (ParseException e) {
                LOG.warn("Unable to parse the JWT token", e);
            }
        } else {
            String ssourl = constructLoginURL(httpRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug("SSO URL = {}  serializedJWT null", ssourl);
            }
            httpServletResponse.sendRedirect(ssourl);
        }

    }


    private boolean isWebUserAgent(String userAgent) {
        boolean isWeb = false;
        if (jwtProperties != null) {
            String userAgentList[] = jwtProperties.getUserAgentList();
            if (userAgentList != null && userAgentList.length > 0) {
                for (String ua : userAgentList) {
                    if (StringUtils.startsWithIgnoreCase(userAgent, ua)) {
                        isWeb = true;
                        break;
                    }
                }
            }
        }
        return isWeb;
    }


    private void setJwtProperties() {
        if (jwtProperties != null) {
            authenticationProviderUrl = jwtProperties.getAuthenticationProviderUrl();
            publicKey = jwtProperties.getPublicKey();
            cookieName = jwtProperties.getCookieName();
            originalUrlQueryParam = jwtProperties.getOriginalUrlQueryParam();
            if (publicKey != null) {
                verifier = new RSASSAVerifier(publicKey);
            }
        }
    }

    /**
     * Do not try to validate JWT if user already authenticated via other
     * provider
     *
     * @return true, if JWT validation required
     */
    private boolean isAuthenticated() {
        Authentication existingAuth = SecurityContextHolder.getContext().getAuthentication();
        return !(!(existingAuth != null && existingAuth.isAuthenticated()) || existingAuth instanceof SSOAuthentication);
    }

    /**
     * Encapsulate the acquisition of the JWT token from HTTP cookies within the
     * request.
     *
     * @param req servlet request to get the JWT token from
     * @return serialized JWT token
     */
    protected String getJWTFromCookie(HttpServletRequest req) {
        String serializedJWT = null;
        Cookie[] cookies = req.getCookies();
        if (cookieName != null && cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookieName.equals(cookie.getName())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} cookie has been found and is being processed", cookieName);
                    }
                    serializedJWT = cookie.getValue();
                    break;
                }
            }
        }
        return serializedJWT;
    }

    /**
     * Create the URL to be used for authentication of the user in the absence
     * of a JWT token within the incoming request.
     *
     * @param request for getting the original request URL
     * @return url to use as login url for redirect
     */
    protected String constructLoginURL(HttpServletRequest request) {
        String delimiter = "?";
        if (authenticationProviderUrl.contains("?")) {
            delimiter = "&";
        }
        StringBuilder loginURL = new StringBuilder();
        loginURL.append(authenticationProviderUrl).append(delimiter).append(originalUrlQueryParam).append("=").append(request.getRequestURL().append(getOriginalQueryString(request)));
        return loginURL.toString();
    }

    private String getOriginalQueryString(HttpServletRequest request) {
        String originalQueryString = request.getQueryString();
        return (originalQueryString == null) ? "" : "?" + originalQueryString;
    }

    /**
     * This method provides a single method for validating the JWT for use in
     * request processing. It provides for the override of specific aspects of
     * this implementation through submethods used within but also allows for
     * the override of the entire token validation algorithm.
     *
     * @param jwtToken the token to validate
     * @return true if valid
     */
    protected boolean validateToken(SignedJWT jwtToken) {
        boolean isValid = validateSignature(jwtToken);

        if (isValid) {
            isValid = validateExpiration(jwtToken);
            if (!isValid) {
                LOG.warn("Expiration time validation of JWT token failed.");
            }
        } else {
            LOG.warn("Signature of JWT token could not be verified. Please check the public key");
        }
        return isValid;
    }

    /**
     * Verify the signature of the JWT token in this method. This method depends
     * on the public key that was established during init based upon the
     * provisioned public key. Override this method in subclasses in order to
     * customize the signature verification behavior.
     *
     * @param jwtToken the token that contains the signature to be validated
     * @return valid true if signature verifies successfully; false otherwise
     */
    protected boolean validateSignature(SignedJWT jwtToken) {
        boolean valid = false;
        if (JWSObject.State.SIGNED == jwtToken.getState()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SSO token is in a SIGNED state");
            }
            if (jwtToken.getSignature() != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSO token signature is not null");
                }
                try {
                    if (verifier != null && jwtToken.verify(verifier)) {
                        valid = true;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("SSO token has been successfully verified");
                        }
                    } else {
                        LOG.warn("SSO signature verification failed.Please check the public key");
                    }
                } catch (JOSEException je) {
                    LOG.warn("Error while validating signature", je);
                } catch (Exception e) {
                    LOG.warn("Error while validating signature", e);
                }
            }
        }
        return valid;
    }

    /**
     * Validate that the expiration time of the JWT token has not been violated.
     * If it has then throw an AuthenticationException. Override this method in
     * subclasses in order to customize the expiration validation behavior.
     *
     * @param jwtToken the token that contains the expiration date to validate
     * @return valid true if the token has not expired; false otherwise
     */
    protected boolean validateExpiration(SignedJWT jwtToken) {
        boolean valid = false;
        try {
            Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
            if (expires == null || new Date().before(expires)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("SSO token expiration date has been successfully validated");
                }
                valid = true;
            } else {
                LOG.warn("SSO expiration date validation failed.");
            }
        } catch (ParseException pe) {
            LOG.warn("SSO expiration date validation failed.", pe);
        }
        return valid;
    }

    @Override
    public void destroy() {
    }

    public SSOAuthenticationProperties loadJwtProperties() {
        String providerUrl = configuration.getString(JWT_AUTH_PROVIDER_URL);
        if (providerUrl != null && configuration.getBoolean("atlas.sso.knox.enabled", false)) {
            SSOAuthenticationProperties jwtProperties = new SSOAuthenticationProperties();
            String publicKeyPathStr = configuration.getString(JWT_PUBLIC_KEY);
            if (publicKeyPathStr == null) {
                LOG.error("Public key pem not specified for SSO auth provider {}. SSO auth will be disabled");
                return null;
            }
            jwtProperties.setAuthenticationProviderUrl(providerUrl);
            jwtProperties.setCookieName(configuration.getString(JWT_COOKIE_NAME, JWT_COOKIE_NAME_DEFAULT));
            jwtProperties.setOriginalUrlQueryParam(configuration.getString(JWT_ORIGINAL_URL_QUERY_PARAM, JWT_ORIGINAL_URL_QUERY_PARAM_DEFAULT));
            String userAgent = configuration.getString(BROWSER_USERAGENT);
            if (userAgent != null && !userAgent.isEmpty()) {
                jwtProperties.setUserAgentList(userAgent.split(","));
            }
            try {
                RSAPublicKey publicKey = parseRSAPublicKey(publicKeyPathStr);
                jwtProperties.setPublicKey(publicKey);
            } catch (IOException e) {
                LOG.error("Unable to read public certificate file. JWT auth will be disabled.", e);
            } catch (CertificateException e) {
                LOG.error("Unable to parse public certificate file. JWT auth will be disabled.", e);
            } catch (ServletException e) {
                LOG.error("ServletException while processing the properties", e);
            }
            return jwtProperties;
        } else {
            return null;
        }
    }

	/*
     * public static RSAPublicKey getPublicKeyFromFile(String filePath) throws
	 * IOException, CertificateException {
	 * FileUtils.readFileToString(new File(filePath));
	 * getPublicKeyFromString(pemString); }
	 */

    public static RSAPublicKey parseRSAPublicKey(String pem)
            throws CertificateException, UnsupportedEncodingException,
            ServletException {
        String PEM_HEADER = "-----BEGIN CERTIFICATE-----\n";
        String PEM_FOOTER = "\n-----END CERTIFICATE-----";
        String fullPem = PEM_HEADER + pem + PEM_FOOTER;
        PublicKey key = null;
        try {
            CertificateFactory fact = CertificateFactory.getInstance("X.509");
            ByteArrayInputStream is = new ByteArrayInputStream(fullPem.getBytes("UTF8"));
            X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
            key = cer.getPublicKey();
        } catch (CertificateException ce) {
            String message = null;
            if (pem.startsWith(PEM_HEADER)) {
                message = "CertificateException - be sure not to include PEM header " + "and footer in the PEM configuration element.";
            } else {
                message = "CertificateException - PEM may be corrupt";
            }
            throw new ServletException(message, ce);
        } catch (UnsupportedEncodingException uee) {
            throw new ServletException(uee);
        }
        return (RSAPublicKey) key;
    }

}
