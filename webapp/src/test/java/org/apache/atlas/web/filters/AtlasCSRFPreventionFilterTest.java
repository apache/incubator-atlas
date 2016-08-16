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
package org.apache.atlas.web.filters;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

public class AtlasCSRFPreventionFilterTest {
	private static final String EXPECTED_MESSAGE = "Missing Required Header for CSRF Vulnerability Protection";
	private static final String X_CUSTOM_HEADER = "X-CUSTOM_HEADER";
	private String userAgent = "Mozilla";
	
	@Test
	public void testNoHeaderDefaultConfig_badRequest() throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn(null);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);		

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		verify(mockRes, atLeastOnce()).sendError(HttpServletResponse.SC_BAD_REQUEST, EXPECTED_MESSAGE);
		Mockito.verifyZeroInteractions(mockChain);
	}
	
	@Test
	public void testHeaderPresentDefaultConfig_goodRequest() throws ServletException, IOException {
		// CSRF HAS been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn("valueUnimportant");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testHeaderPresentCustomHeaderConfig_goodRequest() throws ServletException, IOException {
		// CSRF HAS been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(X_CUSTOM_HEADER)).thenReturn("valueUnimportant");

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testMissingHeaderWithCustomHeaderConfig_badRequest() throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(X_CUSTOM_HEADER)).thenReturn(null);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verifyZeroInteractions(mockChain);
	}

	@Test
	public void testMissingHeaderIgnoreGETMethodConfig_goodRequest()
			throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn(null);
		Mockito.when(mockReq.getMethod()).thenReturn("GET");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testMissingHeaderMultipleIgnoreMethodsConfig_badRequest()
			throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT))
				.thenReturn(null);
		Mockito.when(mockReq.getMethod()).thenReturn("PUT");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verifyZeroInteractions(mockChain);
	}
}
