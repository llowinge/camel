/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.olingo4;

import java.io.IOException;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.recording.RecordingStatus;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public abstract class AbstractOlingo4WireMockTestSupport extends CamelTestSupport {

    protected static final String ODATA_API_BASE_URL = "https://services.odata.org/TripPinRESTierService";

    protected final static WireMockServer wireMockServer
            = new WireMockServer(wireMockConfig().dynamicPort().notifier(new ConsoleNotifier(true)));

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOlingo4WireMockTestSupport.class);

    @BeforeAll
    public static void startWireMockServer() {
        LOG.info("Starting WireMock server");
        wireMockServer.start();
        wireMockServer.startRecording(recordSpec()
                .forTarget(ODATA_API_BASE_URL)
                .allowNonProxied(false));
        wireMockServer.stubFor(post(urlPathMatching(".*\\/\\$batch"))
                .withHeader("Content-Type", matching("multipart/mixed;boundary=.*"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type",
                                "multipart/mixed; boundary=batchresponse_adc42b60-d5a8-4699-8d4b-064c5e1824b1")
                        .withHeader("Cache-Control", "no-cache")
                        .withHeader("OData-Version", "4.0")
                        .withBodyFile("batch-response-file.txt")));
    }

    @AfterAll
    public static void stopWireMockServer() {
        if (wireMockServer.getRecordingStatus().getStatus().equals(RecordingStatus.Recording)) {
            wireMockServer.stopRecording();
        }
        wireMockServer.stop();
    }

    public String getTestServiceBaseUrl() {
        //return ODATA_API_BASE_URL;
        return "http://localhost:" + wireMockServer.port();
    }

    /*
     * Every request to the demo OData 4.0
     * (http://services.odata.org/TripPinRESTierService) generates unique
     * service URL with postfix like (S(tuivu3up5ygvjzo5fszvnwfv)) for each
     * session This method makes request to the base URL and return URL with
     * generated postfix
     */
    @SuppressWarnings("deprecation")
    protected String getRealServiceUrl(String baseUrl) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(baseUrl);
        HttpContext httpContext = new BasicHttpContext();
        httpclient.execute(httpGet, httpContext);
        HttpUriRequest currentReq = (HttpUriRequest) httpContext.getAttribute(ExecutionContext.HTTP_REQUEST);
        HttpHost currentHost = (HttpHost) httpContext.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
        System.out.println("Before:" + baseUrl + " " + currentReq.getURI().toString());
        String currentUrl = (currentReq.getURI().isAbsolute())
                ? currentReq.getURI().toString() : (currentHost.toURI() + currentReq.getURI().toString().substring(22));
        //? currentReq.getURI().toString() : (currentHost.toURI() + currentReq.getURI().toString());

        System.out.println("Current:" + currentUrl);
        return currentUrl;
    }
}
