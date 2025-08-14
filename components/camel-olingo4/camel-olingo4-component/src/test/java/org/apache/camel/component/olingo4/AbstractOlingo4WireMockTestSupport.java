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
import com.github.tomakehurst.wiremock.recording.RecordingStatus;
import org.apache.camel.CamelContext;
import org.apache.camel.test.infra.core.CamelContextExtension;
import org.apache.camel.test.infra.core.DefaultCamelContextExtension;
import org.apache.camel.test.infra.core.annotations.ContextFixture;
import org.apache.camel.test.junit5.ConfigurableContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public abstract class AbstractOlingo4WireMockTestSupport extends AbstractOlingo4TestSupport implements ConfigurableContext {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOlingo4WireMockTestSupport.class);
    protected static WireMockServer wireMockServer;
    protected static String serverUrlWithSessionId;
    protected static String sessionId;
    private TestInfo testInfo;
    @RegisterExtension
    public static final CamelContextExtension contextExtension = new DefaultCamelContextExtension();

    //public AbstractOlingo4WireMockTestSupport() {
    //    testConfiguration().withAutoStartContext(false);
    //}

    @BeforeAll
    public static void startWireMockServer() {
        if (useMockedBackend()) {
            LOG.info("Starting WireMock server");
            wireMockServer = new WireMockServer(wireMockConfig().dynamicPort());
            wireMockServer.start();
            wireMockServer.startRecording(recordSpec()
                    .forTarget(ODATA_API_BASE_URL)
                    .allowNonProxied(false));
            // TODO: Enable when https://github.com/wiremock/wiremock/issues/3133 is fixed
            //wireMockServer.stubFor(post(urlPathMatching(".*\\/\\$batch"))
            //        .withHeader("Content-Type", matching("multipart/mixed;boundary=.*"))
            //        .willReturn(aResponse()
            //                .withStatus(200)
            //                .withHeader("Content-Type",
            //                        "multipart/mixed; boundary=batchresponse_adc42b60-d5a8-4699-8d4b-064c5e1824b1")
            //                .withHeader("Cache-Control", "no-cache")
            //                .withHeader("OData-Version", "4.0")
            //                .withBodyFile("batch-response-file.txt")));
        }
    }

    @AfterAll
    public static void stopWireMockServer() {
        if (useMockedBackend()) {
            if (wireMockServer.getRecordingStatus().getStatus().equals(RecordingStatus.Recording)) {
                wireMockServer.stopRecording();
            }
            wireMockServer.stop();
        }
    }

    protected static boolean useMockedBackend() {
        return !Boolean.TRUE.toString().equals(System.getProperty("use.real.backend"));
    }

    protected static void refreshSession() throws IOException {
        //if (useMockedBackend()) {
        //    serverUrlWithSessionId = computeRealServiceUrl("http://localhost:" + wireMockServer.port());
        //}
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) throws IOException {
        this.testInfo = testInfo;
        // make use of the test method name to avoid collision
        String prefix = testInfo.getDisplayName().toLowerCase() + "-";
        CamelContext context = contextExtension.getContext();
        Olingo4Component component = context.getComponent("olingo4", Olingo4Component.class);
        String resolved = getResolvedTestServiceBaseUrl();
        LOG.info("Ress" + resolved);
        component.setConfiguration(new Olingo4Configuration());
        component.getConfiguration().setServiceUri(resolved);
        // clean solr endpoints
        //executeDeleteAll();
        //solrEndpoint = context.getEndpoint(DEFAULT_SOLR_ENDPOINT, SolrEndpoint.class);
        //solrEndpoint.getConfiguration().setRequestHandler(null);
        //template.setDefaultEndpoint(solrEndpoint);
    }

    @ContextFixture
    public void configureContext(CamelContext camelContext) {
        System.out.println("Configuring camelContext");

    }

    //@BeforeEach
    //void setup(TestInfo testInfo) throws IOException {
    //    System.out.println("Starting " + testInfo);
    //    this.testInfo = testInfo;
    //    Olingo4Component component = (Olingo4Component) context.getComponent("olingo4");
    //    String resolved = getResolvedTestServiceBaseUrl();
    //    System.out.println("Resolved v setup:" + resolved);
    //    component.getConfiguration().setServiceUri(resolved);
    //}

    @Override
    public String getResolvedTestServiceBaseUrl() throws IOException {
        if (useMockedBackend()) {
            //if (serverUrlWithSessionId == null) {
            serverUrlWithSessionId = getRealServiceUrl("http://localhost:" + wireMockServer.port(),
                    testInfo.getTestClass().get().getSimpleName() + "_" + testInfo.getTestMethod().get().getName());
            //}
            return serverUrlWithSessionId;
        }
        return super.getResolvedTestServiceBaseUrl();
    }

    @Override
    protected String modifyServiceUrl(String hostUri, String reqUri) {
        // for wiremock we have url in format http://localhost:<random-port> which redirects to the real service
        // but we want to use the session id but cannot append it the same as for real server because it would result to http://localhost:<random-number>/TripPinRESTierService/(S(e2ikh24z4iexhwyj5mgw4v5p))/
        // but we need http://localhost:<random-number>/(S(e2ikh24z4iexhwyj5mgw4v5p))/
        return useMockedBackend()
                ? hostUri + reqUri.substring(reqUri.indexOf('/', reqUri.indexOf('/') + 1))
                : super.modifyServiceUrl(hostUri, reqUri);
    }

    //protected static String computeRealServiceUrl(String baseUrl) throws IOException {
    //    CloseableHttpClient httpclient = HttpClients.createDefault();
    //    HttpGet httpGet = new HttpGet(baseUrl);
    //    HttpContext httpContext = new BasicHttpContext();
    //    httpclient.execute(httpGet, httpContext);
    //    HttpUriRequest currentReq = (HttpUriRequest) httpContext.getAttribute(ExecutionContext.HTTP_REQUEST);
    //    HttpHost currentHost = (HttpHost) httpContext.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
    //    String currentReqUri = currentReq.getURI().toString();
    //    if (currentReq.getURI().isAbsolute()) {
    //        return currentReqUri;
    //    }
    //    String hostUri = currentHost.toURI();
    //    String reqUri = currentReqUri;
    //    return useMockedBackend()
    //            ? hostUri + reqUri.substring(reqUri.indexOf('/', reqUri.indexOf('/') + 1))
    //            : null;
    //}
}
