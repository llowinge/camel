/* Generated by camel build tools - do NOT edit this file! */
package org.apache.camel.component.smb;

import javax.annotation.processing.Generated;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.camel.spi.EndpointUriFactory;

/**
 * Generated by camel build tools - do NOT edit this file!
 */
@Generated("org.apache.camel.maven.packaging.GenerateEndpointUriFactoryMojo")
public class SmbEndpointUriFactory extends org.apache.camel.support.component.EndpointUriFactorySupport implements EndpointUriFactory {

    private static final String BASE = ":hostname:port/shareName";

    private static final Set<String> PROPERTY_NAMES;
    private static final Set<String> SECRET_PROPERTY_NAMES;
    private static final Set<String> MULTI_VALUE_PREFIXES;
    static {
        Set<String> props = new HashSet<>(91);
        props.add("allowNullBody");
        props.add("antExclude");
        props.add("antFilterCaseSensitive");
        props.add("antInclude");
        props.add("autoCreate");
        props.add("backoffErrorThreshold");
        props.add("backoffIdleThreshold");
        props.add("backoffMultiplier");
        props.add("bridgeErrorHandler");
        props.add("browseLimit");
        props.add("bufferSize");
        props.add("charset");
        props.add("checksumFileAlgorithm");
        props.add("delay");
        props.add("delete");
        props.add("domain");
        props.add("doneFileName");
        props.add("download");
        props.add("eagerDeleteTargetFile");
        props.add("eagerMaxMessagesPerPoll");
        props.add("exceptionHandler");
        props.add("exchangePattern");
        props.add("exclude");
        props.add("excludeExt");
        props.add("exclusiveReadLockStrategy");
        props.add("fileExist");
        props.add("fileName");
        props.add("filter");
        props.add("filterDirectory");
        props.add("filterFile");
        props.add("flatten");
        props.add("greedy");
        props.add("hostname");
        props.add("idempotent");
        props.add("idempotentEager");
        props.add("idempotentKey");
        props.add("idempotentRepository");
        props.add("inProgressRepository");
        props.add("include");
        props.add("includeExt");
        props.add("initialDelay");
        props.add("jailStartingDirectory");
        props.add("keepLastModified");
        props.add("lazyStartProducer");
        props.add("localWorkDirectory");
        props.add("maxDepth");
        props.add("maxMessagesPerPoll");
        props.add("minDepth");
        props.add("move");
        props.add("moveExisting");
        props.add("moveExistingFileStrategy");
        props.add("moveFailed");
        props.add("noop");
        props.add("onCompletionExceptionHandler");
        props.add("password");
        props.add("path");
        props.add("pollStrategy");
        props.add("port");
        props.add("preMove");
        props.add("preSort");
        props.add("processStrategy");
        props.add("readLock");
        props.add("readLockCheckInterval");
        props.add("readLockDeleteOrphanLockFiles");
        props.add("readLockLoggingLevel");
        props.add("readLockMarkerFile");
        props.add("readLockMinAge");
        props.add("readLockMinLength");
        props.add("readLockRemoveOnCommit");
        props.add("readLockRemoveOnRollback");
        props.add("readLockTimeout");
        props.add("recursive");
        props.add("repeatCount");
        props.add("runLoggingLevel");
        props.add("scheduledExecutorService");
        props.add("scheduler");
        props.add("schedulerProperties");
        props.add("searchPattern");
        props.add("sendEmptyMessageWhenIdle");
        props.add("shareName");
        props.add("shuffle");
        props.add("smbConfig");
        props.add("sortBy");
        props.add("sorter");
        props.add("startScheduler");
        props.add("streamDownload");
        props.add("tempFileName");
        props.add("tempPrefix");
        props.add("timeUnit");
        props.add("useFixedDelay");
        props.add("username");
        PROPERTY_NAMES = Collections.unmodifiableSet(props);
        Set<String> secretProps = new HashSet<>(2);
        secretProps.add("password");
        secretProps.add("username");
        SECRET_PROPERTY_NAMES = Collections.unmodifiableSet(secretProps);
        Set<String> prefixes = new HashSet<>(1);
        prefixes.add("scheduler.");
        MULTI_VALUE_PREFIXES = Collections.unmodifiableSet(prefixes);
    }

    @Override
    public boolean isEnabled(String scheme) {
        return "smb".equals(scheme);
    }

    @Override
    public String buildUri(String scheme, Map<String, Object> properties, boolean encode) throws URISyntaxException {
        String syntax = scheme + BASE;
        String uri = syntax;

        Map<String, Object> copy = new HashMap<>(properties);

        uri = buildPathParameter(syntax, uri, "hostname", null, true, copy);
        uri = buildPathParameter(syntax, uri, "port", 445, false, copy);
        uri = buildPathParameter(syntax, uri, "shareName", null, true, copy);
        uri = buildQueryParameters(uri, copy, encode);
        return uri;
    }

    @Override
    public Set<String> propertyNames() {
        return PROPERTY_NAMES;
    }

    @Override
    public Set<String> secretPropertyNames() {
        return SECRET_PROPERTY_NAMES;
    }

    @Override
    public Set<String> multiValuePrefixes() {
        return MULTI_VALUE_PREFIXES;
    }

    @Override
    public boolean isLenientProperties() {
        return false;
    }
}

