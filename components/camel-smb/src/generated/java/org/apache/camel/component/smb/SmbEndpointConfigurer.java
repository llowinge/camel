/* Generated by camel build tools - do NOT edit this file! */
package org.apache.camel.component.smb;

import javax.annotation.processing.Generated;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.spi.ExtendedPropertyConfigurerGetter;
import org.apache.camel.spi.PropertyConfigurerGetter;
import org.apache.camel.spi.ConfigurerStrategy;
import org.apache.camel.spi.GeneratedPropertyConfigurer;
import org.apache.camel.util.CaseInsensitiveMap;
import org.apache.camel.support.component.PropertyConfigurerSupport;

/**
 * Generated by camel build tools - do NOT edit this file!
 */
@Generated("org.apache.camel.maven.packaging.EndpointSchemaGeneratorMojo")
@SuppressWarnings("unchecked")
public class SmbEndpointConfigurer extends PropertyConfigurerSupport implements GeneratedPropertyConfigurer, PropertyConfigurerGetter {

    @Override
    public boolean configure(CamelContext camelContext, Object obj, String name, Object value, boolean ignoreCase) {
        SmbEndpoint target = (SmbEndpoint) obj;
        switch (ignoreCase ? name.toLowerCase() : name) {
        case "allownullbody":
        case "allowNullBody": target.setAllowNullBody(property(camelContext, boolean.class, value)); return true;
        case "antexclude":
        case "antExclude": target.setAntExclude(property(camelContext, java.lang.String.class, value)); return true;
        case "antfiltercasesensitive":
        case "antFilterCaseSensitive": target.setAntFilterCaseSensitive(property(camelContext, boolean.class, value)); return true;
        case "antinclude":
        case "antInclude": target.setAntInclude(property(camelContext, java.lang.String.class, value)); return true;
        case "autocreate":
        case "autoCreate": target.setAutoCreate(property(camelContext, boolean.class, value)); return true;
        case "backofferrorthreshold":
        case "backoffErrorThreshold": target.setBackoffErrorThreshold(property(camelContext, int.class, value)); return true;
        case "backoffidlethreshold":
        case "backoffIdleThreshold": target.setBackoffIdleThreshold(property(camelContext, int.class, value)); return true;
        case "backoffmultiplier":
        case "backoffMultiplier": target.setBackoffMultiplier(property(camelContext, int.class, value)); return true;
        case "bridgeerrorhandler":
        case "bridgeErrorHandler": target.setBridgeErrorHandler(property(camelContext, boolean.class, value)); return true;
        case "browselimit":
        case "browseLimit": target.setBrowseLimit(property(camelContext, int.class, value)); return true;
        case "buffersize":
        case "bufferSize": target.setBufferSize(property(camelContext, int.class, value)); return true;
        case "charset": target.setCharset(property(camelContext, java.lang.String.class, value)); return true;
        case "checksumfilealgorithm":
        case "checksumFileAlgorithm": target.setChecksumFileAlgorithm(property(camelContext, java.lang.String.class, value)); return true;
        case "delay": target.setDelay(property(camelContext, long.class, value)); return true;
        case "delete": target.setDelete(property(camelContext, boolean.class, value)); return true;
        case "domain": target.getConfiguration().setDomain(property(camelContext, java.lang.String.class, value)); return true;
        case "donefilename":
        case "doneFileName": target.setDoneFileName(property(camelContext, java.lang.String.class, value)); return true;
        case "download": target.getConfiguration().setDownload(property(camelContext, boolean.class, value)); return true;
        case "eagerdeletetargetfile":
        case "eagerDeleteTargetFile": target.setEagerDeleteTargetFile(property(camelContext, boolean.class, value)); return true;
        case "eagermaxmessagesperpoll":
        case "eagerMaxMessagesPerPoll": target.setEagerMaxMessagesPerPoll(property(camelContext, boolean.class, value)); return true;
        case "exceptionhandler":
        case "exceptionHandler": target.setExceptionHandler(property(camelContext, org.apache.camel.spi.ExceptionHandler.class, value)); return true;
        case "exchangepattern":
        case "exchangePattern": target.setExchangePattern(property(camelContext, org.apache.camel.ExchangePattern.class, value)); return true;
        case "exclude": target.setExclude(property(camelContext, java.lang.String.class, value)); return true;
        case "excludeext":
        case "excludeExt": target.setExcludeExt(property(camelContext, java.lang.String.class, value)); return true;
        case "exclusivereadlockstrategy":
        case "exclusiveReadLockStrategy": target.setExclusiveReadLockStrategy(property(camelContext, org.apache.camel.component.file.GenericFileExclusiveReadLockStrategy.class, value)); return true;
        case "fileexist":
        case "fileExist": target.setFileExist(property(camelContext, org.apache.camel.component.file.GenericFileExist.class, value)); return true;
        case "filename":
        case "fileName": target.setFileName(property(camelContext, java.lang.String.class, value)); return true;
        case "filter": target.setFilter(property(camelContext, org.apache.camel.component.file.GenericFileFilter.class, value)); return true;
        case "filterdirectory":
        case "filterDirectory": target.setFilterDirectory(property(camelContext, java.lang.String.class, value)); return true;
        case "filterfile":
        case "filterFile": target.setFilterFile(property(camelContext, java.lang.String.class, value)); return true;
        case "flatten": target.setFlatten(property(camelContext, boolean.class, value)); return true;
        case "greedy": target.setGreedy(property(camelContext, boolean.class, value)); return true;
        case "idempotent": target.setIdempotent(property(camelContext, java.lang.Boolean.class, value)); return true;
        case "idempotenteager":
        case "idempotentEager": target.setIdempotentEager(property(camelContext, java.lang.Boolean.class, value)); return true;
        case "idempotentkey":
        case "idempotentKey": target.setIdempotentKey(property(camelContext, java.lang.String.class, value)); return true;
        case "idempotentrepository":
        case "idempotentRepository": target.setIdempotentRepository(property(camelContext, org.apache.camel.spi.IdempotentRepository.class, value)); return true;
        case "inprogressrepository":
        case "inProgressRepository": target.setInProgressRepository(property(camelContext, org.apache.camel.spi.IdempotentRepository.class, value)); return true;
        case "include": target.setInclude(property(camelContext, java.lang.String.class, value)); return true;
        case "includeext":
        case "includeExt": target.setIncludeExt(property(camelContext, java.lang.String.class, value)); return true;
        case "initialdelay":
        case "initialDelay": target.setInitialDelay(property(camelContext, long.class, value)); return true;
        case "jailstartingdirectory":
        case "jailStartingDirectory": target.setJailStartingDirectory(property(camelContext, boolean.class, value)); return true;
        case "keeplastmodified":
        case "keepLastModified": target.setKeepLastModified(property(camelContext, boolean.class, value)); return true;
        case "lazystartproducer":
        case "lazyStartProducer": target.setLazyStartProducer(property(camelContext, boolean.class, value)); return true;
        case "localworkdirectory":
        case "localWorkDirectory": target.setLocalWorkDirectory(property(camelContext, java.lang.String.class, value)); return true;
        case "maxdepth":
        case "maxDepth": target.setMaxDepth(property(camelContext, int.class, value)); return true;
        case "maxmessagesperpoll":
        case "maxMessagesPerPoll": target.setMaxMessagesPerPoll(property(camelContext, int.class, value)); return true;
        case "mindepth":
        case "minDepth": target.setMinDepth(property(camelContext, int.class, value)); return true;
        case "move": target.setMove(property(camelContext, java.lang.String.class, value)); return true;
        case "moveexisting":
        case "moveExisting": target.setMoveExisting(property(camelContext, java.lang.String.class, value)); return true;
        case "moveexistingfilestrategy":
        case "moveExistingFileStrategy": target.setMoveExistingFileStrategy(property(camelContext, org.apache.camel.component.file.strategy.FileMoveExistingStrategy.class, value)); return true;
        case "movefailed":
        case "moveFailed": target.setMoveFailed(property(camelContext, java.lang.String.class, value)); return true;
        case "noop": target.setNoop(property(camelContext, boolean.class, value)); return true;
        case "oncompletionexceptionhandler":
        case "onCompletionExceptionHandler": target.setOnCompletionExceptionHandler(property(camelContext, org.apache.camel.spi.ExceptionHandler.class, value)); return true;
        case "password": target.getConfiguration().setPassword(property(camelContext, java.lang.String.class, value)); return true;
        case "path": target.getConfiguration().setPath(property(camelContext, java.lang.String.class, value)); return true;
        case "pollstrategy":
        case "pollStrategy": target.setPollStrategy(property(camelContext, org.apache.camel.spi.PollingConsumerPollStrategy.class, value)); return true;
        case "premove":
        case "preMove": target.setPreMove(property(camelContext, java.lang.String.class, value)); return true;
        case "presort":
        case "preSort": target.setPreSort(property(camelContext, boolean.class, value)); return true;
        case "processstrategy":
        case "processStrategy": target.setProcessStrategy(property(camelContext, org.apache.camel.component.file.GenericFileProcessStrategy.class, value)); return true;
        case "readlock":
        case "readLock": target.setReadLock(property(camelContext, java.lang.String.class, value)); return true;
        case "readlockcheckinterval":
        case "readLockCheckInterval": target.setReadLockCheckInterval(property(camelContext, long.class, value)); return true;
        case "readlockdeleteorphanlockfiles":
        case "readLockDeleteOrphanLockFiles": target.setReadLockDeleteOrphanLockFiles(property(camelContext, boolean.class, value)); return true;
        case "readlocklogginglevel":
        case "readLockLoggingLevel": target.setReadLockLoggingLevel(property(camelContext, org.apache.camel.LoggingLevel.class, value)); return true;
        case "readlockmarkerfile":
        case "readLockMarkerFile": target.setReadLockMarkerFile(property(camelContext, boolean.class, value)); return true;
        case "readlockminage":
        case "readLockMinAge": target.setReadLockMinAge(property(camelContext, long.class, value)); return true;
        case "readlockminlength":
        case "readLockMinLength": target.setReadLockMinLength(property(camelContext, long.class, value)); return true;
        case "readlockremoveoncommit":
        case "readLockRemoveOnCommit": target.setReadLockRemoveOnCommit(property(camelContext, boolean.class, value)); return true;
        case "readlockremoveonrollback":
        case "readLockRemoveOnRollback": target.setReadLockRemoveOnRollback(property(camelContext, boolean.class, value)); return true;
        case "readlocktimeout":
        case "readLockTimeout": target.setReadLockTimeout(property(camelContext, long.class, value)); return true;
        case "recursive": target.setRecursive(property(camelContext, boolean.class, value)); return true;
        case "repeatcount":
        case "repeatCount": target.setRepeatCount(property(camelContext, long.class, value)); return true;
        case "runlogginglevel":
        case "runLoggingLevel": target.setRunLoggingLevel(property(camelContext, org.apache.camel.LoggingLevel.class, value)); return true;
        case "scheduledexecutorservice":
        case "scheduledExecutorService": target.setScheduledExecutorService(property(camelContext, java.util.concurrent.ScheduledExecutorService.class, value)); return true;
        case "scheduler": target.setScheduler(property(camelContext, java.lang.Object.class, value)); return true;
        case "schedulerproperties":
        case "schedulerProperties": target.setSchedulerProperties(property(camelContext, java.util.Map.class, value)); return true;
        case "searchpattern":
        case "searchPattern": target.getConfiguration().setSearchPattern(property(camelContext, java.lang.String.class, value)); return true;
        case "sendemptymessagewhenidle":
        case "sendEmptyMessageWhenIdle": target.setSendEmptyMessageWhenIdle(property(camelContext, boolean.class, value)); return true;
        case "shuffle": target.setShuffle(property(camelContext, boolean.class, value)); return true;
        case "smbconfig":
        case "smbConfig": target.getConfiguration().setSmbConfig(property(camelContext, com.hierynomus.smbj.SmbConfig.class, value)); return true;
        case "sortby":
        case "sortBy": target.setSortBy(property(camelContext, java.lang.String.class, value)); return true;
        case "sorter": target.setSorter(property(camelContext, java.util.Comparator.class, value)); return true;
        case "startscheduler":
        case "startScheduler": target.setStartScheduler(property(camelContext, boolean.class, value)); return true;
        case "streamdownload":
        case "streamDownload": target.getConfiguration().setStreamDownload(property(camelContext, boolean.class, value)); return true;
        case "tempfilename":
        case "tempFileName": target.setTempFileName(property(camelContext, java.lang.String.class, value)); return true;
        case "tempprefix":
        case "tempPrefix": target.setTempPrefix(property(camelContext, java.lang.String.class, value)); return true;
        case "timeunit":
        case "timeUnit": target.setTimeUnit(property(camelContext, java.util.concurrent.TimeUnit.class, value)); return true;
        case "usefixeddelay":
        case "useFixedDelay": target.setUseFixedDelay(property(camelContext, boolean.class, value)); return true;
        case "username": target.getConfiguration().setUsername(property(camelContext, java.lang.String.class, value)); return true;
        default: return false;
        }
    }

    @Override
    public String[] getAutowiredNames() {
        return new String[]{"smbConfig"};
    }

    @Override
    public Class<?> getOptionType(String name, boolean ignoreCase) {
        switch (ignoreCase ? name.toLowerCase() : name) {
        case "allownullbody":
        case "allowNullBody": return boolean.class;
        case "antexclude":
        case "antExclude": return java.lang.String.class;
        case "antfiltercasesensitive":
        case "antFilterCaseSensitive": return boolean.class;
        case "antinclude":
        case "antInclude": return java.lang.String.class;
        case "autocreate":
        case "autoCreate": return boolean.class;
        case "backofferrorthreshold":
        case "backoffErrorThreshold": return int.class;
        case "backoffidlethreshold":
        case "backoffIdleThreshold": return int.class;
        case "backoffmultiplier":
        case "backoffMultiplier": return int.class;
        case "bridgeerrorhandler":
        case "bridgeErrorHandler": return boolean.class;
        case "browselimit":
        case "browseLimit": return int.class;
        case "buffersize":
        case "bufferSize": return int.class;
        case "charset": return java.lang.String.class;
        case "checksumfilealgorithm":
        case "checksumFileAlgorithm": return java.lang.String.class;
        case "delay": return long.class;
        case "delete": return boolean.class;
        case "domain": return java.lang.String.class;
        case "donefilename":
        case "doneFileName": return java.lang.String.class;
        case "download": return boolean.class;
        case "eagerdeletetargetfile":
        case "eagerDeleteTargetFile": return boolean.class;
        case "eagermaxmessagesperpoll":
        case "eagerMaxMessagesPerPoll": return boolean.class;
        case "exceptionhandler":
        case "exceptionHandler": return org.apache.camel.spi.ExceptionHandler.class;
        case "exchangepattern":
        case "exchangePattern": return org.apache.camel.ExchangePattern.class;
        case "exclude": return java.lang.String.class;
        case "excludeext":
        case "excludeExt": return java.lang.String.class;
        case "exclusivereadlockstrategy":
        case "exclusiveReadLockStrategy": return org.apache.camel.component.file.GenericFileExclusiveReadLockStrategy.class;
        case "fileexist":
        case "fileExist": return org.apache.camel.component.file.GenericFileExist.class;
        case "filename":
        case "fileName": return java.lang.String.class;
        case "filter": return org.apache.camel.component.file.GenericFileFilter.class;
        case "filterdirectory":
        case "filterDirectory": return java.lang.String.class;
        case "filterfile":
        case "filterFile": return java.lang.String.class;
        case "flatten": return boolean.class;
        case "greedy": return boolean.class;
        case "idempotent": return java.lang.Boolean.class;
        case "idempotenteager":
        case "idempotentEager": return java.lang.Boolean.class;
        case "idempotentkey":
        case "idempotentKey": return java.lang.String.class;
        case "idempotentrepository":
        case "idempotentRepository": return org.apache.camel.spi.IdempotentRepository.class;
        case "inprogressrepository":
        case "inProgressRepository": return org.apache.camel.spi.IdempotentRepository.class;
        case "include": return java.lang.String.class;
        case "includeext":
        case "includeExt": return java.lang.String.class;
        case "initialdelay":
        case "initialDelay": return long.class;
        case "jailstartingdirectory":
        case "jailStartingDirectory": return boolean.class;
        case "keeplastmodified":
        case "keepLastModified": return boolean.class;
        case "lazystartproducer":
        case "lazyStartProducer": return boolean.class;
        case "localworkdirectory":
        case "localWorkDirectory": return java.lang.String.class;
        case "maxdepth":
        case "maxDepth": return int.class;
        case "maxmessagesperpoll":
        case "maxMessagesPerPoll": return int.class;
        case "mindepth":
        case "minDepth": return int.class;
        case "move": return java.lang.String.class;
        case "moveexisting":
        case "moveExisting": return java.lang.String.class;
        case "moveexistingfilestrategy":
        case "moveExistingFileStrategy": return org.apache.camel.component.file.strategy.FileMoveExistingStrategy.class;
        case "movefailed":
        case "moveFailed": return java.lang.String.class;
        case "noop": return boolean.class;
        case "oncompletionexceptionhandler":
        case "onCompletionExceptionHandler": return org.apache.camel.spi.ExceptionHandler.class;
        case "password": return java.lang.String.class;
        case "path": return java.lang.String.class;
        case "pollstrategy":
        case "pollStrategy": return org.apache.camel.spi.PollingConsumerPollStrategy.class;
        case "premove":
        case "preMove": return java.lang.String.class;
        case "presort":
        case "preSort": return boolean.class;
        case "processstrategy":
        case "processStrategy": return org.apache.camel.component.file.GenericFileProcessStrategy.class;
        case "readlock":
        case "readLock": return java.lang.String.class;
        case "readlockcheckinterval":
        case "readLockCheckInterval": return long.class;
        case "readlockdeleteorphanlockfiles":
        case "readLockDeleteOrphanLockFiles": return boolean.class;
        case "readlocklogginglevel":
        case "readLockLoggingLevel": return org.apache.camel.LoggingLevel.class;
        case "readlockmarkerfile":
        case "readLockMarkerFile": return boolean.class;
        case "readlockminage":
        case "readLockMinAge": return long.class;
        case "readlockminlength":
        case "readLockMinLength": return long.class;
        case "readlockremoveoncommit":
        case "readLockRemoveOnCommit": return boolean.class;
        case "readlockremoveonrollback":
        case "readLockRemoveOnRollback": return boolean.class;
        case "readlocktimeout":
        case "readLockTimeout": return long.class;
        case "recursive": return boolean.class;
        case "repeatcount":
        case "repeatCount": return long.class;
        case "runlogginglevel":
        case "runLoggingLevel": return org.apache.camel.LoggingLevel.class;
        case "scheduledexecutorservice":
        case "scheduledExecutorService": return java.util.concurrent.ScheduledExecutorService.class;
        case "scheduler": return java.lang.Object.class;
        case "schedulerproperties":
        case "schedulerProperties": return java.util.Map.class;
        case "searchpattern":
        case "searchPattern": return java.lang.String.class;
        case "sendemptymessagewhenidle":
        case "sendEmptyMessageWhenIdle": return boolean.class;
        case "shuffle": return boolean.class;
        case "smbconfig":
        case "smbConfig": return com.hierynomus.smbj.SmbConfig.class;
        case "sortby":
        case "sortBy": return java.lang.String.class;
        case "sorter": return java.util.Comparator.class;
        case "startscheduler":
        case "startScheduler": return boolean.class;
        case "streamdownload":
        case "streamDownload": return boolean.class;
        case "tempfilename":
        case "tempFileName": return java.lang.String.class;
        case "tempprefix":
        case "tempPrefix": return java.lang.String.class;
        case "timeunit":
        case "timeUnit": return java.util.concurrent.TimeUnit.class;
        case "usefixeddelay":
        case "useFixedDelay": return boolean.class;
        case "username": return java.lang.String.class;
        default: return null;
        }
    }

    @Override
    public Object getOptionValue(Object obj, String name, boolean ignoreCase) {
        SmbEndpoint target = (SmbEndpoint) obj;
        switch (ignoreCase ? name.toLowerCase() : name) {
        case "allownullbody":
        case "allowNullBody": return target.isAllowNullBody();
        case "antexclude":
        case "antExclude": return target.getAntExclude();
        case "antfiltercasesensitive":
        case "antFilterCaseSensitive": return target.isAntFilterCaseSensitive();
        case "antinclude":
        case "antInclude": return target.getAntInclude();
        case "autocreate":
        case "autoCreate": return target.isAutoCreate();
        case "backofferrorthreshold":
        case "backoffErrorThreshold": return target.getBackoffErrorThreshold();
        case "backoffidlethreshold":
        case "backoffIdleThreshold": return target.getBackoffIdleThreshold();
        case "backoffmultiplier":
        case "backoffMultiplier": return target.getBackoffMultiplier();
        case "bridgeerrorhandler":
        case "bridgeErrorHandler": return target.isBridgeErrorHandler();
        case "browselimit":
        case "browseLimit": return target.getBrowseLimit();
        case "buffersize":
        case "bufferSize": return target.getBufferSize();
        case "charset": return target.getCharset();
        case "checksumfilealgorithm":
        case "checksumFileAlgorithm": return target.getChecksumFileAlgorithm();
        case "delay": return target.getDelay();
        case "delete": return target.isDelete();
        case "domain": return target.getConfiguration().getDomain();
        case "donefilename":
        case "doneFileName": return target.getDoneFileName();
        case "download": return target.getConfiguration().isDownload();
        case "eagerdeletetargetfile":
        case "eagerDeleteTargetFile": return target.isEagerDeleteTargetFile();
        case "eagermaxmessagesperpoll":
        case "eagerMaxMessagesPerPoll": return target.isEagerMaxMessagesPerPoll();
        case "exceptionhandler":
        case "exceptionHandler": return target.getExceptionHandler();
        case "exchangepattern":
        case "exchangePattern": return target.getExchangePattern();
        case "exclude": return target.getExclude();
        case "excludeext":
        case "excludeExt": return target.getExcludeExt();
        case "exclusivereadlockstrategy":
        case "exclusiveReadLockStrategy": return target.getExclusiveReadLockStrategy();
        case "fileexist":
        case "fileExist": return target.getFileExist();
        case "filename":
        case "fileName": return target.getFileName();
        case "filter": return target.getFilter();
        case "filterdirectory":
        case "filterDirectory": return target.getFilterDirectory();
        case "filterfile":
        case "filterFile": return target.getFilterFile();
        case "flatten": return target.isFlatten();
        case "greedy": return target.isGreedy();
        case "idempotent": return target.getIdempotent();
        case "idempotenteager":
        case "idempotentEager": return target.getIdempotentEager();
        case "idempotentkey":
        case "idempotentKey": return target.getIdempotentKey();
        case "idempotentrepository":
        case "idempotentRepository": return target.getIdempotentRepository();
        case "inprogressrepository":
        case "inProgressRepository": return target.getInProgressRepository();
        case "include": return target.getInclude();
        case "includeext":
        case "includeExt": return target.getIncludeExt();
        case "initialdelay":
        case "initialDelay": return target.getInitialDelay();
        case "jailstartingdirectory":
        case "jailStartingDirectory": return target.isJailStartingDirectory();
        case "keeplastmodified":
        case "keepLastModified": return target.isKeepLastModified();
        case "lazystartproducer":
        case "lazyStartProducer": return target.isLazyStartProducer();
        case "localworkdirectory":
        case "localWorkDirectory": return target.getLocalWorkDirectory();
        case "maxdepth":
        case "maxDepth": return target.getMaxDepth();
        case "maxmessagesperpoll":
        case "maxMessagesPerPoll": return target.getMaxMessagesPerPoll();
        case "mindepth":
        case "minDepth": return target.getMinDepth();
        case "move": return target.getMove();
        case "moveexisting":
        case "moveExisting": return target.getMoveExisting();
        case "moveexistingfilestrategy":
        case "moveExistingFileStrategy": return target.getMoveExistingFileStrategy();
        case "movefailed":
        case "moveFailed": return target.getMoveFailed();
        case "noop": return target.isNoop();
        case "oncompletionexceptionhandler":
        case "onCompletionExceptionHandler": return target.getOnCompletionExceptionHandler();
        case "password": return target.getConfiguration().getPassword();
        case "path": return target.getConfiguration().getPath();
        case "pollstrategy":
        case "pollStrategy": return target.getPollStrategy();
        case "premove":
        case "preMove": return target.getPreMove();
        case "presort":
        case "preSort": return target.isPreSort();
        case "processstrategy":
        case "processStrategy": return target.getProcessStrategy();
        case "readlock":
        case "readLock": return target.getReadLock();
        case "readlockcheckinterval":
        case "readLockCheckInterval": return target.getReadLockCheckInterval();
        case "readlockdeleteorphanlockfiles":
        case "readLockDeleteOrphanLockFiles": return target.isReadLockDeleteOrphanLockFiles();
        case "readlocklogginglevel":
        case "readLockLoggingLevel": return target.getReadLockLoggingLevel();
        case "readlockmarkerfile":
        case "readLockMarkerFile": return target.isReadLockMarkerFile();
        case "readlockminage":
        case "readLockMinAge": return target.getReadLockMinAge();
        case "readlockminlength":
        case "readLockMinLength": return target.getReadLockMinLength();
        case "readlockremoveoncommit":
        case "readLockRemoveOnCommit": return target.isReadLockRemoveOnCommit();
        case "readlockremoveonrollback":
        case "readLockRemoveOnRollback": return target.isReadLockRemoveOnRollback();
        case "readlocktimeout":
        case "readLockTimeout": return target.getReadLockTimeout();
        case "recursive": return target.isRecursive();
        case "repeatcount":
        case "repeatCount": return target.getRepeatCount();
        case "runlogginglevel":
        case "runLoggingLevel": return target.getRunLoggingLevel();
        case "scheduledexecutorservice":
        case "scheduledExecutorService": return target.getScheduledExecutorService();
        case "scheduler": return target.getScheduler();
        case "schedulerproperties":
        case "schedulerProperties": return target.getSchedulerProperties();
        case "searchpattern":
        case "searchPattern": return target.getConfiguration().getSearchPattern();
        case "sendemptymessagewhenidle":
        case "sendEmptyMessageWhenIdle": return target.isSendEmptyMessageWhenIdle();
        case "shuffle": return target.isShuffle();
        case "smbconfig":
        case "smbConfig": return target.getConfiguration().getSmbConfig();
        case "sortby":
        case "sortBy": return target.getSortBy();
        case "sorter": return target.getSorter();
        case "startscheduler":
        case "startScheduler": return target.isStartScheduler();
        case "streamdownload":
        case "streamDownload": return target.getConfiguration().isStreamDownload();
        case "tempfilename":
        case "tempFileName": return target.getTempFileName();
        case "tempprefix":
        case "tempPrefix": return target.getTempPrefix();
        case "timeunit":
        case "timeUnit": return target.getTimeUnit();
        case "usefixeddelay":
        case "useFixedDelay": return target.isUseFixedDelay();
        case "username": return target.getConfiguration().getUsername();
        default: return null;
        }
    }

    @Override
    public Object getCollectionValueType(Object target, String name, boolean ignoreCase) {
        switch (ignoreCase ? name.toLowerCase() : name) {
        case "exclusivereadlockstrategy":
        case "exclusiveReadLockStrategy": return com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation.class;
        case "filter": return com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation.class;
        case "processstrategy":
        case "processStrategy": return com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation.class;
        case "schedulerproperties":
        case "schedulerProperties": return java.lang.Object.class;
        case "sorter": return org.apache.camel.component.file.GenericFile.class;
        default: return null;
        }
    }
}

