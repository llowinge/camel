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
package org.apache.camel.component.file.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.camel.util.function.ThrowingHelper;
import org.apache.camel.util.function.ThrowingSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miscellaneous utility methods for managing the file lock cluster state.
 */
final class FileLockClusterUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileLockClusterUtils.class);

    /**
     * Length of byte[] obtained from java.util.UUID.
     */
    static final int UUID_BYTE_LENGTH = 36;
    /**
     * The lock file buffer capacity when writing data for the cluster leader.
     * <ul>
     * <li>Cluster leader ID (UUID String) - 36 bytes</li>
     * <li>Cluster leader heartbeat timestamp (long) - 8 bytes</li>
     * <li>Cluster leader update interval (long) - 8 bytes</li>
     * </ul>
     */
    static final int LOCKFILE_BUFFER_SIZE = UUID_BYTE_LENGTH + 2 * Long.BYTES;

    private FileLockClusterUtils() {
        // Utility class
    }

    /**
     * Writes information about the state of the cluster leader to the lock file.
     *
     * @param  leaderDataPath    The path to the lock file
     * @param  channel           The file channel to write to
     * @param  clusterLeaderInfo The {@link FileLockClusterLeaderInfo} instance where the cluster leader state is held
     * @param  forceMetaData     Whether to force changes to both the file content and metadata
     * @throws IOException       If the lock file is missing or writing data failed
     */
    static void writeClusterLeaderInfo(
            Path leaderDataPath,
            FileChannel channel,
            FileLockClusterLeaderInfo clusterLeaderInfo,
            boolean forceMetaData)
            throws Exception {

        Objects.requireNonNull(channel, "channel cannot be null");
        Objects.requireNonNull(clusterLeaderInfo, "clusterLeaderInfo cannot be null");

        Supplier<Void> task = ThrowingHelper.wrapAsSupplier(new ThrowingSupplier<Void, Throwable>() {
            @Override
            public Void get() throws Throwable {
                if (!Files.exists(leaderDataPath)) {
                    throw new FileNotFoundException("Cluster leader data file " + leaderDataPath + " not found");
                }

                String uuidStr = clusterLeaderInfo.getId();
                byte[] uuidBytes = uuidStr.getBytes(StandardCharsets.UTF_8);

                ByteBuffer buf = ByteBuffer.allocate(LOCKFILE_BUFFER_SIZE);
                buf.put(uuidBytes);
                buf.putLong(clusterLeaderInfo.getHeartbeatUpdateIntervalMilliseconds());
                buf.putLong(clusterLeaderInfo.getHeartbeatMilliseconds());
                buf.flip();

                if (forceMetaData) {
                    channel.truncate(0);
                }

                channel.position(0);
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
                channel.force(forceMetaData);
                return null;
            }
        });

        runClusterLeaderInfoTask(task);
    }

    /**
     * Reads information about the state of the cluster leader from the lock file.
     *
     * @param  leaderDataPath The path to the lock file
     * @return                {@link FileLockClusterLeaderInfo} instance representing the state of the cluster leader.
     *                        {@code null} if the lock file does not exist or reading the file content is in an
     *                        inconsistent state
     * @throws IOException    If reading the lock file failed
     */
    static FileLockClusterLeaderInfo readClusterLeaderInfo(Path leaderDataPath) throws Exception {
        Supplier<FileLockClusterLeaderInfo> task
                = ThrowingHelper.wrapAsSupplier(new ThrowingSupplier<FileLockClusterLeaderInfo, Throwable>() {
                    @Override
                    public FileLockClusterLeaderInfo get() throws Throwable {
                        try {
                            byte[] bytes = Files.readAllBytes(leaderDataPath);

                            if (bytes.length < LOCKFILE_BUFFER_SIZE) {
                                // Data is incomplete or in a transient / corrupt state
                                return null;
                            }

                            // Parse the cluster leader data
                            ByteBuffer buf = ByteBuffer.wrap(bytes);
                            byte[] uuidBytes = new byte[UUID_BYTE_LENGTH];
                            buf.get(uuidBytes);

                            String uuidStr = new String(uuidBytes, StandardCharsets.UTF_8);
                            long intervalMillis = buf.getLong();
                            long lastHeartbeat = buf.getLong();

                            return new FileLockClusterLeaderInfo(uuidStr, intervalMillis, lastHeartbeat);
                        } catch (FileNotFoundException | NoSuchFileException e) {
                            // Handle NoSuchFileException to give the ClusterView a chance to recreate the leadership data
                            return null;
                        }
                    }
                });

        return runClusterLeaderInfoTask(task);
    }

    /**
     * Determines whether the current cluster leader is stale. Typically, when the leader has not updated the cluster
     * lock file within acceptable bounds.
     *
     * @param  latestClusterLeaderInfo   The {@link FileLockClusterLeaderInfo} instance representing the latest cluster
     *                                   leader state
     * @param  previousClusterLeaderInfo The {@link FileLockClusterLeaderInfo} instance representing the previously
     *                                   recorded cluster leader state
     * @param  currentTimeMillis         The current time in milliseconds, as returned by
     *                                   {@link System#currentTimeMillis()} is held
     * @return                           {@code true} if the leader is considered stale. {@code false} if the leader is
     *                                   still active
     */
    static boolean isLeaderStale(
            FileLockClusterLeaderInfo latestClusterLeaderInfo,
            FileLockClusterLeaderInfo previousClusterLeaderInfo,
            long currentTimeMillis,
            int heartbeatTimeoutMultiplier) {

        if (latestClusterLeaderInfo == null) {
            return true;
        }

        // Cluster leader changed since last observation so assume not stale
        if (!latestClusterLeaderInfo.equals(previousClusterLeaderInfo)) {
            return false;
        }

        final long latestHeartbeat = latestClusterLeaderInfo.getHeartbeatMilliseconds();
        final long previousObservedHeartbeat = previousClusterLeaderInfo.getHeartbeatMilliseconds();

        if (latestHeartbeat > previousObservedHeartbeat) {
            // Not stale. Cluster leader is alive and updating the lock file
            return false;
        }

        if (latestHeartbeat < previousObservedHeartbeat) {
            // Heartbeat somehow went backwards, maybe due to stale data
            return true;
        }

        // Check if cluster leader has updated the lock file within acceptable limits
        final long elapsed = currentTimeMillis - previousObservedHeartbeat;
        final long heartbeatUpdateIntervalMilliseconds = latestClusterLeaderInfo.getHeartbeatUpdateIntervalMilliseconds();
        final long timeout = heartbeatUpdateIntervalMilliseconds * (long) heartbeatTimeoutMultiplier;
        return elapsed > timeout;
    }

    /**
     * If the cluster data root is network based, like an NFS mount, avoid potential long blocking I/O to fail fast and
     * reliably reason about the cluster state.
     *
     * @param  task Supplier representing a task to run
     * @return      The result of the task
     */
    static <T> T runClusterLeaderInfoTask(Supplier<T> task) throws ExecutionException, TimeoutException {
        int maxRetries = 6;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            LOGGER.debug("Running cluster leader info task attempt {} of {}", attempt, maxRetries);

            CompletableFuture<T> future = CompletableFuture.supplyAsync(task);
            try {
                return future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.trace("Cluster leader info task interrupted on attempt {} of {}", attempt, maxRetries);
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException | TimeoutException e) {
                LOGGER.debug("Cluster leader info task encountered an exception on attempt {} of {}", attempt, maxRetries, e);
                if (attempt == maxRetries) {
                    LOGGER.debug("Cluster leader info task retry limit ({}) reached", maxRetries, e);
                    throw e;
                }
            } finally {
                LOGGER.debug("Cluster leader info task attempt {} ended", attempt);
            }
        }
        return null;
    }
}
