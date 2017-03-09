/*
 * MIT License
 *
 * Copyright (c) 2017 EPAM Systems
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.epam.cmbi.s3.parallel;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.services.s3.AmazonS3URI;
import com.epam.cmbi.s3.Configuration;
import com.epam.cmbi.s3.S3InputStreamFactory;
import htsjdk.samtools.util.Log;

/**
 * A class for parallel parts downloading. It produces a task for each file part,
 * submits them to a queue, collects the results in a correct order and gives them on request.
 */
class ParallelPartsLoader implements Runnable {

    private static final Log LOG = Log.getInstance(ParallelPartsLoader.class);
    public static final int CAPACITY_BUFFER_COEFFICIENT = 3;
    public static final byte[] EOF = new byte[0];

    private final AtomicBoolean canceledFlag = new AtomicBoolean(false);

    private final BlockingQueue<Future<Optional<byte[]>>> tasksQueue;
    private final ExecutorService threadPool;
    private final AmazonS3URI uri;
    private final long from;
    private final long to;
    private final S3InputStreamFactory factory;

    ParallelPartsLoader(AmazonS3URI uri, long from, long to, S3InputStreamFactory factory) {
        this(uri, from, to, factory, new ArrayBlockingQueue<>(
                CAPACITY_BUFFER_COEFFICIENT * Configuration.getNumberOfConnections()));
    }

    ParallelPartsLoader(AmazonS3URI uri, long from, long to, S3InputStreamFactory factory,
            BlockingQueue<Future<Optional<byte[]>>> tasksQueue) {
        this.threadPool = ExecutorsFactory.getTasksExecutor();
        this.from = from;
        this.to = to;
        this.uri = uri;
        this.factory = factory;
        this.tasksQueue = tasksQueue;
        threadPool.execute(this);
    }

    @Override public void run() {
        Thread.currentThread().setName("Parallel Parts Loader");
        try {
            produceTasks();
            putEndTasksSignal();
        } catch (InterruptedException e) {
            LOG.error(e, "Thread was interrupt during the producing of tasks for ", uri.toString());
            Thread.currentThread().interrupt();
            emergencyCancelLoading();
        }

        LOG.debug("Exit, all tasks were completed for ", uri.toString());
    }

    /**
     * This method returns next part.
     *
     * @return byte[], part of loaded file.
     */
    byte[] fetchNextPart() {
        try {
            LOG.debug("New task was get from queue.");
            return tasksQueue.take().get().orElse(EOF);
        } catch (ExecutionException | InterruptedException e) {
            LOG.error(e, "Unable to restore data stream");
            return EOF;
        }
    }

    private void produceTasks() throws InterruptedException {
        int downlPartSize = Configuration.getMinDownloadPartSize();
        int count = 0;
        for (long curPosition = from; curPosition < to; ) {
            if (canceledFlag.get()) {
                LOG.debug("Canceled ", uri.toString());
                break;
            }

            long destPosition = Math.min(to, curPosition + downlPartSize);
            tasksQueue.put(submitTask(curPosition, destPosition));
            LOG.debug("Submit task with position:" + " " + "[" + curPosition + " - " + destPosition
                    + "] for ", uri.toString());
            curPosition = destPosition;
            count++;
            if (count == Configuration.getNumberOfConnections()
                    && downlPartSize < Configuration.getMaxDownloadPartSize()) {
                downlPartSize += downlPartSize;
                count = 0;
            }
        }
    }

    private void putEndTasksSignal() throws InterruptedException {
        if (!canceledFlag.get()) {
            //poisoned task, to show that no more tasks shell be presented
            tasksQueue.put(threadPool.submit(() -> {
                LOG.debug("future poison");
                return Optional.empty();
            }));
        }
    }

    private Future<Optional<byte[]>> submitTask(long currentPosition, long destPosition) {
        PartReader task = new PartReader(uri, currentPosition, destPosition, canceledFlag, factory);
        return threadPool.submit(task);
    }

    /**
     * This method terminates work with the current resource.
     * Sets canceled flag true, clears queue of tasks and shutdowns the executor.
     */
    void cancelLoading() {
        canceledFlag.set(true);
        tasksQueue.clear();
        threadPool.shutdown();
        LOG.debug("Thread pool was shut down for ", uri.toString());
    }

    private void emergencyCancelLoading() {
        threadPool.shutdownNow();
    }
}
