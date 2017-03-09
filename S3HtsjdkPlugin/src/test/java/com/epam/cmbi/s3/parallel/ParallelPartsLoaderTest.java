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

import com.epam.cmbi.s3.Configuration;
import com.epam.cmbi.s3.S3InputStreamFactory;
import com.epam.cmbi.s3.utils.S3DataLoaderMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
public class ParallelPartsLoaderTest {

    private final static int NUM_OF_THREADS = 2;
    private final static int DATA_SIZE = 37;
    private final static int MIN_PART_SIZE = 2;
    private final static int MAX_PART_SIZE = 6;

    private S3InputStreamFactory mockFactory;

    @Before
    public void setUp() {
        System.setProperty(Configuration.CONNECTIONS_NUMBER_PARAMETER, Integer.toString(NUM_OF_THREADS));
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, Integer.toString(MAX_PART_SIZE));
        System.setProperty(Configuration.MIN_CHUNK_SIZE_PARAMETER, Integer.toString(MIN_PART_SIZE));
        Configuration.init();

        mockFactory = Mockito.mock(S3InputStreamFactory.class);
    }

    @After
    public void resetConfiguration() throws InterruptedException {
        Thread.sleep(300);
        Configuration.resetToDefault();
    }

    @Test
    public void rightNumberOfProduceTasksTest() throws InterruptedException {
        BlockingQueue<Future<Optional<byte[]>>> tasksQueue = new LinkedBlockingQueue<>();

        S3DataLoaderMocker.mockPrimitiveLoadFromTo(mockFactory, DATA_SIZE);

        new ParallelPartsLoader(
                S3DataLoaderMocker.FAKE_URI,
                0,
                DATA_SIZE,
                mockFactory,
                tasksQueue
        );

        int numberOfPayloadChunk = 9;
        for (int i = 0; i < numberOfPayloadChunk; i++) {
            tasksQueue.poll(100, TimeUnit.MILLISECONDS);
        }
        Assert.assertTrue(tasksQueue.isEmpty());
    }

    @Test
    public void rightBoundsOfProduceTasksTest() throws Exception {
        BlockingQueue<Future<Optional<byte[]>>> tasksQueue = new LinkedBlockingQueue<>();
        S3DataLoaderMocker.mockPrimitiveLoadFromTo(mockFactory, DATA_SIZE);

        new ParallelPartsLoader(
                S3DataLoaderMocker.FAKE_URI,
                0,
                DATA_SIZE,
                mockFactory,
                tasksQueue
        );
        int numberOfFullChunks = 7;
        for (int i = 0; i < numberOfFullChunks; i++) {
            int ruleForIncreaseChunkSize = (i / 2 + 1) < 4 ? i / 2 + 1 : 3;
            checkRightSizeOfChunk(tasksQueue, (int) Math.pow(MIN_PART_SIZE, ruleForIncreaseChunkSize));
        }

        checkRightSizeOfChunk(tasksQueue, 1);
        Assert.assertFalse(tasksQueue.take().get().isPresent());
    }

    @Test
    public void taskProducerShouldTerminateWhenItIsCanceled() throws InterruptedException {
        BlockingQueue<Future<Optional<byte[]>>> tasksQueue = new LinkedBlockingQueue<>();

        ParallelPartsLoader taskProducer = new ParallelPartsLoader(
                S3DataLoaderMocker.FAKE_URI,
                0,
                DATA_SIZE,
                mockFactory,
                tasksQueue
        );
        CompletableFuture.runAsync(taskProducer)
                .thenAccept(r -> taskProducer.cancelLoading())
                .thenAccept(r -> Assert.assertTrue(tasksQueue.isEmpty()));

    }

    private void checkRightSizeOfChunk(BlockingQueue<Future<Optional<byte[]>>> tasksQueue,
                                       int size) throws InterruptedException, java.util.concurrent.ExecutionException {
        byte[] byteArray = tasksQueue.take().get().orElseThrow(RuntimeException::new);
        Assert.assertEquals(size, byteArray.length);
    }
}