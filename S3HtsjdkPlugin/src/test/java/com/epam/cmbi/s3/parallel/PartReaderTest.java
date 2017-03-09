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
import htsjdk.samtools.util.RuntimeIOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(MockitoJUnitRunner.class)
@PrepareForTest({S3InputStreamFactory.class})
public class PartReaderTest {

    private final static int DATA_SIZE = 125;
    private S3InputStreamFactory mockFactory;

    @Before
    public void mockDataStream() {
        mockFactory = Mockito.mock(S3InputStreamFactory.class);
        S3DataLoaderMocker.mockAutoSeqLoadFromTo(mockFactory);
    }

    @Test
    public void testFullLoading() throws InterruptedException {
        AtomicBoolean canceledFlag = new AtomicBoolean(false);
        PartReader reader =
                new PartReader(S3DataLoaderMocker.FAKE_URI, 0, DATA_SIZE,
                        canceledFlag, mockFactory);
        byte[] buffer = reader.call().orElseThrow(RuntimeException::new);

        for (int i = 0; i < DATA_SIZE; i++) {
            Assert.assertEquals(i, buffer[i]);
        }
    }

    @Test
    public void testPartLoading() throws InterruptedException {
        AtomicBoolean canceledFlag = new AtomicBoolean(false);
        final int partSize = 10;
        PartReader reader = new PartReader(S3DataLoaderMocker.FAKE_URI, DATA_SIZE - partSize,
                DATA_SIZE,
                canceledFlag,
                mockFactory);
        byte[] bufferStream = reader.call().orElseThrow(RuntimeException::new);

        for (int i = 0; i < partSize; i++) {
            Assert.assertEquals(i + DATA_SIZE - partSize, bufferStream[i]);
        }
    }

    @Test
    public void testCancellation() throws ExecutionException, InterruptedException {
        AtomicBoolean canceledFlag = new AtomicBoolean(false);
        PartReader reader =
                new PartReader(S3DataLoaderMocker.FAKE_URI, 0,
                        DATA_SIZE, canceledFlag, mockFactory);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Optional<byte[]>> future = executor.submit(reader);
        canceledFlag.set(true);

        try {
            future.get();
        } catch (ExecutionException e) {
            if (!e.getCause().getClass().equals(InterruptedException.class)) {
                Assert.fail();
            }
        }
        executor.shutdown();
    }

    @Test
    public void partReaderShouldReconnect() throws InterruptedException {
        S3DataLoaderMocker.mockIOException(mockFactory);

        PartReader reader = new PartReader(S3DataLoaderMocker.FAKE_URI,
                0,
                DATA_SIZE,
                new AtomicBoolean(false),
                mockFactory);
        Assert.assertEquals(Optional.empty(), reader.call());
        Assert.assertEquals(Configuration.getCustomRetryCount(),
                S3DataLoaderMocker.getExceptionsCount());
    }

    @Test(expected = RuntimeIOException.class)
    public void partReaderThrowExceptionWhenDataStreamEndsAhead() throws InterruptedException {
        S3DataLoaderMocker.mockPrimitiveLoadFromTo(mockFactory, DATA_SIZE/2);

        PartReader reader = new PartReader(S3DataLoaderMocker.FAKE_URI,
                0,
                DATA_SIZE,
                new AtomicBoolean(false),
                mockFactory);
        reader.call();
    }

}