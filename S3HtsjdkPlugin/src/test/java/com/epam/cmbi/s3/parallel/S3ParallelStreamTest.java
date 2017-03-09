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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;


@RunWith(PowerMockRunner.class)
@PrepareForTest({S3InputStreamFactory.class})
public class S3ParallelStreamTest {

    private final static int DATA_SIZE = 16;
    private final static int NUM_OF_THREADS = 2;
    private final static int PART_SIZE = 7;
    private S3InputStreamFactory mockFactory;

    @Before
    public void mockDataStream() {
        mockFactory = Mockito.mock(S3InputStreamFactory.class);
        S3DataLoaderMocker.mockAutoSeqLoadFromTo(mockFactory);
    }

    @AfterClass
    public static void resetConfiguration() {
        Configuration.resetToDefault();
    }

    @Test
    public void testLoadingInParallel() throws IOException {
        System.setProperty(Configuration.CONNECTIONS_NUMBER_PARAMETER,
                Integer.toString(NUM_OF_THREADS));
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, Integer.toString(PART_SIZE));
        System.setProperty(Configuration.MIN_CHUNK_SIZE_PARAMETER, Integer.toString(PART_SIZE));
        Configuration.init();


        S3ParallelStream parallelStream =
                new S3ParallelStream(S3DataLoaderMocker.FAKE_URI, 0, DATA_SIZE, mockFactory);

        for (int i = 0; i < DATA_SIZE; i++) {
            final int read = parallelStream.read();
            Assert.assertEquals(i, read);
        }

        Assert.assertEquals(-1, parallelStream.read());
    }
}