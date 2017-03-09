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

package com.epam.cmbi.s3;

import com.amazonaws.services.s3.AmazonS3URI;
import com.epam.cmbi.s3.utils.S3DataLoaderMocker;
import htsjdk.samtools.seekablestream.SeekableStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class S3SeekableStreamTest {

    private static final long FILE_SIZE = 2 * Configuration.getMaxDownloadPartSize() * 3;

    private S3Client client;
    private S3InputStreamFactory factory;

    @Before
    public void mockDataStream() {
        client = Mockito.mock(S3Client.class);
        Mockito.when(client.getFileSize(Mockito.any(AmazonS3URI.class))).thenReturn(FILE_SIZE);

        factory = Mockito.mock(S3InputStreamFactory.class);
        S3DataLoaderMocker.mockAutoSeqLoadFromTo(factory);
    }

    @After
    public void synch() throws InterruptedException {
        Thread.sleep(400);
    }

    @Test
    public void testRead() throws IOException {
        SeekableStream fakeSeekable = new S3SeekableStream(S3DataLoaderMocker.FAKE_URI, client, factory);
        assertEquals(0, fakeSeekable.read());

        final int arraySize = 16;
        byte[] loadArray = new byte[arraySize];
        assertEquals(fakeSeekable.read(loadArray), arraySize);

        final int offset = 8;
        assertEquals(offset, fakeSeekable.read(loadArray, offset, offset));

        assertEquals(fakeSeekable.position(), 1 + arraySize + offset);
        fakeSeekable.close();
    }

    @Test
    public void streamShouldSkipSmallSeek() throws IOException {

        final int seekPosition = 100;

        SeekableStream fakeSeekable = new S3SeekableStream(S3DataLoaderMocker.FAKE_URI,
                client,
                factory);
        assertEquals(0, fakeSeekable.position());
        fakeSeekable.seek(seekPosition);
        assertEquals(seekPosition, fakeSeekable.position());

        final int READ_COUNT = 12;
        for (int i = 0; i < READ_COUNT; i++) {
            assertEquals(seekPosition + i, fakeSeekable.read());
        }
        assertEquals(seekPosition + READ_COUNT, fakeSeekable.position());
        fakeSeekable.close();
    }

    @Test
    public void streamShouldSeek() throws IOException {

        System.setProperty("samjdk.s3plugin.number_of_connections", "2");
        Configuration.init();
        final int seekPosition = Configuration.getMaxDownloadPartSize() * 2;

        SeekableStream fakeSeekable = new S3SeekableStream(S3DataLoaderMocker.FAKE_URI,
                client,
                factory);
        fakeSeekable.seek(seekPosition);

        final int READ_COUNT = 1024;
        for (int i = 0; i < READ_COUNT; i++) {
            final int expectedByte = (seekPosition + i) & (0xff);
            assertEquals(expectedByte, fakeSeekable.read());
        }
        assertEquals(seekPosition + READ_COUNT, fakeSeekable.position());
        fakeSeekable.close();
    }

    @Test
    public void testSkip() throws IOException {

        final int skipPosition = 100;

        S3SeekableStream fakeSeekable = new S3SeekableStream(S3DataLoaderMocker.FAKE_URI,
                client,
                factory);
        //noinspection ResultOfMethodCallIgnored
        fakeSeekable.skip(skipPosition);

        assertEquals(skipPosition, fakeSeekable.position());
        assertEquals(skipPosition, fakeSeekable.read());
        assertEquals(skipPosition + 1, fakeSeekable.position());

        fakeSeekable.close();
    }

    @Test
    public void streamShouldReturnEOF() throws IOException {
        long fileSize = 1042 * 1042;
        PowerMockito.when(client.getFileSize(Mockito.any(AmazonS3URI.class))).thenReturn(fileSize);

        S3SeekableStream fakeSeekable = new S3SeekableStream(S3DataLoaderMocker.FAKE_URI,
                client,
                factory);
        for (int i = 0; i < fileSize; i++) {
            final int expectedByte = i & (0xff);
            assertEquals(expectedByte, fakeSeekable.read());
        }
        assertEquals(-1, fakeSeekable.read());
    }

}
