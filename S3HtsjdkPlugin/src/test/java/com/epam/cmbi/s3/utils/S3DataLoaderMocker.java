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

package com.epam.cmbi.s3.utils;

import com.amazonaws.services.s3.AmazonS3URI;
import com.epam.cmbi.s3.S3InputStreamFactory;
import htsjdk.samtools.util.Log;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.io.InputStream;

public class S3DataLoaderMocker {

    private static final int END_BYTE = -1;
    private static final int FROM_STREAM_CHAR = '@';
    private static int exceptionsCount = 0;
    public static final AmazonS3URI FAKE_URI = new AmazonS3URI("https://s3-eu-west-1.amazonaws.com/fake-sam-mt/fake.bam");

    public static void mockPrimitiveLoadFromTo(S3InputStreamFactory factory, int dataSize) {
        PowerMockito.mockStatic(S3InputStreamFactory.class);
        PowerMockito.when(factory.loadFromTo(Mockito.any(AmazonS3URI.class), Mockito.anyLong(), Mockito.anyLong()))
                .then((invocation) -> new InputStream() {
                            final Log log = Log.getInstance(InputStream.class);
                            private int size = dataSize;

                            @Override
                            public int read() throws IOException {
                                final int returnByte = (size-- > 0) ? FROM_STREAM_CHAR : -1;
                                log.debug("Stream(", this.hashCode(), " size = ", size, " return: ", returnByte);
                                return returnByte;
                            }
                        }
                );
    }

    public static void mockAutoSeqLoadFromTo(S3InputStreamFactory factory) {
        PowerMockito.mockStatic(S3InputStreamFactory.class);
        PowerMockito
                .when(factory.loadFromTo(
                        Mockito.any(AmazonS3URI.class),
                        Mockito.anyLong(),
                        Mockito.anyLong()))
                .then((invocation) -> new InputStream() {

                            long from = (Long) invocation.getArguments()[1];
                            final long to = (Long) invocation.getArguments()[2];

                            @Override
                            public int read() throws IOException {
                                return (from < to) ? (int) from++ : END_BYTE;
                            }
                        }
                );
    }

    public static void mockIOException(S3InputStreamFactory factory) {
        PowerMockito.mockStatic(S3InputStreamFactory.class);
        PowerMockito
                .when(factory.loadFromTo(
                        Mockito.any(AmazonS3URI.class),
                        Mockito.anyLong(),
                        Mockito.anyLong()))
                .then((invocation -> new InputStream() {
                    @Override
                    public int read() throws IOException {
                        exceptionsCount++;
                        throw new IOException("Hello from Mockito!");
                    }
                }));
    }

    public static int getExceptionsCount() {
        return exceptionsCount;
    }
}
