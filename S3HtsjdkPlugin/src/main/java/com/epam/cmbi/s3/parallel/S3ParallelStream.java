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

import com.amazonaws.services.s3.AmazonS3URI;
import com.epam.cmbi.s3.S3InputStreamFactory;
import htsjdk.samtools.util.Log;

import java.io.IOException;
import java.io.InputStream;

/**
 * A custom Stream for parallel file reading.
 */
public class S3ParallelStream extends InputStream {

    private static final int EOF_BYTE = -1;
    private final Log log = Log.getInstance(S3ParallelStream.class);

    private final ParallelPartsLoader taskProducer;

    private volatile byte[] currentDataChunck;
    private volatile int chunckIndex;

    public S3ParallelStream(AmazonS3URI uri,
                            long from,
                            long to,
                            S3InputStreamFactory factory) {

        taskProducer = new ParallelPartsLoader(uri, from, to, factory);
        currentDataChunck = new byte[0];
    }

    @Override
    public int read() throws IOException {
       if (chunckEndReached()) {
           currentDataChunck = taskProducer.fetchNextPart();
           chunckIndex = 0;
       }
        return getNextByte();
    }

    private int getNextByte() throws IOException {
        if (currentDataChunck == ParallelPartsLoader.EOF) {
            close();
            return EOF_BYTE;
        }
        int nextByte = currentDataChunck[chunckIndex] & (0xff);
        chunckIndex++;
        return nextByte;
    }

    private boolean chunckEndReached() {
        return currentDataChunck.length == chunckIndex;
    }

    @Override
    public void close() throws IOException {
        taskProducer.cancelLoading();
        log.debug("Loading is stopped.");
    }
}
