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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class S3InputStreamFactory {

    private final S3Client client;

    public S3InputStreamFactory(S3Client client) {
        this.client = client;
    }

    /**
     * A method that creates an InputStream on a specific range of the file.
     * InputStream classes wrapping order can be reversed.
     *
     * @param obj    target file URI
     * @param offset range start position
     * @param end    range end position
     * @return an InputStream object on the specific range of the file.
     */
    public InputStream loadFromTo(AmazonS3URI obj, long offset, long end) {
        GetObjectRequest rangeObjectRequest = new GetObjectRequest(obj.getBucket(), obj.getKey());
        rangeObjectRequest.setRange(offset, end);
        S3Object s3Object = client.getAws().getObject(rangeObjectRequest);
        PerformanceMonitor.logRequest();
        S3ObjectInputStream objectStream = s3Object.getObjectContent();
        return new MonitoredInputStream(new BufferedInputStream(objectStream));
    }

    /**
     * A method that creates an InputStream on a range
     * from a specific position to the end of the file.
     *
     * @param obj    target file URI
     * @param offset range start position
     * @return an InputStream object on the specific range of the file.
     */
    @SuppressWarnings("WeakerAccess") public InputStream loadFrom(AmazonS3URI obj,
            @SuppressWarnings("SameParameterValue") long offset) {
        long contentLength = client.getFileSize(obj);
        return loadFromTo(obj, offset, contentLength);
    }

    /**
     * A method that creates an InputStream on a specific file URI.
     *
     * @param obj target file URI
     * @return an InputStream object on the file URI.
     */
    @SuppressWarnings("WeakerAccess") public InputStream loadFully(AmazonS3URI obj) {
        return loadFrom(obj, 0);
    }

    private static class MonitoredInputStream extends InputStream {
        private final InputStream wrappedStream;

        MonitoredInputStream(InputStream stream) {
            this.wrappedStream = stream;
        }

        @Override public int read() throws IOException {
            PerformanceMonitor.logLoadedData(1);
            return wrappedStream.read();
        }

        @Override public int read(byte[] b) throws IOException {
            int bytesRead = wrappedStream.read(b);
            PerformanceMonitor.logLoadedData(bytesRead);
            return bytesRead;
        }

        @Override public int read(byte[] b, int off, int len) throws IOException {
            int bytesRead = wrappedStream.read(b, off, len);
            PerformanceMonitor.logLoadedData(bytesRead);
            return bytesRead;
        }

        @Override public long skip(long n) throws IOException {
            return wrappedStream.skip(n);
        }

        @Override public int available() throws IOException {
            return wrappedStream.available();
        }

        @Override public void close() throws IOException {
            wrappedStream.close();
        }

        // This method is inherently synchronized from superclass.
        @Override public synchronized void mark(int readlimit) {
            wrappedStream.mark(readlimit);
        }

        // This method is inherently synchronized from superclass.
        @Override public synchronized void reset() throws IOException {
            wrappedStream.reset();
        }

        @Override public boolean markSupported() {
            return wrappedStream.markSupported();
        }
    }
}
