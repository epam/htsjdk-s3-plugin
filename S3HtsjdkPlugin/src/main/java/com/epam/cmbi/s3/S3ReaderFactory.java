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

import java.io.IOException;
import java.net.URL;
import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3URI;
import htsjdk.samtools.CustomReaderFactory;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.RuntimeIOException;

/**
 * Class to be used as a custom reader for HTSJDK.
 * S3ReaderFactory class implements ICustomReaderFactory so that it can be used with HTSJDK runtime loading
 * mechanism for supporting alternative BAM/SAM readers.
 */
@SuppressWarnings("WeakerAccess")
public class S3ReaderFactory implements CustomReaderFactory.ICustomReaderFactory {

    /**
     * A method that creates a SamReader object that's passed on to the HTSJDK.
     * Each time someone tries to open a SamReader on an URL,
     * HTSJDK checks if there's a custom reader factory and if it's there, this method is called.
     *
     * @param url target file URL
     * @return A SamReader object on a specified file URL
     */
    @Override
    public SamReader open(URL url) {
        PerformanceMonitor.start();
        AmazonS3URI amazonURI = new AmazonS3URI(url.toString());
        S3Client client = new S3Client();
        S3InputStreamFactory streamFactory = new S3InputStreamFactory(client);

        //download index file if is possible, and then start download .bam file
        final Optional<SeekableStream> indexStream;
        try {
            IndexLoader loader = new IndexLoader(client);
            indexStream = loader.loadIndex(amazonURI);
        } catch (IOException e) {
            throw new RuntimeIOException(e.getMessage() + " failed to download index", e);
        }

        SeekableStream stream = new S3SeekableStream(amazonURI, client, streamFactory);
        SamReaderFactory factory = SamReaderFactory.makeDefault();
        SamInputResource inputResource = SamInputResource.of(stream);

        indexStream.ifPresent(inputResource::index);

        return factory.open(inputResource);
    }
}
