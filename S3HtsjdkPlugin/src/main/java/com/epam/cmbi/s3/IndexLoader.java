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
import java.io.InputStream;
import java.util.Optional;

import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.util.IOUtils;
import htsjdk.samtools.seekablestream.SeekableMemoryStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Log;

class IndexLoader {

    private static final Log LOG = Log.getInstance(IndexLoader.class);
    private static final int BAM_EXTENSION_LENGTH = 3;
    private static final String BAM_BAI_EXTENSION = "bam.bai";
    private static final String BAI_EXTENSION = "bai";
    private final S3Client client;

    IndexLoader(S3Client client) {
        this.client = client;
    }

    /**
     * A method that seeks and downloads the index for the set BAM URI.
     * Seeks an index file with the same name in the BAM directory
     * in case there's no custom index URI specified
     *
     * @param bamURI an http address of the required file.
     * @return A SeekableStream optional on index file URI
     */
    Optional<SeekableStream> loadIndex(AmazonS3URI bamURI) throws IOException {
        LOG.info("Trying to set index file for " + bamURI.toString());
        Optional<AmazonS3URI> index = providedIndexURI()
                .map(Optional::of)
                .orElseGet(() -> nearbyIndexURI(bamURI));

        if (!index.isPresent()) {
            LOG.info("Index wasn't provided for " + bamURI.toString());
            return Optional.empty();
        }

        LOG.info("Start download index: " + index.get());
        AmazonS3URI indexURI = index.get();
        S3InputStreamFactory streamFactory = new S3InputStreamFactory(client);
        InputStream stream = streamFactory.loadFully(indexURI);
        long fileSize = client.getFileSize(indexURI);
        byte[] buffer = IOUtils.toByteArray(stream);

        if (fileSize != buffer.length) {
            throw new IOException("Failed to fully download index " + indexURI);
        }

        LOG.info("Finished download index: " + index.get());
        return Optional.of(new SeekableMemoryStream(buffer, indexURI.toString()));
    }

    /**
     * A method that returns an Optional of the set custom index URI.
     *
     * @return index URI
     */
    Optional<AmazonS3URI> providedIndexURI() {
        return Configuration.getIndexCustomUrl().map(url -> {
            AmazonS3URI uri = new AmazonS3URI(url.toString());
            if (!client.isFileExisting(uri)) {
                throw new IllegalArgumentException("Provided index file doesn't exist.");
            }
            return uri;
        });
    }

    /**
     * A method for when no custom index URI is set. Try find index file with same name and location.
     *
     * @param bamURI the BAM file URI.
     * @return Optional of index URI.
     */
    Optional<AmazonS3URI> nearbyIndexURI(AmazonS3URI bamURI) {
        String uri = bamURI.toString();
        String uriWithNoFormat = uri.substring(0, uri.length() - BAM_EXTENSION_LENGTH);

        Optional<AmazonS3URI> indBamBai = Optional.of(new AmazonS3URI(uriWithNoFormat + BAM_BAI_EXTENSION))
                .filter(client::isFileExisting);
        Optional<AmazonS3URI> indBai = Optional.of(new AmazonS3URI(uriWithNoFormat + BAI_EXTENSION))
                .filter(client::isFileExisting);
        return indBamBai.map(Optional::of).orElseGet(() -> indBai);
    }
}
