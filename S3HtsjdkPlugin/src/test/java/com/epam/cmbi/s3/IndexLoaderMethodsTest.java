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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class IndexLoaderMethodsTest {
    private static final String OTHER_BAM =
            "https://s3-eu-west-1.amazonaws.com/view-sam-mt/other.bam";
    private static final String SMALL_BAM =
            "https://s3-eu-west-1.amazonaws.com/view-sam-mt/small.bam";
    private static final String NOT_EXIST =
            "https://s3-eu-west-1.amazonaws.com/view-sam-mt/notExist.bam";

    private static IndexLoader loader;
    private static S3Client mock;

    @BeforeClass
    public static void setUp() {
        Configuration.resetToDefault();
    }

    @Test
    public void testMaybeProvidedIndex() {
        System.setProperty(Configuration.INDEX_URL_PARAMETER, OTHER_BAM + ".bai");
        Configuration.init();
        mock = mockIsFileExisting("other.bam.bai");
        loader = new IndexLoader(mock);
        assertTrue(loader.providedIndexURI().isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionProvidingWrongIndex() {
        System.setProperty(Configuration.INDEX_URL_PARAMETER, NOT_EXIST + ".bai");
        Configuration.init();
        mock = mockIsFileExisting("notExist.bam");
        loader = new IndexLoader(mock);
        loader.providedIndexURI();
    }

    @Test
    public void testMaybeNearbyIndex() {
        mock = mockIsFileExisting("other.bam.bai");
        loader = new IndexLoader(mock);
        assertTrue(loader.nearbyIndexURI(new AmazonS3URI(OTHER_BAM)).isPresent());
        mock = mockIsFileExisting("small.bam");
        loader = new IndexLoader(mock);
        assertFalse(loader.nearbyIndexURI(new AmazonS3URI(SMALL_BAM)).isPresent());
    }

    @AfterClass
    public static void resetConfiguration() {
     Configuration.resetToDefault();
    }

    private S3Client mockIsFileExisting(String key) {
        S3Client mock = Mockito.mock(S3Client.class);
        Mockito.when(mock.isFileExisting(Mockito.any(AmazonS3URI.class)))
                .then(invocation -> {
                    AmazonS3URI uri = (AmazonS3URI) invocation.getArguments()[0];
                    return key.equals(uri.getKey());
                });
        return mock;
    }
}
