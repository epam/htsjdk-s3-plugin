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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import static org.junit.Assert.assertEquals;


public class ConfigurationTest {

    /**
     *  BeforeClass is necessary here to initialize Configuration for the first time
     *  with default properties. Otherwise, the IllegalArgumentException expecting tests
     *  would receive ExceptionInInitializerError and fail.
     *  And if we change the expected Exception to ExceptionInInitializerError,
     *      the tests would fail if a non-exception expecting test runs before it, as it initializes
     *      Configuration correctly and we would receive IllegalArgumentException instead.
     */

    @Before
    public void localSetUp() {
        Configuration.resetToDefault();
    }

    @Test
    public void testConfigurationShouldReturnDefaultValuesWhenNoneAreSet() {
        assertEquals(Configuration.DEFAULT_CONNECTIONS_NUMBER, Configuration.getNumberOfConnections());
        assertEquals(Configuration.DEFAULT_MAX_CHUNK_SIZE, Configuration.getMaxDownloadPartSize());
        assertEquals(Configuration.DEFAULT_MIN_CHUNK_SIZE, Configuration.getMinDownloadPartSize());
        assertEquals(Optional.empty(), Configuration.getIndexCustomUrl());
    }

    @Test
    public void testConfigurationShouldReturnSetValues() throws MalformedURLException {
        System.setProperty(Configuration.CONNECTIONS_NUMBER_PARAMETER, "49");
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, "524287");
        System.setProperty(Configuration.MIN_CHUNK_SIZE_PARAMETER, "52428");
        System.setProperty(Configuration.INDEX_URL_PARAMETER,
                "http://www.example.com/docs/resource1.html");
        Configuration.init();
        assertEquals(49, Configuration.getNumberOfConnections());
        assertEquals(52428, Configuration.getMinDownloadPartSize());
        assertEquals(524287, Configuration.getMaxDownloadPartSize());
        assertEquals(Optional.of(new URL("http://www.example.com/docs/resource1.html")),
                Configuration.getIndexCustomUrl());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenIllegalNumberOfConnectionsIsSet() {
        System.setProperty(Configuration.CONNECTIONS_NUMBER_PARAMETER, "-1");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenNumberOfConnectionsIsSetToAString() {
        System.setProperty(Configuration.CONNECTIONS_NUMBER_PARAMETER, "text");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenNegativeMaxChunkSizeIsSet() {
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, "-1");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenNegativeMinChunkSizeIsSet() {
        System.setProperty(Configuration.MIN_CHUNK_SIZE_PARAMETER, "-1");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenMaxChunkSizeIsSetToAString() {
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, "text");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenMinChunkSizeIsSetToAString() {
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, "text");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenMaxChunkSizeIsNotAnInteger() {
        System.setProperty(Configuration.MAX_CHUNK_SIZE_PARAMETER, "524288.6");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenMinChunkSizeIsNotAnInteger() {
        System.setProperty(Configuration.MIN_CHUNK_SIZE_PARAMETER, "524288.6");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenIndexURLIsInvalid() {
        System.setProperty(Configuration.INDEX_URL_PARAMETER, "not a URL");
        Configuration.init();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testConfigurationShouldThrowExceptionWhenCustomRetryCountInvalid() {
        System.setProperty(Configuration.CUSTOM_RETRY_COUNT_PARAMETER, "-1");
        Configuration.init();
    }

    @After
    public void resetConfiguration() {
       Configuration.resetToDefault();
    }
}


