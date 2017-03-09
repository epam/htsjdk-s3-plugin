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

import com.epam.cmbi.s3.parallel.ExecutorsFactory;
import htsjdk.samtools.util.Log;

import java.text.DecimalFormat;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

class PerformanceMonitor {

    private static final int MILLISEC_IN_SEC = 1000;
    private static final int SECS_IN_MINUTE = 60;
    private static final int KILO = 1024;

    private static final Log log = Log.getInstance(PerformanceMonitor.class);

    private static volatile long startTime;
    private static final LongAdder dataLoaded = new LongAdder();
    private static final LongAdder requestCounter = new LongAdder();

    //Performance monitor is made daemon as we do not know when the InputStream
    //is going to be closed or if it even is, as HTSJDK doesn't always close the stream
    //This way it is closed automatically with the main thread.
    private static final ScheduledExecutorService executor =
            ExecutorsFactory.getDaemonScheduledExecutorService("PerformanceLogWriter");
    private static final int INITIAL_DELAY_MILLISECONDS = 3000;
    private static final int LOG_PERIOD_MILLISECONDS = 5000;

    /**
     * A method for collecting the data about the amount downloaded.
     *
     * @param data number of bytes to add.
     *
     */
    static void logLoadedData(int data) {
        dataLoaded.add(data);
    }

    /**
     * A method for summing up the get-requests made.
     */
    static void logRequest() {
        requestCounter.increment();
    }

    /**
     * A method for starting the monitoring.
     * Performance monitor is being run in a separate thread.
     */
    static void start() {
        startTime = System.currentTimeMillis();
        executor.scheduleAtFixedRate(
                PerformanceMonitor::printSummary,
                INITIAL_DELAY_MILLISECONDS,
                LOG_PERIOD_MILLISECONDS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * A method for logging the current counters state, as well as the average downloading speed.
     */
    static void printSummary() {
        long curTimeMillis = System.currentTimeMillis();
        double elapsedMinutes = (((double) curTimeMillis - startTime) / MILLISEC_IN_SEC) / SECS_IN_MINUTE;
        double averageSpeed = dataLoaded.doubleValue() /
                ((curTimeMillis - startTime) / MILLISEC_IN_SEC) / KILO / KILO;

        log.info(requestCounter.longValue()
                + " GetRequests made, "
                + dataLoaded.longValue()
                + " bytes downloaded. Average speed: "
                + new DecimalFormat("#0.00").format(averageSpeed)
                + " MB/s. Time Elapsed: "
                + new DecimalFormat("#0.00").format(elapsedMinutes) + " minutes"
        );

    }
}
