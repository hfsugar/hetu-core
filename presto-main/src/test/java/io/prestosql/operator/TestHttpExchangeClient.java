/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.block.BlockAssertions;
import io.prestosql.execution.buffer.TestingPagesSerdeFactory;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.spi.Page;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHttpExchangeClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        HttpExchangeClient httpExchangeClient = new HttpExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                new TestingPagesSerdeFactory().createPagesSerde());

        httpExchangeClient.addLocation(location);
        httpExchangeClient.setNoMoreLocation();

        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(1));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(2));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(3));
        assertNull(getNextPage(httpExchangeClient));
        assertEquals(httpExchangeClient.isClosed(), true);

        ExchangeClientStatus status = httpExchangeClient.getStatistics();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        HttpExchangeClient httpExchangeClient = new HttpExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                new TestingPagesSerdeFactory().createPagesSerde());

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        httpExchangeClient.addLocation(location1);

        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(1));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(2));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(3));

        assertFalse(tryGetFutureValue(httpExchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(httpExchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        httpExchangeClient.addLocation(location2);

        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(4));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(5));
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(httpExchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(httpExchangeClient.isClosed(), false);

        httpExchangeClient.setNoMoreLocation();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!httpExchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(httpExchangeClient.getStatistics().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testBufferLimit()
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        HttpExchangeClient httpExchangeClient = new HttpExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                new TestingPagesSerdeFactory().createPagesSerde());

        httpExchangeClient.addLocation(location);
        httpExchangeClient.setNoMoreLocation();
        assertEquals(httpExchangeClient.isClosed(), false);

        long start = System.nanoTime();

        // start fetching pages
        httpExchangeClient.scheduleRequestIfNecessary();
        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (httpExchangeClient.getStatistics().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertEquals(httpExchangeClient.getStatistics().getBufferedPages(), 1);
        assertTrue(httpExchangeClient.getStatistics().getBufferedBytes() > 0);
        assertStatus(httpExchangeClient.getStatistics().getPageBufferClientStatuses().get(0), location, "queued", 1, 1, 1, "not scheduled");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(httpExchangeClient.pollPage(), createPage(1));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (httpExchangeClient.getStatistics().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(httpExchangeClient.getStatistics().getPageBufferClientStatuses().get(0), location, "queued", 2, 2, 2, "not scheduled");
        assertEquals(httpExchangeClient.getStatistics().getBufferedPages(), 1);
        assertTrue(httpExchangeClient.getStatistics().getBufferedBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(httpExchangeClient.pollPage(), createPage(2));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (httpExchangeClient.getStatistics().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(httpExchangeClient.getStatistics().getPageBufferClientStatuses().get(0), location, "queued", 3, 3, 3, "not scheduled");
        assertEquals(httpExchangeClient.getStatistics().getBufferedPages(), 1);
        assertTrue(httpExchangeClient.getStatistics().getBufferedBytes() > 0);

        // remove last page
        assertPageEquals(getNextPage(httpExchangeClient), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(getNextPage(httpExchangeClient));
        assertEquals(httpExchangeClient.getStatistics().getBufferedPages(), 0);
        assertTrue(httpExchangeClient.getStatistics().getBufferedBytes() == 0);
        assertEquals(httpExchangeClient.isClosed(), true);
        assertStatus(httpExchangeClient.getStatistics().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        HttpExchangeClient httpExchangeClient = new HttpExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor,
                new TestingPagesSerdeFactory().createPagesSerde());
        httpExchangeClient.addLocation(location);
        httpExchangeClient.setNoMoreLocation();

        // fetch a page
        assertEquals(httpExchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(httpExchangeClient), createPage(1));

        // close client while pages are still available
        httpExchangeClient.close();
        while (!httpExchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(httpExchangeClient.isClosed(), true);
        assertNull(httpExchangeClient.pollPage());
        assertEquals(httpExchangeClient.getStatistics().getBufferedPages(), 0);
        assertEquals(httpExchangeClient.getStatistics().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = httpExchangeClient.getStatistics().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static Page getNextPage(HttpExchangeClient httpExchangeClient)
    {
        ListenableFuture<Page> futurePage = Futures.transform(httpExchangeClient.isBlocked(), ignored -> httpExchangeClient.pollPage(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(Page actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(PageBufferClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }
}
