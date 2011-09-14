/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.javakaffee.simplequeue;

import static de.javakaffee.simplequeue.BDBQueueTest.createTempSubdir;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for {@link RichBDBQueue}.
 *
 * Created on Jun 27, 2011
 *
 * @author Martin Grotzke (initial creation)
 */
public class RichBDBQueueTest {

    private RichBDBQueue<String> _cut;
    private ExecutorService _executor;
    private boolean _clearQueue;

    @BeforeMethod
    public void beforeMethod() throws IOException {
    	// We reset the interrupted state as it might have been set by the previous test
    	Thread.interrupted();
        _cut = createQueue();
        _cut.clear();
        _clearQueue = true;
    }

	private RichBDBQueue<String> createQueue() throws IOException {
		return new RichBDBQueue<String>(createTempSubdir("test-queue"), "test", 10, String.class);
	}

    @AfterMethod
    public void afterMethod() throws IOException {
    	if (_clearQueue) {
    		_cut.clear();
    	}
        _cut.close();
        if (_executor != null) {
            _executor.shutdown();
        }
    }

    @Test
    public void testPush() throws Throwable {
        _cut.push("1");
        _cut.push("2");
        final String head = _cut.peek();
        assertEquals(head, "1");
    }

    @Test
    public void testConsumeAfterPush() throws Throwable {
        _cut.push("1");
        _cut.push("2");
        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final CountDownLatch doneSignal = new CountDownLatch(2);
        final CountingConsumer c = new CountingConsumer(doneSignal);
        _executor = Executors.newSingleThreadExecutor();
        _executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                await(startBarrier);
                _cut.consume(c);
                return null;
            }
        });

        await(startBarrier);

        waitForEmptyQueue(2);

        assertEquals(c.items.size(), 2);
        assertEquals(c.items, Arrays.asList("1", "2"));
    }

    @Test
    public void testConsumeDuringPush() throws Throwable {
        _executor = Executors.newFixedThreadPool(10);
        final MyConsumer c = new MyConsumer();
        _executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                _cut.consume(c);
                return null;
            }
        });

        final List<String> seq = seqAsStrings(0, 500);
        final LinkedBlockingQueue<String> items = new LinkedBlockingQueue<String>(seq);

        _executor.invokeAll(Arrays.asList(new Producer(items), new Producer(items), new Producer(items),
                new Producer(items), new Producer(items), new Producer(items)));

        waitForEmptyQueue(2);

        assertEquals(c.items.size(), seq.size());
        final HashSet<String> itemSet = new HashSet<String>(seq);
        itemSet.removeAll(c.items);
        assertEquals(itemSet.size(), 0);
    }

    @Test
    public void testInterruptionDuringConsumeAndProperShutdown() throws Throwable {
    	// offer 1 element to that Consumer.consume is invoked
        _cut.push("foo");

        final CountDownLatch cdl = new CountDownLatch(2);
       final  Thread consumerThread = new Thread(new Runnable() {
			@Override
			public void run() {
                try {
					_cut.consume(new Consumer<String>() {
					    @Override
					    public boolean consume(final String item) {
					        try {
					        	cdl.countDown();
					        	cdl.await();
					            Thread.sleep(100);
						        return true;
					        } catch (final InterruptedException e1) {
					            Thread.currentThread().interrupt();
					            throw new RuntimeException("EXPECTED EXCEPTION: Got interrupted while sleeping.", e1);
					        }
					    }
					});
				} catch (final Exception e) { throw new RuntimeException(e); }
			}});
        consumerThread.start();

        cdl.countDown();
        cdl.await();

        // interrupt the consumer thread
        consumerThread.interrupt();

        assertFalse(_cut.isEmpty());

        // the queue.close/afterMethod should finish cleanly
    }

    @Test
    public void testInterruptionDuringConsumeWait() throws Throwable {

    	// offer 1 element, as otherwise the afterMethod does not fail on queue.close().
        _cut.push("foo");

        final CountDownLatch cdl = new CountDownLatch(1);
        final Thread consumerThread = new Thread(new Runnable() {
			@Override
			public void run() {
                try {
					_cut.consume(new Consumer<String>() {
					    @Override
					    public boolean consume(final String item) {
					    	cdl.countDown();
					    	return true;
					    }
					});
				} catch (final Exception e) { throw new RuntimeException(e); }
			}});
        consumerThread.start();

        // wait until the richqueue.consume was invoked and the consumer has been invoked
        cdl.await();

        // wait a little bit so that the richqueue entered _monitor.wait();
        Thread.sleep(100);

        // now interrupt this thread
        Thread.currentThread().interrupt();

        assertTrue(_cut.isEmpty());

        _clearQueue = false;

        /* before the fix (interrupted status handling in bdbqueue) the aftermethod failed with this exception:
           com.sleepycat.je.ThreadInterruptedException: (JE 4.1.10) /tmp/test-queue Channel closed,
           may be due to thread interrupt THREAD_INTERRUPTED: InterruptedException may cause incorrect internal
           state, unable to continue. Environment is invalid and must be closed.
			at com.sleepycat.je.log.FileManager$LogEndFileDescriptor.force(FileManager.java:2720)
			at com.sleepycat.je.log.FileManager$LogEndFileDescriptor.access$500(FileManager.java:2390)
			at com.sleepycat.je.log.FileManager.syncLogEnd(FileManager.java:1713)
			at com.sleepycat.je.log.LogManager.flush(LogManager.java:1116)
			at com.sleepycat.je.recovery.Checkpointer.syncDatabase(Checkpointer.java:851)
			at com.sleepycat.je.dbi.DatabaseImpl.sync(DatabaseImpl.java:981)
			at com.sleepycat.je.dbi.DatabaseImpl.handleClosed(DatabaseImpl.java:867)
			at com.sleepycat.je.Database.closeInternal(Database.java:458)
			at com.sleepycat.je.Database.close(Database.java:314)
			at de.javakaffee.simplequeue.BDBQueue.close(BDBQueue.java:371)
			at de.javakaffee.simplequeue.RichBDBQueue.close(RichBDBQueue.java:146)
         */

    }

    /**
     * @param secondsToWait
     * @throws InterruptedException
     * @throws AssertionError
     */
    private void waitForEmptyQueue(final int secondsToWait) throws InterruptedException, AssertionError {
        final long start = currentTimeMillis();
        while(_cut.size() > 0) {
            Thread.sleep(10);
            if (currentTimeMillis() > start + SECONDS.toMillis(secondsToWait)) {
                throw new AssertionError("The queue got not empty within "+ secondsToWait +" seconds.");
            }
        }
    }

    private List<String> seqAsStrings(final int from, final int to) {
        final List<String> result = new ArrayList<String>();
        for(int i = from; i < to; i++) {
            result.add(String.valueOf(i));
        }
        return result;
    }

    @Test
    public void testSize() throws Throwable {
        assertEquals(_cut.size(), 0);
        _cut.push("1");
        _cut.push("2");
        assertEquals(_cut.size(), 2);
    }

    @Test
    public void testClear() throws Throwable {
        _cut.push("1");
        _cut.push("2");
        _cut.clear();
        assertEquals(_cut.size(), 0);
    }

    // ------------------ some stuff from commons io FileUtils

    private void await(final CyclicBarrier barrier) throws RuntimeException {
        try {
            barrier.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Got interrupted.", e);
        } catch (final BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a directory recursively.
     *
     * @param directory  directory to delete
     * @throws IOException in case deletion is unsuccessful
     */
    public static void deleteDirectory(final File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        cleanDirectory(directory);

        if (!directory.delete()) {
            final String message =
                "Unable to delete directory " + directory + ".";
            throw new IOException(message);
        }
    }

    /**
     * Cleans a directory without deleting it.
     *
     * @param directory directory to clean
     * @throws IOException in case cleaning is unsuccessful
     */
    public static void cleanDirectory(final File directory) throws IOException {
        if (!directory.exists()) {
            final String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            final String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        final File[] files = directory.listFiles();
        if (files == null) {  // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (final File file : files) {
            try {
                forceDelete(file);
            } catch (final IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    /**
     * Deletes a file. If file is a directory, delete it and all sub-directories.
     * <p>
     * The difference between File.delete() and this method are:
     * <ul>
     * <li>A directory to be deleted does not have to be empty.</li>
     * <li>You get exceptions when a file or directory cannot be deleted.
     *      (java.io.File methods returns a boolean)</li>
     * </ul>
     *
     * @param file  file or directory to delete, must not be <code>null</code>
     * @throws NullPointerException if the directory is <code>null</code>
     * @throws FileNotFoundException if the file was not found
     * @throws IOException in case deletion is unsuccessful
     */
    public static void forceDelete(final File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            final boolean filePresent = file.exists();
            if (!file.delete()) {
                if (!filePresent){
                    throw new FileNotFoundException("File does not exist: " + file);
                }
                final String message =
                    "Unable to delete file: " + file;
                throw new IOException(message);
            }
        }
    }

    private final class Producer implements Callable<Void> {
        private final LinkedBlockingQueue<String> _items;

        private Producer(final LinkedBlockingQueue<String> items) {
            _items = items;
        }

        @Override
        public Void call() throws Exception {
            String item = null;
            while((item = _items.poll()) != null) {
                _cut.push(item);
            }
            return null;
        }
    }

    static class MyConsumer implements Consumer<String> {

        private final List<String> items = new ArrayList<String>();

        @Override
        public boolean consume(final String item) {
            return items.add(item);
        }

    }

    static class CountingConsumer implements Consumer<String> {

        private final List<String> items = new ArrayList<String>();
        private final CountDownLatch _counter;

        public CountingConsumer(final CountDownLatch counter) {
            _counter = counter;
        }

        @Override
        public boolean consume(final String item) {
            final boolean result = items.add(item);
            _counter.countDown();
            return result;
        }

    }

}
