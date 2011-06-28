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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import de.javakaffee.simplequeue.BDBQueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test for {@link BDBQueue}.
 * 
 * Created on Jun 27, 2011
 *
 * @author Martin Grotzke (initial creation)
 */
public class BDBQueueTest {
    
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));
    
    private File _queueDir;
    
    @BeforeMethod
    public void beforeMethod() throws IOException {
        _queueDir = createTempSubdir("test-queue");
    }
    
    @AfterMethod
    public void afterMethod() throws IOException {
        deleteDirectory(_queueDir);
    }
    
    @Test
    public void testCreateQueue() throws IOException {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            assertTrue(Arrays.asList(_queueDir.listFiles()).contains(new File(_queueDir, "00000000.jdb")));
        } finally {
            queue.close();
        }
    }

    @Test
    public void testPush() throws Throwable {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("1".getBytes(UTF8));
            queue.push("2".getBytes(UTF8));
            final String head = new String(queue.poll(), UTF8);
            assertEquals(head, "1");
        } finally {
            queue.close();
        }
    }

    @Test
    public void testConsumeAfterPush() throws Throwable {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("1".getBytes(UTF8));
            queue.push("2".getBytes(UTF8));
            final String head = new String(queue.poll(), UTF8);
            assertEquals(head, "1");
        } finally {
            queue.close();
        }
    }

    @Test
    public void testSize() throws Throwable {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("1".getBytes(UTF8));
            queue.push("2".getBytes(UTF8));
            assertEquals(queue.size(), 2);
        } finally {
            queue.close();
        }
    }

    @Test
    public void testClear() throws Throwable {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("1".getBytes(UTF8));
            queue.push("2".getBytes(UTF8));
            assertEquals(queue.clear(), 2);
            assertEquals(queue.size(), 0);
        } finally {
            queue.close();
        }
    }
 
    @Test
    public void testQueueSurviveReopen() throws Throwable {
        BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            queue.push("5".getBytes(UTF8));
        } finally {
            queue.close();
        }
 
        queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            final String head = new String(queue.poll(), UTF8);
            assertEquals(head, "5");
        } finally {
            queue.close();
        }
    }
 
    @Test
    public void testQueuePushOrder() throws Throwable {
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 1000);
        try {
            for (int i = 0; i < 300; i++) {
                queue.push(Integer.toString(i).getBytes(UTF8));
            }
 
            for (int i = 0; i < 300; i++) {
                final String element = new String(queue.poll(), UTF8);
                if (!Integer.toString(i).equals(element)) {
                    throw new AssertionError("Expected element " + i + ", but got " + element);
                }
            }
        } finally {
            queue.close();
        }
 
    }
 
    @Test
    public void testMultiThreadedPoll() throws Throwable {
        
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            final int threadCount = 20;
            for (int i = 0; i < threadCount; i++) {
                queue.push(Integer.toString(i).getBytes(UTF8));
            }
 
            final Set<String> set = Collections.synchronizedSet(new HashSet<String>());
            final CountDownLatch startLatch = new CountDownLatch(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);
 
            for (int i = 0; i < threadCount; i++) {
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            startLatch.countDown();
                            startLatch.await();
 
                            final byte[] data = queue.poll();
                            if (data != null) {
                                final String val = new String(data, UTF8);
                                set.add(val);
                            }
                            latch.countDown();
                        } catch (final Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }
 
            latch.await(5, TimeUnit.SECONDS);
 
            assertEquals(set.size(), threadCount);
        } finally {
            queue.close();
        }
    }
 
    @Test
    public void testMultiThreadedPush() throws Throwable {
        
        final BDBQueue queue = new BDBQueue(_queueDir.getPath(), "test-queue", 3);
        try {
            final int threadCount = 20;
 
            final CountDownLatch startLatch = new CountDownLatch(threadCount);
            final CountDownLatch latch = new CountDownLatch(threadCount);
 
            for (int i = 0; i < threadCount; i++) {
                new Thread(Integer.toString(i)) {
                    @Override
                    public void run() {
                        try {
                            startLatch.countDown();
                            startLatch.await();
 
                            queue.push(getName().getBytes(UTF8));
                            latch.countDown();
                        } catch (final Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }
 
            latch.await(5, TimeUnit.SECONDS);
 
            assertEquals(queue.size(), threadCount);
        } finally {
            queue.close();
        }
    }
    
    // ------------------ some stuff from commons io FileUtils
 
    public static File createTempSubdir(final String name) throws IOException {
        final File dir = new File(TMP_DIR, name);
        BDBQueue.mkdir(dir);
        return dir;
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
    
}
