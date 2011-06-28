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
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to setup the queue, how to consume it
 * and how to send a message.
 */
public class RichBDBQueueExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(RichBDBQueueExample.class);
    
    public static void main(String[] args) throws IOException,
    InterruptedException, BrokenBarrierException {
        // Setup the queue
        final File queueDirectory = getQueueDirectory();
        final RichBDBQueue<String> queue = new RichBDBQueue<String>(
                queueDirectory, "test", 10, String.class);

        // This barrier is just used so that our main thread does not finish before
        // our consumer is running. This is just here to make this example definitely work.
        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final ExecutorService executor = consumeQueue(queue, startBarrier);

        // We don't want to finish our main method before the consumer also is running... 
        startBarrier.await();
        
        // Now simulate a client that writes a message to the queue
        queue.push("Hello, it's me, thread " + Thread.currentThread().getName());
        
        // Finally, close the queue to release aquired resources.
        queue.close();
        
        // Release the executor
        executor.shutdown();
        
    }

    private static File getQueueDirectory() {
        final File queueDirectory = new File(System.getProperty("java.io.tmpdir"),
                "test-queue");
        queueDirectory.mkdirs();
        return queueDirectory;
    }

    private static ExecutorService consumeQueue(final RichBDBQueue<String> queue,
            final CyclicBarrier startBarrier) {
        // Setup the queue consumer that writes messages to a database - or just to std out.
        final Consumer<String> printingConsumer = new Consumer<String>() {
            @Override
            public boolean consume(String item) {
                LOG.info("Got message: {}", item);
                return true;
            }
        };
        // The queue consumer runs in a separate thread as queue.consume(Consumer)
        // does not return.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                startBarrier.await();
                queue.consume(printingConsumer);
                return null;
            }
        });
        return executor;
    }
    
}
