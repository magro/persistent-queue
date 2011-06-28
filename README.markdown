# Introduction
This project provides a simple reliable, persistent queue.
It is meant to be used within a single jvm to decouple event processing from external
resources like e.g. a database or webservice. So when such an external resource is not available
clients shall be able to continue their work and produced events/messages are being processed as
soon as the external resource is available again.

The current version uses [jackson](http://jackson.codehaus.org/) to serialize pojos. This is going to be extended
so that other serialization mechanisms can be used (e.g. standard java serialization for minimal dependencies).

For persistence/durability currently [Berkeley DB Java Edition](http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html)
is used. Other storage solutions (like e.g. [HSQLDB](http://hsqldb.org/) or [SQLite](http://www.sqlite.org/)) could be added in the future.

Any contributions are welcome, so if you're missing s.th. you're invited to submit a pull request or just
[submit an issue](https://github.com/magro/persistent-queue/issues).

# Usage
Here's an example snippet of code showing the creation of the queue, a client sending pushing a message and
the consumption of the message by another thread.

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
