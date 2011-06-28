/**
 * 
 */
package de.javakaffee.persistentqueue.tools;

import java.io.File;
import java.io.IOException;

import de.javakaffee.simplequeue.BDBQueue;
import de.javakaffee.simplequeue.BDBQueue.CloseableIterator;

/**
 * @author Martin Grotzke
 *
 */
public class ListQueueEntries {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing queue directory.\nUsage: " + ListQueueEntries.class.getName() +
                    " </path/to/queue/> <queue name>");
        }
        File queueDirectory = new File(args[0]);
        if(!queueDirectory.exists()) {
            throw new IllegalArgumentException("The queue directory does not exist.\nUsage: " + ListQueueEntries.class.getName() + " </path/to/queue/>");
        }

        final BDBQueue queue = new BDBQueue(queueDirectory.getPath(), args[1], 0, true, false);
        if(queue.isEmpty()) {
            System.out.println("Queue is empty.");
        }
        else {
            System.out.println("Listing " + queue.size() + " queue elements:");
            CloseableIterator<byte[]> iter = queue.iterator();
            try {
                while(iter.hasNext()) {
                    System.out.println(new String(iter.next()));
                }
            } finally {
                iter.close();
            }
        }
    }
}
