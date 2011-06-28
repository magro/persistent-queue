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
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * 
 * Fast queue implementation on top of Berkley DB Java Edition. This class is thread-safe.
 * <p>
 * This is based on
 * <a href="http://sysgears.com/articles/lightweight-fast-persistent-queue-in-java-using-berkley-db"></a>.
 * </p>
 * 
 * Created on Jun 27, 2011
 *
 * @author Martin Grotzke (initial creation)
 */
public class BDBQueue {
    
    /**
     * Berkley DB environment.
     */
    private final Environment dbEnv;
 
    /**
     * Berkley DB instance for the queue.
     */
    private final Database queueDatabase;
 
    /**
     * Queue cache size - number of element operations it is allowed to loose in case of system crash.
     */
    private final int cacheSize;
 
    /**
     * This queue name.
     */
    private final String queueName;
 
    /**
     * Queue operation counter, which is used to sync the queue database to disk periodically.
     */
    private int opsCounter;
 
    /**
     * Creates instance of persistent queue.
     *
     * @param queueEnvPath   queue database environment directory path
     * @param queueName      descriptive queue name
     * @param cacheSize      how often to sync the queue to disk
     * @throws IOException   thrown when the given queueEnvPath does not exist and cannot be created.
     */
    public BDBQueue(final String queueEnvPath,
                 final String queueName,
                 final int cacheSize) throws IOException {
        // Create parent dirs for queue environment directory
        mkdir(new File(queueEnvPath));
 
        // Setup database environment
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        this.dbEnv = new Environment(new File(queueEnvPath), dbEnvConfig);
 
        // Setup non-transactional deferred-write queue database
        final DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        dbConfig.setBtreeComparator(new KeyComparator());
        this.queueDatabase = dbEnv.openDatabase(null, queueName, dbConfig);
        this.queueName = queueName;
        this.cacheSize = cacheSize;
        this.opsCounter = 0;
    }

    /**
     * Asserts that the given directory exists and creates it if necessary.
     * @param dir the directory that shall exist
     * @throws IOException thrown if the directory could not be created.
     */
    public static void mkdir( @Nonnull final File dir ) throws IOException {
        // commons io FileUtils.forceMkdir would be useful here, we just want to omit this dependency
        if (!dir.exists() && !dir.mkdirs()) {            
            throw new IOException( "Could not create directory " + dir.getAbsolutePath() );
        }
        if (!dir.isDirectory()) {
            throw new IOException("File " + dir + " exists and is not a directory. Unable to create directory.");
        }
    }
 
    /**
     * Retrieves and and removes element from the head of this queue.
     *
     * @return element from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized byte[] poll() throws IOException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null) {
                return null;
            }
            cursor.delete();
            opsCounter++;
            if (opsCounter >= cacheSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }
            return data.getData();
        } finally {
            cursor.close();
        }
    }

    /**
     * Retrieves element from the head of this queue.
     *
     * @return element from the head of the queue or null if queue is empty
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized byte[] peek() throws IOException {
      final DatabaseEntry key = new DatabaseEntry();
      final DatabaseEntry data = new DatabaseEntry();
      final Cursor cursor = queueDatabase.openCursor(null, null);
      try {
          cursor.getFirst(key, data, LockMode.RMW);
          if (data.getData() == null) {
              return null;
          }
          return data.getData();
      } finally {
          cursor.close();
      }
    }
    
    /**
     * Removes the eldest element.
     *
     * @throw NoSuchElementException if the queue is empty
     */
    public synchronized void remove() throws NoSuchElementException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getFirst(key, data, LockMode.RMW);
            if (data.getData() == null) {
                throw new NoSuchElementException();
            }
            cursor.delete();
            opsCounter++;
            if (opsCounter >= cacheSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }
        } finally {
            cursor.close();
        }
    }
 
    /**
     * Pushes element to the tail of this queue.
     *
     * @param element element
     *
     * @throws IOException in case of disk IO failure
     */
    public synchronized void push(final byte[] element) throws IOException {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            cursor.getLast(key, data, LockMode.RMW);
 
            BigInteger prevKeyValue;
            if (key.getData() == null) {
                prevKeyValue = BigInteger.valueOf(-1);
            } else {
                prevKeyValue = new BigInteger(key.getData());
            }
            final BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);

            final DatabaseEntry newKey = new DatabaseEntry(
                    newKeyValue.toByteArray());
            final DatabaseEntry newData = new DatabaseEntry(element);
            queueDatabase.put(null, newKey, newData);

            opsCounter++;
            if (opsCounter >= cacheSize) {
                queueDatabase.sync();
                opsCounter = 0;
            }
        } finally {
            cursor.close();
        }
    }
    
    public synchronized int clear() {
        final DatabaseEntry key = new DatabaseEntry();
        final DatabaseEntry data = new DatabaseEntry();
        final Cursor cursor = queueDatabase.openCursor(null, null);
        try {
            int itemsRemoved = 0;
            while(cursor.getNext(key, data, LockMode.RMW) == OperationStatus.SUCCESS && data.getData() != null) {
                cursor.delete();
                itemsRemoved++;
            }
            
            queueDatabase.sync();
            opsCounter = 0;
            
            return itemsRemoved;
        } finally {
            cursor.close();
        }
    }
 
   /**
     * Returns the size of this queue.
     *
     * @return the size of the queue
     */
    public long size() {
        return queueDatabase.count();
    }
    
    /**
      * Determines if this queue is empty (equivalent to <code>{@link #size()} == 0</code>).
      *
      * @return <code>true</code> if this queue is empty, otherwise <code>false</code>.
      */
     public boolean isEmpty() {
         return queueDatabase.count() == 0;
     }
 
    /**
     * Returns this queue name.
     *
     * @return this queue name
     */
    public String getQueueName() {
        return queueName;
    }
 
    /**
     * Closes this queue and frees up all resources associated to it.
     */
    public void close() {
        queueDatabase.close();
        dbEnv.close();
    }
    
}

/**
 * Key comparator for DB keys.
 */
class KeyComparator implements Comparator<byte[]>, Serializable {
 
    private static final long serialVersionUID = -7403144993786576375L;

    /**
     * Compares two DB keys.
     *
     * @param key1 first key
     * @param key2 second key
     *
     * @return comparison result
     */
    @Override
    public int compare(final byte[] key1, final byte[] key2) {
        return new BigInteger(key1).compareTo(new BigInteger(key2));
    }
    
}