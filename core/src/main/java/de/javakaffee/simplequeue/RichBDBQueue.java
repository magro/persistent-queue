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

import javax.annotation.Nullable;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around {@link BDBQueue} that provides json serialization and
 * blocking queue semantics.
 *
 * Created on Jun 22, 2011
 *
 * @author Martin Grotzke (initial creation)
 */
public class RichBDBQueue<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RichBDBQueue.class);

    private final ObjectMapper _mapper;
    private final BDBQueue _q;
    private final Class<T> _type;
    private long pauseTimeInMillis = 100;

    private volatile boolean _doRun = true;
    private final Object _monitor = new Object();

    /**
     * Constructs the queue using the given file for persistence.
     *
     * @param queueBaseDir   queue database environment directory path
     * @param queueName      descriptive queue name
     * @param cacheSize      how often to sync the queue to disk
     *
     * @throws IOException thrown when the given file does not exist or is not writeable.
     */
    public RichBDBQueue(final File queueBaseDir, final String queueName, final int cacheSize, final Class<T> type) throws IOException {
        _mapper = createObjectMapper();
        if (!_mapper.canSerialize(type)) {
            throw new IllegalArgumentException("The given type cannot be serialized by jackson (checked with new ObjectMapper().canSerialize(type)).");
        }
        _q = new BDBQueue(queueBaseDir.getAbsolutePath(), queueName, cacheSize);
        _type = type;
    }

    /**
     * Create the {@link ObjectMapper} used for serializing.
     * @return the configured {@link ObjectMapper}.
     */
    protected ObjectMapper createObjectMapper() {
        return new ObjectMapper();
    }

    /**
     * Serializes the given object and adds the serialized form to the queue file.
     * @param object the object to add.
     * @see BDBQueue#push(byte[])
     */
    public void push(final T object) throws IOException {
        final byte[] data = _mapper.writeValueAsBytes(object);
        _q.push(data);
        synchronized(_monitor) {
            _monitor.notifyAll();
        }
    }

    /**
     * Consume the queue using the given consumer. This method will run forever and feed the
     * consumer with new elements read from the queue. An element is removed from the queue
     * after it was consumed successfully (by {@link Consumer#consume(Object)}). If the consumer
     * fails consuming it, the element will try again. Therefore, if you can't process the element
     * but want that it's removed, just handle your exception accordingly and don't throw an
     * exception.
     * @param consumer the consumer used for handling queue elements.
     * @throws IOException
     * @throws InterruptedException
     */
    public void consume(final Consumer<T> consumer) throws IOException, InterruptedException {
        while(_doRun) {
            final boolean queueEmpty;
            synchronized(_q) {
                queueEmpty = !processMessage(consumer);
            }
            if(queueEmpty) {
                synchronized(_monitor) {
                    _monitor.wait();
                }
            }
        }
    }

    /**
     * Process the head of the queue. Must return true if the queue was not empty.
     */
    private boolean processMessage(final Consumer<T> consumer) throws IOException {
        final byte[] data = _q.peek();
        final T item = deserialize(data);
        if (item != null) {
            try {
                final boolean success = consumer.consume(item);
                if (success) {
                    _q.remove();
                }
            } catch(final RuntimeException e) {
            	// If we got interrupted we want to exit, so that this client/consumer also is told to exit
            	if(Thread.currentThread().isInterrupted()) {
                    LOG.error("Consumer got interrupted, exiting / rethrowing exception.", e);
                    throw e;
            	}
                LOG.error("Consumer could not read object.", e);
            }
        }
        return data != null;
    }

    public void clear() {
        _q.clear();
    }

    /**
     * Close the queue and release the thread processing {@link #consume(Consumer)} is there is any.
     */
    public void close() {
        synchronized(_q) {
            _doRun = false;
            synchronized(_monitor) {
                _monitor.notifyAll();
            }
            LOG.info("Closing queue.");
            _q.close();
        }
    }

    public boolean isEmpty() {
        return _q.isEmpty();
    }

    public T peek() throws IOException {
        final byte[] data = _q.peek();
        return deserialize(data);
    }

    public void remove() {
        _q.remove();
    }

    public long size() {
        return _q.size();
    }

    private T deserialize(@Nullable final byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return _mapper.readValue(data, 0, data.length, _type);
        } catch(final Exception e) {
            LOG.error("Element could not be deserialized, item will be removed. Data: {}", new Object[]{data}, e);
            _q.remove();
            return null;
        }
    }

    /**
     * The time in millis that {@link #consume(Consumer)} goes to sleep when the queue
     * is empty before checking again.
     * @return the pauseTimeInMillis
     */
    public long getPauseTimeInMillis() {
        return pauseTimeInMillis;
    }

    /**
     * Sets the time in millis that {@link #consume(Consumer)} goes to sleep when the queue
     * is empty before checking again.
     * @param pauseTimeInMillis the pauseTimeInMillis to set
     */
    public void setPauseTimeInMillis(final long pauseTimeInMillis) {
        this.pauseTimeInMillis = pauseTimeInMillis;
    }

    /**
     * For testing only.
     */
    BDBQueue getQ() {
        return _q;
    }

    /**
     * For testing only.
     */
    ObjectMapper getMapper() {
        return _mapper;
    }

}
