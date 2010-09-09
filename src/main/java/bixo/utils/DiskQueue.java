package bixo.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

// FUTURE KKr - worry about multi-threaded access. Could wrap syncronized(_memoryQueue)
// around most class bodies (also synchronized(_backingStore) for file methods.

/**
 * A queue that writes extra elements to disk, and reads them in as needed.
 * 
 * This implementation is optimized for being filled once (ie by the iterator in a reducer)
 * and then incrementally read. So it wouldn't work very well if reads/writes were happening
 * simultaneously, once anything had spilled to disk.
 *
 */
public class DiskQueue<E extends Serializable> extends AbstractQueue<E> {
    private static final Logger LOGGER = Logger.getLogger(DiskQueue.class);

    private static class IndexQueue<E> extends AbstractQueue<E> {

        private int _capacity;
        private LinkedList<E> _queue;
        
        public IndexQueue(int capacity) {
            _capacity = capacity;
            _queue = new LinkedList<E>();
        }
        
        @Override
        public Iterator<E> iterator() {
            return _queue.iterator();
        }

        public int getCapacity() {
            return _capacity;
        }
        
        @Override
        public int size() {
            return _queue.size();
        }

        @Override
        public boolean offer(E o) {
            if (o == null) {
                throw new NullPointerException();
            } else if (_queue.size() >= _capacity) {
                return false;
            } else {
                _queue.add(o);
                return true;
            }
        }

        @Override
        public E peek() {
            return peek(0);
        }

        public E peek(int index) throws IndexOutOfBoundsException {
            if (index >= _capacity) {
                throw new IndexOutOfBoundsException("Can't peek past end of memory queue");
            } else if (index >= _queue.size()) {
                return null;
            } else {
                return _queue.get(index);
            }
        }

        @Override
        public E poll() {
            if (_queue.size() == 0) {
                return null;
            } else {
                return _queue.remove();
            }
        }
        
        public E remove(int index) throws IndexOutOfBoundsException {
            return _queue.remove(index);
        }
        
        /**
        private final E[] _items;
        private transient int _takeIndex;
        private transient int _putIndex;
        private int _numItems;

        @SuppressWarnings("unchecked")
        public IndexQueue(int capacity) {
            _items = (E[]) new Object[capacity];
        }

        @Override
        public Iterator<E> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return _numItems;
        }

        @Override
        public boolean offer(E o) {
            if (o == null) {
                throw new NullPointerException();
            }

            if (_numItems == _items.length) {
                return false;
            } else {
                insert(o);
                return true;
            }
        }

        @Override
        public E peek() {
            return (_numItems == 0) ? null : _items[_takeIndex];
        }

        public E peek(int index) {
            if (index >= _numItems) {
                return null;
            } else {
                return _items[(index + _takeIndex) % _items.length];
            }
        }
        
        public E get(int index) {
            if ((index >= _numItems) || (index < 0)) {
                throw new IndexOutOfBoundsException("Index passed to get is invalid");
            }

            // Now for the tricky part. We need to pull out the item at <index>, and shift
            // things to fill in the gap. But there are three cases to consider, based on
            // the relative ordering of the take, put, and index positions. Note that in
            // these diagrams, <index> has been adjusted to absolute, versus an offset
            // relative to <put> (and t = take, p = put, i = index)
            // [  t..i..p  ] = easy, shift i+1 to p down
            // [..i..p  t..] = easy, shift i+1 to p down
            // [..p  t..i..] = hard, shift i+1 to end down, move 0 to end, shift 0 to p down
            // TODO make it so
            throw new UnsupportedOperationException();
        }
        
        @Override
        public E poll() {
            if (_numItems == 0) {
                return null;
            } else {
                E x = extract();
                return x;
            }
        }

        @Override
        public void clear() {
            final E[] items = _items;
            int i = _takeIndex;
            int k = _numItems;
            while (k-- > 0) {
                items[i] = null;
                i = inc(i);
            }

            _numItems = 0;
            _putIndex = 0;
            _takeIndex = 0; 
        }

        private int inc(int i) {
            return (++i == _items.length)? 0 : i;
        }

        private void insert(E x) {
            _items[_putIndex] = x;
            _putIndex = inc(_putIndex);
            ++_numItems;
        }

        private E extract() {
            final E[] items = _items;
            E x = items[_takeIndex];
            items[_takeIndex] = null;
            _takeIndex = inc(_takeIndex);
            --_numItems;
            return x;
        } 
        **/
    }
    
    // The _memoryQueue represents the head of the queue. It can also be the tail, if
    // nothing has spilled over onto the disk.
    private IndexQueue<E> _memoryQueue;
    
    // Number of elements in the backing store file on disk.
    private int _fileElements;

    private ObjectOutputStream _fileOut;
    private ObjectInputStream _fileIn;
    
    // When moving elements from disk to memory, we don't know whether the memory queue
    // has space until the offer is rejected. So rather than trying to push back an element
    // into the file, just cache it in _fileInSaved.
    private E _fileInSaved;
    private File _backingStore;

    // FUTURE - KKr could have another memory-resident queue for the tail, so that writes
    // to the file are buffered that way, and we get better performance if the queue is
    // being used for parallel read/write access, so you frequently wind up crossing the
    // 'spill' boundary from memory onto disk.
    //
    // Another issue with parallel read/write is that the backing store file could grow
    // unbounded, if elements spill to disk and then the disk is never exhausted when
    // refilling the memory queue, and you keep adding more elements at the same time.
    
    /**
     * Construct a disk-backed queue that keeps at most <maxSize> elements in memory.
     * 
     * @param maxSize Maximum number of elements to keep in memory.
     */
    public DiskQueue(int maxSize) {
        if (maxSize < 1) {
            throw new InvalidParameterException("DiskQueue max size must be at least one");
        }

        _memoryQueue = new IndexQueue<E>(maxSize);
    }


    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     * 
     * Close down streams, and toss the temp file.
     */
    @Override
    protected void finalize() throws Throwable {
        if (closeFile()) {
            LOGGER.warn("Disk queue still had open file in finalize");
        }
    }


    /**
     * Make sure the file streams are all closed down, the temp file is closed, and the
     * temp file has been deleted.
     * 
     * @return true if we had to close down the file.
     */
    private boolean closeFile() {
        if (_backingStore == null) {
            return false;
        }

        IoUtils.safeClose(_fileIn);
        _fileIn = null;
        _fileInSaved = null;

        IoUtils.safeClose(_fileOut);
        _fileOut = null;

        _fileElements = 0;

        _backingStore.delete();
        _backingStore = null;
        return true;
    }

    private void openFile() throws IOException {
        
        if (_backingStore == null) {
            _backingStore = File.createTempFile(DiskQueue.class.getSimpleName() + "-backingstore-", null);
            _fileOut = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(_backingStore)));

            // Flush output file, so there's something written when we open the input stream.
            _fileOut.flush();

            _fileIn = new ObjectInputStream(new FileInputStream(_backingStore));
        }
    }

    @Override
    public Iterator<E> iterator() {
        // FUTURE KKr - support iterator
        throw new RuntimeException("Iterator not supported for DiskQueue (yet)");
    }


    @Override
    public int size() {
        return _memoryQueue.size() + _fileElements + (_fileInSaved != null ? 1 : 0);
    }


    @Override
    public boolean offer(E element) {
        if (element == null) {
            throw new NullPointerException("Element cannot be null for AbstractQueue");
        }
        
        // If there's anything in the file, or the queue is full, then we have to write to the file.
        if ((_backingStore != null) || !_memoryQueue.offer(element)) {
            try {
                openFile();
                _fileOut.writeObject(element);
                _fileElements += 1;
                
                // Release memory ref to <element>, since we don't have any back-references from
                // it to other serialized objects.
                _fileOut.reset();
            } catch (IOException e) {
                LOGGER.error("Error writing to DiskQueue backing store", e);
                return false;
            }
        }

        return true;
    }

    @Override
    public E peek() {
        loadMemoryQueue();

        return _memoryQueue.peek();
    }

    public E peek(int index) throws IndexOutOfBoundsException {
        loadMemoryQueue();

        return _memoryQueue.peek(index);
    }

    public E remove(int index) {
        loadMemoryQueue();

        return _memoryQueue.remove(index);
    }
    
    @Override
    public E poll() {
        loadMemoryQueue();

        return _memoryQueue.poll();
    }

    /* (non-Javadoc)
     * @see java.util.AbstractQueue#clear()
     * 
     * Implement faster clear (so AbstractQueue doesn't call poll() repeatedly)
     */
    @Override
    public void clear() {
        _memoryQueue.clear();
        _fileInSaved = null;
        closeFile();
    }
    
    @SuppressWarnings("unchecked")
    private void loadMemoryQueue() {
        // use the memory queue as our buffer, so only load it up when it's empty
        if (!_memoryQueue.isEmpty()) {
            return;
        }

        // See if we have one saved element from the previous read request
        if (_fileInSaved != null) {
            if (!_memoryQueue.offer(_fileInSaved)) {
                throw new RuntimeException("Unexpected error - can't offer to an empty queue");
            }

            _fileInSaved = null;
        }

        // Now see if we have anything on disk
        if (_backingStore != null) {
            try {
                // Since we buffer writes, we need to make sure everything has been written before
                // we start reading.
                _fileOut.flush();

                while (_fileElements > 0) {
                    E nextFileElement = (E)_fileIn.readObject();
                    _fileElements -= 1;

                    if (!_memoryQueue.offer(nextFileElement)) {
                        _fileInSaved = nextFileElement;
                        return;
                    }
                }

                // Nothing left in the file, so close/delete it.
                closeFile();

                // FUTURE KKr - file queue is empty, so could reset length of file, read/write offsets
                // to start from zero instead of closing file (but for current use case of fill once, drain
                // once this works just fine)
            } catch (IOException e) {
                LOGGER.error("Error reading from DiskQueue backing store", e);
                return;
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unexpected error - can't find class for object in backing store");
            }
        }
    }
}
