/*
 * Copyright 2009-2015 Scale Unlimited
 *
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
 *
 */
package bixo.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class DiskBytesWritable implements WritableComparable {
    // private static final int LENGTH_BYTES = 4;
    // private static final byte[] EMPTY_BYTES = {};

    private int _memSize;
    private byte[] _bytes;
    
    private File _backingStore;
    private FileOutputStream _diskOut;
    private int _diskSize;

    public DiskBytesWritable(byte[] bytes) {
        _bytes = bytes;
        _memSize = bytes.length;
    }
    
    public void append(byte[] bytes) throws IOException {
        if (_memSize + bytes.length <= _bytes.length) {
            System.arraycopy(bytes, 0, _bytes, _memSize, bytes.length);
            _memSize += bytes.length;
        } else {
            // Copy what we can into memory.
            int memBytes = _bytes.length - _memSize;
            System.arraycopy(bytes, 0, _bytes, _memSize, memBytes);
            _memSize += memBytes;
            
            // Spill the remainder to disk
            copyToDisk(bytes, memBytes, bytes.length - memBytes);
        }
    }
    
    private void copyToDisk(byte[] src, int offset, int length) throws IOException {
        if (_backingStore == null) {
            _backingStore = File.createTempFile(DiskBytesWritable.class.getSimpleName() + "-backingstore-", null);
            _diskOut = new FileOutputStream(_backingStore);
            _diskSize = 0;
        }

        _diskOut.write(src, offset, length);
        _diskSize += length;
    }
    
    /**
     * Get the data from the BytesWritable.
     * @return The data is only valid between 0 and getSize() - 1.
     * @throws IOException 
     */
    public byte[] get() throws IOException {
        if (_diskSize == 0) {
            return _bytes;
        }
        
        // Need to build array that combines disk with memory.
        byte[] result = new byte[_memSize + _diskSize];
        System.arraycopy(_bytes, 0, result, 0, _memSize);
        
        FileInputStream fis = new FileInputStream(_backingStore);
        fis.read(result, _memSize, _diskSize);
        fis.close();
        
        return result;
    }
    
    /**
     * Get the current size of the buffer.
     */
    public int getSize() {
      return _memSize + _diskSize;
    }
    
    /**
     * Change the size of the buffer. The values in the old range are preserved
     * and any new values are undefined. The capacity is changed if it is 
     * necessary.
     * @param size The new number of bytes
     */
    public void setSize(int size) {
        // TODO KKr - what does this mean in the context of a disk-based BytesWritable
      if (size > getCapacity()) {
        setCapacity(size * 3 / 2);
      }
      
      _memSize = size;
    }
    
    /**
     * Get the capacity, which is the maximum size that could handled without
     * resizing the backing storage.
     * @return The number of bytes
     */
    public int getCapacity() {
        // TODO KKr - what does this mean?
      return _bytes.length;
    }
    
    /**
     * Change the capacity of the backing storage.
     * The data is preserved.
     * @param new_cap The new capacity in bytes.
     */
    public void setCapacity(int new_cap) {
        // TODO KKr - what does this mean?
      if (new_cap != getCapacity()) {
        byte[] new_data = new byte[new_cap];
        if (new_cap < _memSize) {
          _memSize = new_cap;
        }
        
        if (_memSize != 0) {
          System.arraycopy(_bytes, 0, new_data, 0, _memSize);
        }
        _bytes = new_data;
      }
    }

    /**
     * Set the BytesWritable to the contents of the given newData.
     * @param newData the value to set this BytesWritable to.
     */
    public void set(DiskBytesWritable newData) {
        // TODO KKr - handle disk copy issue
      set(newData._bytes, 0, newData._memSize);
    }

    /**
     * Set the value to a copy of the given byte range
     * @param newData the new values to copy in
     * @param offset the offset in newData to start at
     * @param length the number of bytes to copy
     */
    public void set(byte[] newData, int offset, int length) {
        // TODO KKr - handle spilling to disk
      setSize(0);
      setSize(length);
      System.arraycopy(newData, offset, _bytes, 0, _memSize);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        _memSize = in.readInt();
        int diskBytes = in.readInt();
        _bytes = new byte[_memSize];
        in.readFully(_bytes);
        
        int writeOffset = 0;
        byte[] buffer = new byte[8096];
        while (writeOffset < diskBytes) {
            int bytesToRead = Math.min(buffer.length, _diskSize - writeOffset);
            in.readFully(buffer, 0, bytesToRead);
            copyToDisk(buffer, 0, bytesToRead);
            writeOffset += bytesToRead;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(_memSize);
        out.writeInt(_diskSize);
        out.write(_bytes, 0, _memSize);
        
        if (_diskSize > 0) {
            FileInputStream fis = new FileInputStream(_backingStore);
            
            byte[] buffer = new byte[8096];
            int readOffset = 0;
            while (readOffset < _diskSize) {
                int bytesToRead = Math.min(buffer.length, _diskSize - readOffset);
                fis.read(buffer, 0, bytesToRead);
                out.write(buffer, 0, bytesToRead);
                readOffset += bytesToRead;
            }
            
            fis.close();
        }
    }

    @Override
    public int compareTo(Object arg0) {
        // TODO Auto-generated method stub
        return 0;
    }

}
