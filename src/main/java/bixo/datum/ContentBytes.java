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
package bixo.datum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

@SuppressWarnings("serial")
public class ContentBytes extends BinaryComparable implements WritableComparable<BinaryComparable>, Serializable {

    private static final int LENGTH_BYTES = 4;
    private static final byte[] EMPTY_BYTES = {};

    private byte[] bytes;

    public ContentBytes() {
        this(EMPTY_BYTES);
    }
    
    public ContentBytes(byte[] bytes) {
        this.bytes = bytes;
    }
    
    public byte[] getBytes() {
        return bytes;
      }

    @Override
    public int getLength() {
        return bytes.length;
    }
    
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        bytes = new byte[size];
        in.readFully(bytes, 0, size);
      }
      
      // inherit javadoc
      public void write(DataOutput out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
      }
      
      public int hashCode() {
        return super.hashCode();
      }

      /**
       * Are the two byte sequences equal?
       */
      public boolean equals(Object right_obj) {
          if (right_obj instanceof ContentBytes) {
              return super.equals(right_obj);
          } else {
              return false;
          }
      }

      /**
       * Generate the stream of bytes as hex pairs separated by ' '.
       */
      public String toString() {
          int size = bytes.length;
          StringBuffer sb = new StringBuffer(3*size);
          for (int idx = 0; idx < size; idx++) {
              // if not the first, put a blank separator in
              if (idx != 0) {
                  sb.append(' ');
              }
              String num = Integer.toHexString(0xff & bytes[idx]);
              // if it is only one digit, add a leading 0.
              if (num.length() < 2) {
                  sb.append('0');
              }
              sb.append(num);
          }
          return sb.toString();
      }

      /** A Comparator optimized for BytesWritable. */ 
      public static class Comparator extends WritableComparator {
        public Comparator() {
          super(ContentBytes.class);
        }
        
        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
          return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
                              b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
        }
      }
      
      static {
       // register this comparator
        WritableComparator.define(ContentBytes.class, new Comparator());
      }
      
}
