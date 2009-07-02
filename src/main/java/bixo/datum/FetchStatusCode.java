/*
 * Copyright (c) 1997-2009 101tec Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
 * copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in 
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package bixo.datum;

public enum FetchStatusCode {

    UNFETCHED,      // Fetch of URL hasn't been attempted yet. This must be the first, so that its ordinal is 0.
    FETCHED,        // URL was successfully fetched.
    MOVED,          // URL has changed (normalized, permanently redirected)
    ABORTED,        // URL fetch was aborted.
    ERROR;          // URL fetch failed due to an error.

    // TODO KKr - convert to using name strings, vs. ordinal values.
    
    public static FetchStatusCode fromOrdinal(int i) {
        if (i < 0 || i >= FetchStatusCode.values().length) {
            throw new IndexOutOfBoundsException("Invalid ordinal");
        }
        return FetchStatusCode.values()[i];
    }

    public String toString() {
        if (this == UNFETCHED) {
            return "UNFETCHED";
        } else if (this == FETCHED) {
            return "FETCHED";
        } else if (this == MOVED) {
            return "MOVED";
        } else if (this == ABORTED) {
            return "ABORTED";
        } else if (this == ERROR) {
            return "ERROR";
        } else  {
            return "UNKNOWN STATUS";
        }
    }
}
