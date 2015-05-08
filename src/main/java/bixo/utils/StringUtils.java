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
package bixo.utils;

import java.util.ArrayList;

public class StringUtils {
	private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	private StringUtils() {
        // Enforce class isn't instantiated
	}

	/**
	 * Convenience call for {@link #toHexString(byte[], String, int)}, where
	 * <code>sep = null; lineLen = Integer.MAX_VALUE</code>.
	 * 
	 * @param buf
	 */
	public static String toHexString(byte[] buf) {
		return toHexString(buf, null, Integer.MAX_VALUE);
	}

	/**
	 * Get a text representation of a byte[] as hexadecimal String, where each
	 * pair of hexadecimal digits corresponds to consecutive bytes in the array.
	 * 
	 * @param buf
	 *            input data
	 * @param sep
	 *            separate every pair of hexadecimal digits with this separator,
	 *            or null if no separation is needed.
	 * @param lineLen
	 *            break the output String into lines containing output for
	 *            lineLen bytes.
	 */
	public static String toHexString(byte[] buf, String sep, int lineLen) {
		if (buf == null)
			return null;
		if (lineLen <= 0)
			lineLen = Integer.MAX_VALUE;
		StringBuffer res = new StringBuffer(buf.length * 2);
		for (int i = 0; i < buf.length; i++) {
			int b = buf[i];
			res.append(HEX_DIGITS[(b >> 4) & 0xf]);
			res.append(HEX_DIGITS[b & 0xf]);
			if (i > 0 && (i % lineLen) == 0)
				res.append('\n');
			else if (sep != null && i < lineLen - 1)
				res.append(sep);
		}
		return res.toString();
	}

	/**
	 * Convert a String containing consecutive (no inside whitespace)
	 * hexadecimal digits into a corresponding byte array. If the number of
	 * digits is not even, a '0' will be appended in the front of the String
	 * prior to conversion. Leading and trailing whitespace is ignored.
	 * 
	 * @param text
	 *            input text
	 * @return converted byte array, or null if unable to convert
	 */
	public static byte[] fromHexString(String text) {
		text = text.trim();
		if (text.length() % 2 != 0)
			text = "0" + text;
		int resLen = text.length() / 2;
		int loNibble, hiNibble;
		byte[] res = new byte[resLen];
		for (int i = 0; i < resLen; i++) {
			int j = i << 1;
			hiNibble = charToNibble(text.charAt(j));
			loNibble = charToNibble(text.charAt(j + 1));
			if (loNibble == -1 || hiNibble == -1)
				return null;
			res[i] = (byte) (hiNibble << 4 | loNibble);
		}
		return res;
	}

    // Do our own version of String.split(), which returns every section even if
    // it's empty. This then satisfies what we need, namely:
    //
    // "a=b" => "a" "b"
    // "" => ""
    // "=" => "" ""
    // "a=" => "a" ""
    // "a==" => "a" "" ""
    public static String[] splitOnChar(String str, char c) {
        ArrayList<String> result = new ArrayList<String>();
        
        int lastOffset = 0;
        int curOffset;
        while ((curOffset = str.indexOf(c, lastOffset)) != -1) {
            result.add(str.substring(lastOffset, curOffset));
            lastOffset = curOffset + 1;
        }
        
        result.add(str.substring(lastOffset));
        
        return result.toArray(new String[result.size()]);
    }


	private static final int charToNibble(char c) {
		if (c >= '0' && c <= '9') {
			return c - '0';
		} else if (c >= 'a' && c <= 'f') {
			return 0xa + (c - 'a');
		} else if (c >= 'A' && c <= 'F') {
			return 0xA + (c - 'A');
		} else {
			return -1;
		}
	}

}
