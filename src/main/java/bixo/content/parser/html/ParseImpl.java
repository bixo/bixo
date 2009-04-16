/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bixo.content.parser.html;


/** The result of parsing a page's raw content.
 * @see Parser#getParse(Content)
 */
public class ParseImpl implements Parse {
  private ParseText text;
  private ParseData data;
  private boolean isCanonical;

  public ParseImpl() {}

  public ParseImpl(Parse parse) {
    this(new ParseText(parse.getText()), parse.getData(), true);
  }

  public ParseImpl(String text, ParseData data) {
    this(new ParseText(text), data, true);
  }
  
  public ParseImpl(ParseText text, ParseData data) {
    this(text, data, true);
  }

  public ParseImpl(ParseText text, ParseData data, boolean isCanonical) {
    this.text = text;
    this.data = data;
    this.isCanonical = isCanonical;
  }

  public String getText() { return text.getText(); }

  public ParseData getData() { return data; }

  public boolean isCanonical() { return isCanonical; }
  
}
