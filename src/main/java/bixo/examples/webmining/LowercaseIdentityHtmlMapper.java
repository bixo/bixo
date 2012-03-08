/*
 * Copyright (c) 2009-2012 Scale Unlimited
 *
 * All rights reserved.
 *
 */
package bixo.examples.webmining;

import java.io.Serializable;

import org.apache.tika.parser.html.HtmlMapper;

@SuppressWarnings("serial")
public class LowercaseIdentityHtmlMapper implements HtmlMapper, Serializable  {

    public boolean isDiscardElement(String name) {
        return false;
    }

    public String mapSafeAttribute(String elementName, String attributeName) {
        return attributeName.toLowerCase();
    }

    public String mapSafeElement(String name) {
        return name.toLowerCase();
    }
}
