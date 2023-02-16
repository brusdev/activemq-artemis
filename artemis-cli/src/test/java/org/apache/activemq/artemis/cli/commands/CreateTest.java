/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.cli.commands;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.apache.activemq.artemis.utils.XmlProvider;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

@RunWith(Parameterized.class)
public class CreateTest {

  private final String testName;
  private String httpHost;
  private boolean relaxJolokia;


  public CreateTest(String testName, String httpHost, boolean relaxJolokia) {
    this.testName = testName;
    this.httpHost = httpHost;
    this.relaxJolokia = relaxJolokia;
  }

  @Parameters
  public static Collection<Object[]> testData() {
    return Arrays.asList(new Object[][]{
        {"Happy path + relaxJolokia", "sampledomain.com", true},
        {"Happy path - relaxJolokia", "sampledomain.net", false},
        {"Domain with dash + relaxJolokia", "sample-domain.co", true},
        {"Domain with dash - relaxJolokia", "sample-domain.co.uk", false},
        {"Domain with double dashes + relaxJolokia", "sample--domain.name", true},
        {"Domain with double dashes - relaxJolokia", "sample--domain.biz", false},
        {"Domain with leading dashes + relaxJolokia", "--sampledomain.company", true},
        {"Domain with leading dashes - relaxJolokia", "--sampledomain.email", false},
        {"Domain with trailing dashes + relaxJolokia", "sampledomain--.shop", true},
        {"Domain with trailing dashes - relaxJolokia", "sampledomain--.java", false},
    });
  }

  @Test
  public void testWriteJolokiaAccessXmlCreatesValidXml() throws Exception {
    Create c = new Create();
    String source = Create.ETC_JOLOKIA_ACCESS_XML;
    HashMap<String, String> filters = new LinkedHashMap<>();

    filters.put("${http.host}", this.httpHost);

    // This is duplicated from Create.java, but it's embedded into the middle of the class.
    // TODO: Refactor that code to it's own method.
    if (this.relaxJolokia) {
      filters.put("${jolokia.options}",
          "<!-- option relax-jolokia used, so strict-checking will be removed here -->");
    } else {
      filters.put("${jolokia.options}",
          "<!-- Check for the proper origin on the server side, too -->\n" +
              "        <strict-checking/>");
    }

    Path temp = Files.createTempFile("jolokia-access-", ".xml");
    try {
      c.write("etc/" + source, temp.toFile(), filters, false, true);

      String xml = Files.readString(temp);
      Assert.assertTrue(testName + " - should be valid, but isn't", isXmlValid(xml));
    } finally {
      Files.delete(temp);
    }
  }

  /**
   * IsXmlValid will check if a given xml string is valid by parsing the xml to create a Document.
   * <p>
   * If it parses, the xml is assumed to be valid. If any exceptions occur, the xml is not valid.
   *
   * @param xml The xml string to check for validity.
   * @return whether the xml string represents a valid xml document.
   */
  private boolean isXmlValid(String xml) {
    try {
      var xmlStream = new ByteArrayInputStream(xml.getBytes());

      DocumentBuilder dbuilder = XmlProvider.newDocumentBuilder();
      Document doc = dbuilder.parse(xmlStream);

    } catch (ParserConfigurationException e) {
      return false;
    } catch (IOException e) {
      return false;
    } catch (SAXException e) {
      return false;
    }
    return true;
  }
}