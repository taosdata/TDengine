/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for files containing test cases consisting of
 * <code>&lt;String,String&gt;</code> pairs, where the first string is the input
 * to the test case, and the second string is the expected output of the test
 * case.
 *
 * <p>
 * A test-case file is a sequence of
 * <a href="en.wikipedia.org/wiki/Here_document">here documents</a>
 * ("heredocs"), very similar in syntax to Unix Shell heredocs. Heredocs labeled
 * "INPUT" indicate the start of a new case, and these INPUT heredocs the inputs
 * of test cases. Following an "INPUT" heredoc can more zero or more
 * "expected-output" heredocs. Each of these expected-output heredocs defines
 * what we call a <dfn>subcase</dfn>. The assumption here is that for each
 * interesting test input, there are often multiple different tests one could
 * run, each with different expected outputs.
 *
 * <p>
 * Consumers of this class call the {@link #find} method to find all subcases
 * marked with a given label. For example, imagine the following test-case file:
 * <blockquote>
 * 
 * <pre>
 *    &lt;&lt;INPUT 0
 *    &lt;&lt;VALUE 0
 *    &lt;&lt;PPRINT 0
 *    &lt;&lt;INPUT 1+1
 *    &lt;&lt;VALUE 2
 *    &lt;&lt;PPRINT 1 + 1
 *    &lt;&lt;SEXP (+ 1 1)
 *    SEXP
 * </pre>
 * 
 * </blockquote> Calling {@link #find} on the label "VALUE" will return two test
 * cases, the pair <code>&lt;"0","0"&gt;</code> and
 * <code>&lt;"1+1","2"&gt;</code>. Calling it on the label "PPRINT" will return
 * <code>&lt;"0","0"&gt;</code> and <code>&lt;"1+1","1 +
 * 1"&gt;</code>. Notice that there need not be a subcase for every INPUT. In
 * the case of "SEXP", for example, {@link #find} will return only the single
 * pair <code>&lt;"1+1","(+ 1 1)"&gt;</code>.
 *
 * <p>
 * There are two forms of heredocs, single-line and multi-line. The examples
 * above (except "SEXP") are single-line heredocs. The general syntax for these
 * is: <blockquote>
 * 
 * <pre>
 * ^&lt;&lt;([a-zA-Z][_a-zA-Z0-9]*) (.*)$
 * </pre>
 * 
 * </blockquote> The first group in this regex is the label of the heredoc, and
 * the second group is the text of the heredoc. A single space separates the two
 * groups and is not part of there heredoc (subsequent spaces <em>will</em> be
 * included in the heredoc). A "line terminator" as defined by the Java language
 * (i.e., CR, LR, or CR followed by LF) terminates a singline-line heredoc but
 * is not included in the text of the heredoc.
 *
 * <p>
 * As the name implies, multi-line heredocs are spread across multiple lines, as
 * in this example: <blockquote>
 * 
 * <pre>
 *    &lt;&lt;INPUT
 *    1
 *    +1 +
 *    1
 *    INPUT
 *    &lt;&lt;VALUE 3
 *    &lt;&lt;PPRINT 1 + 1 + 1
 * </pre>
 * 
 * </blockquote> In this case, the input to the test case is spread across
 * multiple lines (the line terminators in these documents are preserved as part
 * of the document text). Multi-line heredocs can be used for both the inputs of
 * text cases and the expected outputs of them.
 * 
 * <p>
 * The syntax of multi-line heredocs obey the following pseudo-regex:
 * <blockquote>
 * 
 * <pre>
 * ^&lt;&lt;([a-zA-Z][_a-zA-Z0-9]*)$(.*)$^\1$
 * </pre>
 * 
 * </blockquote> That is, as illustrated by the example, a multi-line heredoc
 * named "LABEL" consists of the text <code>&lt;lt;LABEL</code> on a line by
 * itself, followed by the text of the heredoc, followed by the text
 * <code>LABEL</code> on a line by itself (if LABEL starts a line but is not the
 * <em>only</em> text on that line, then that entire line is part of the
 * heredoc, and the heredoc is not terminated by that line).
 *
 * <p>
 * In multi-line heredocs, neither the line terminator that terminates the start
 * of the document, nor the one just before the label that ends the heredoc, are
 * part of the text of the heredoc. Thus, for example, the text of the
 * multi-line input from above would be exactly <code>"1\n+1&nbsp;+\n1"</code>.
 * If you want a new line at the end of a multi-line heredoc, put a blank line
 * before the label ending the heredoc.
 *
 * <p>
 * Also in multi-line heredocs, line-terminators within the heredoc are
 * normalized to line-feeds ('\n'). Thus, for example, when a test file written
 * on a Windows machine is parsed on any machine, the Windows-style line
 * terminators within heredocs will be translated to Unix-style line
 * terminators, no matter what platform the tests are run on.
 *
 * <p>
 * Note that lines between heredocs are ignored, and can be used to provide
 * spacing between and/or commentary on the test cases.
 */
public class CaseFinder {
  /**
   * Scan test-case file <code>in</code> looking for test subcases marked with
   * <code>caseLabel</code>. Any such cases are appended (in order) to the "cases"
   * parameter. If <code>caseLabel</code> equals the string <code>"INPUT"</code>,
   * then returns the list of &lt;<i>input</i>, <code>null</code>&gt; pairs for
   * <i>input</i> equal to all heredoc's named INPUT's found in the input stream.
   */
  public static List<Object[]> find(BufferedReader in, String label, List<Object[]> cases) throws IOException {
    if (!Pattern.matches(LABEL_REGEX, label))
      throw new IllegalArgumentException("Bad case subcase label: " + label);

    final String subcaseMarker = "<<" + label;

    for (String line = in.readLine();;) {
      // Find next new case
      while (line != null && !line.startsWith(NEW_CASE_MARKER))
        line = in.readLine();
      if (line == null)
        break;
      String input;
      input = processHereDoc(in, line);

      if (label.equals(NEW_CASE_NAME)) {
        cases.add(new Object[] { input, null });
        line = in.readLine();
        continue;
      }

      // Check to see if there's a subcase named "label" for that case
      do {
        line = in.readLine();
      } while (line != null && (!line.startsWith(NEW_CASE_MARKER) && !line.startsWith(subcaseMarker)));
      if (line == null || line.startsWith(NEW_CASE_MARKER))
        continue;
      String expectedOutput = processHereDoc(in, line);

      cases.add(new Object[] { input, expectedOutput });
    }
    in.close();
    return cases;
  }

  private static final String NEW_CASE_NAME = "INPUT";
  private static final String NEW_CASE_MARKER = "<<" + NEW_CASE_NAME;
  private static final String LABEL_REGEX = "[a-zA-Z][_a-zA-Z0-9]*";
  private static final Pattern START_LINE_PATTERN = Pattern.compile("^<<(" + LABEL_REGEX + ")(.*)$");

  /**
   * Reads and returns content of a heredoc. Assumes we just read a
   * start-of-here-doc marker for a here-doc labeled "docMarker." Replaces
   * arbitrary newlines with system newlines, but strips newline from final line
   * of heredoc. Throws IOException if EOF is reached before heredoc is terminate.
   */
  private static String processHereDoc(BufferedReader in, String docStart) throws IOException {
    Matcher m = START_LINE_PATTERN.matcher(docStart);
    if (!m.matches())
      throw new IllegalArgumentException("Wasn't given the start of a heredoc (\"" + docStart + "\")");
    String docName = m.group(1);

    // Determine if this is a single-line heredoc, and process if it is
    String singleLineText = m.group(2);
    if (singleLineText.length() != 0) {
      if (!singleLineText.startsWith(" "))
        throw new IOException("Single-line heredoc missing initial space (\"" + docStart + "\")");
      return singleLineText.substring(1);
    }

    // Process multi-line heredocs
    StringBuilder result = new StringBuilder();
    String line = in.readLine();
    String prevLine = "";
    boolean firstTime = true;
    while (line != null && !line.equals(docName)) {
      if (!firstTime)
        result.append(prevLine).append('\n');
      else
        firstTime = false;
      prevLine = line;
      line = in.readLine();
    }
    if (line == null)
      throw new IOException("Here document (" + docName + ") terminated by end-of-file.");
    return result.append(prevLine).toString();
  }
}
