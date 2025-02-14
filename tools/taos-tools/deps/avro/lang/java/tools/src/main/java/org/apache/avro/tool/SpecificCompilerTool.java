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
package org.apache.avro.tool;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility;

/**
 * A Tool for compiling avro protocols or schemas to Java classes using the Avro
 * SpecificCompiler.
 */

public class SpecificCompilerTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> origArgs) throws Exception {
    if (origArgs.size() < 3) {
      System.err
          .println("Usage: [-encoding <outputencoding>] [-string] [-bigDecimal] [-fieldVisibility <visibilityType>] "
              + "[-noSetters] [-addExtraOptionalGetters] [-optionalGetters <optionalGettersType>] "
              + "[-templateDir <templateDir>] (schema|protocol) input... outputdir");
      System.err.println(" input - input files or directories");
      System.err.println(" outputdir - directory to write generated java");
      System.err.println(" -encoding <outputencoding> - set the encoding of " + "output file(s)");
      System.err.println(" -string - use java.lang.String instead of Utf8");
      System.err.println(" -fieldVisibility [private|public] - use either and default private");
      System.err.println(" -noSetters - do not generate setters");
      System.err
          .println(" -addExtraOptionalGetters - generate extra getters with this format: 'getOptional<FieldName>'");
      System.err.println(
          " -optionalGetters [all_fields|only_nullable_fields]- generate getters returning Optional<T> for all fields or only for nullable fields");
      System.err
          .println(" -bigDecimal - use java.math.BigDecimal for " + "decimal type instead of java.nio.ByteBuffer");
      System.err.println(" -templateDir - directory with custom Velocity templates");
      return 1;
    }

    CompilerOptions compilerOpts = new CompilerOptions();
    compilerOpts.stringType = StringType.CharSequence;
    compilerOpts.useLogicalDecimal = false;
    compilerOpts.createSetters = true;
    compilerOpts.optionalGettersType = Optional.empty();
    compilerOpts.addExtraOptionalGetters = false;
    compilerOpts.encoding = Optional.empty();
    compilerOpts.templateDir = Optional.empty();
    compilerOpts.fieldVisibility = Optional.empty();

    List<String> args = new ArrayList<>(origArgs);

    if (args.contains("-noSetters")) {
      compilerOpts.createSetters = false;
      args.remove(args.indexOf("-noSetters"));
    }

    if (args.contains("-addExtraOptionalGetters")) {
      compilerOpts.addExtraOptionalGetters = true;
      args.remove(args.indexOf("-addExtraOptionalGetters"));
    }
    int arg = 0;

    if (args.contains("-optionalGetters")) {
      arg = args.indexOf("-optionalGetters") + 1;
      try {
        compilerOpts.optionalGettersType = Optional
            .of(OptionalGettersType.valueOf(args.get(arg).toUpperCase(Locale.ENGLISH)));
      } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
        System.err.println("Expected one of" + Arrays.toString(OptionalGettersType.values()));
        return 1;
      }
      args.remove(arg);
      args.remove(arg - 1);
    }

    if (args.contains("-encoding")) {
      arg = args.indexOf("-encoding") + 1;
      compilerOpts.encoding = Optional.of(args.get(arg));
      args.remove(arg);
      args.remove(arg - 1);
    }

    if (args.contains("-string")) {
      compilerOpts.stringType = StringType.String;
      args.remove(args.indexOf("-string"));
    }

    if (args.contains("-fieldVisibility")) {
      arg = args.indexOf("-fieldVisibility") + 1;
      try {
        compilerOpts.fieldVisibility = Optional.of(FieldVisibility.valueOf(args.get(arg).toUpperCase(Locale.ENGLISH)));
      } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
        System.err.println("Expected one of" + Arrays.toString(FieldVisibility.values()));
        return 1;
      }
      args.remove(arg);
      args.remove(arg - 1);
    }
    if (args.contains("-templateDir")) {
      arg = args.indexOf("-templateDir") + 1;
      compilerOpts.templateDir = Optional.of(args.get(arg));
      args.remove(arg);
      args.remove(arg - 1);
    }

    arg = 0;
    if ("-bigDecimal".equalsIgnoreCase(args.get(arg))) {
      compilerOpts.useLogicalDecimal = true;
      arg++;
    }

    String method = args.get(arg);
    List<File> inputs = new ArrayList<>();
    File output = new File(args.get(args.size() - 1));

    for (int i = arg + 1; i < args.size() - 1; i++) {
      inputs.add(new File(args.get(i)));
    }

    if ("schema".equals(method)) {
      Schema.Parser parser = new Schema.Parser();
      for (File src : determineInputs(inputs, SCHEMA_FILTER)) {
        Schema schema = parser.parse(src);
        final SpecificCompiler compiler = new SpecificCompiler(schema);
        executeCompiler(compiler, compilerOpts, src, output);
      }
    } else if ("protocol".equals(method)) {
      for (File src : determineInputs(inputs, PROTOCOL_FILTER)) {
        Protocol protocol = Protocol.parse(src);
        final SpecificCompiler compiler = new SpecificCompiler(protocol);
        executeCompiler(compiler, compilerOpts, src, output);
      }
    } else {
      System.err.println("Expected \"schema\" or \"protocol\".");
      return 1;
    }
    return 0;
  }

  private void executeCompiler(SpecificCompiler compiler, CompilerOptions opts, File src, File output)
      throws IOException {
    compiler.setStringType(opts.stringType);
    compiler.setCreateSetters(opts.createSetters);

    opts.optionalGettersType.ifPresent(choice -> {
      compiler.setGettersReturnOptional(true);
      switch (choice) {
      case ALL_FIELDS:
        compiler.setOptionalGettersForNullableFieldsOnly(false);
        break;
      case ONLY_NULLABLE_FIELDS:
        compiler.setOptionalGettersForNullableFieldsOnly(true);
        break;
      default:
        throw new IllegalStateException("Unsupported value '" + choice + "'");
      }
    });

    compiler.setCreateOptionalGetters(opts.addExtraOptionalGetters);
    opts.templateDir.ifPresent(compiler::setTemplateDir);
    compiler.setEnableDecimalLogicalType(opts.useLogicalDecimal);
    opts.encoding.ifPresent(compiler::setOutputCharacterEncoding);
    opts.fieldVisibility.ifPresent(compiler::setFieldVisibility);
    compiler.compileToDestination(src, output);
  }

  @Override
  public String getName() {
    return "compile";
  }

  @Override
  public String getShortDescription() {
    return "Generates Java code for the given schema.";
  }

  /**
   * For an Array of files, sort using {@link String#compareTo(String)} for each
   * filename.
   *
   * @param files Array of File objects to sort
   * @return the sorted File array
   */
  private static File[] sortFiles(File[] files) {
    Objects.requireNonNull(files, "files cannot be null");
    Arrays.sort(files, Comparator.comparing(File::getName));
    return files;
  }

  /**
   * For a List of files or directories, returns a File[] containing each file
   * passed as well as each file with a matching extension found in the directory.
   * Each directory is sorted using {@link String#compareTo(String)} for each
   * filename.
   *
   * @param inputs List of File objects that are files or directories
   * @param filter File extension filter to match on when fetching files from a
   *               directory
   * @return Unique array of files
   */
  private static File[] determineInputs(List<File> inputs, FilenameFilter filter) {
    Set<File> fileSet = new LinkedHashSet<>(); // preserve order and uniqueness

    for (File file : inputs) {
      // if directory, look at contents to see what files match extension
      if (file.isDirectory()) {
        File[] files = file.listFiles(filter);
        // sort files in directory to compile deterministically
        // independent of system/ locale
        Collections.addAll(fileSet, files != null ? sortFiles(files) : new File[0]);
      }
      // otherwise, just add the file.
      else {
        fileSet.add(file);
      }
    }

    if (fileSet.size() > 0) {
      System.err.println("Input files to compile:");
      for (File file : fileSet) {
        System.err.println("  " + file);
      }
    } else {
      System.err.println("No input files found.");
    }

    return fileSet.toArray(new File[0]);
  }

  private static final FileExtensionFilter SCHEMA_FILTER = new FileExtensionFilter("avsc");
  private static final FileExtensionFilter PROTOCOL_FILTER = new FileExtensionFilter("avpr");

  private static class CompilerOptions {
    Optional<String> encoding;
    StringType stringType;
    Optional<FieldVisibility> fieldVisibility;
    boolean useLogicalDecimal;
    boolean createSetters;
    boolean addExtraOptionalGetters;
    Optional<OptionalGettersType> optionalGettersType;
    Optional<String> templateDir;
  }

  private enum OptionalGettersType {
    ALL_FIELDS, ONLY_NULLABLE_FIELDS
  }

  private static class FileExtensionFilter implements FilenameFilter {
    private String extension;

    private FileExtensionFilter(String extension) {
      this.extension = extension;
    }

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(this.extension);
    }
  }
}
