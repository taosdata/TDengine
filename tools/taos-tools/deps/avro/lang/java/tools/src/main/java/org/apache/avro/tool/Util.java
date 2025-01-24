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
 *
 * The section demarcated by 'copied from Apache commons-codec' is
 * from Apache Commons Codec v1.9.
 */
package org.apache.avro.tool;

import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.Deflater;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import joptsimple.OptionSet;
import joptsimple.OptionParser;
import joptsimple.OptionSpec;

/** Static utility methods for tools. */
class Util {
  /**
   * Returns stdin if filename is "-", else opens the File in the owning
   * filesystem and returns an InputStream for it. Relative paths will be opened
   * in the default filesystem.
   *
   * @param filename The filename to be opened
   * @throws IOException
   */
  static BufferedInputStream fileOrStdin(String filename, InputStream stdin) throws IOException {
    return new BufferedInputStream(filename.equals("-") ? stdin : openFromFS(filename));
  }

  /**
   * Returns stdout if filename is "-", else opens the file from the owning
   * filesystem and returns an OutputStream for it. Relative paths will be opened
   * in the default filesystem.
   *
   * @param filename The filename to be opened
   * @throws IOException
   */
  static BufferedOutputStream fileOrStdout(String filename, OutputStream stdout) throws IOException {
    return new BufferedOutputStream(filename.equals("-") ? stdout : createFromFS(filename));
  }

  /**
   * Returns an InputStream for the file using the owning filesystem, or the
   * default if none is given.
   *
   * @param filename The filename to be opened
   * @throws IOException
   */
  static InputStream openFromFS(String filename) throws IOException {
    Path p = new Path(filename);
    return p.getFileSystem(new Configuration()).open(p);
  }

  /**
   * Returns an InputStream for the file using the owning filesystem, or the
   * default if none is given.
   *
   * @param filename The filename to be opened
   * @throws IOException
   */
  static InputStream openFromFS(Path filename) throws IOException {
    return filename.getFileSystem(new Configuration()).open(filename);
  }

  /**
   * Returns a seekable FsInput using the owning filesystem, or the default if
   * none is given.
   *
   * @param filename The filename to be opened
   * @throws IOException
   */
  static FsInput openSeekableFromFS(String filename) throws IOException {
    return new FsInput(new Path(filename), new Configuration());
  }

  /**
   * Opens the file for writing in the owning filesystem, or the default if none
   * is given.
   *
   * @param filename The filename to be opened.
   * @return An OutputStream to the specified file.
   * @throws IOException
   */
  static OutputStream createFromFS(String filename) throws IOException {
    Path p = new Path(filename);
    return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
  }

  /**
   * Closes the inputstream created from {@link Util.fileOrStdin} unless it is
   * System.in.
   *
   * @param in The inputstream to be closed.
   */
  static void close(InputStream in) {
    if (!System.in.equals(in)) {
      try {
        in.close();
      } catch (IOException e) {
        System.err.println("could not close InputStream " + in.toString());
      }
    }
  }

  /**
   * Closes the outputstream created from {@link Util.fileOrStdout} unless it is
   * System.out.
   *
   * @param out The outputStream to be closed.
   */
  static void close(OutputStream out) {
    if (!System.out.equals(out)) {
      try {
        out.close();
      } catch (IOException e) {
        System.err.println("could not close OutputStream " + out.toString());
      }
    }
  }

  /**
   * Parses a schema from the specified file.
   *
   * @param filename The file name to parse
   * @return The parsed schema
   * @throws IOException
   */
  static Schema parseSchemaFromFS(String filename) throws IOException {
    InputStream stream = openFromFS(filename);
    try {
      return new Schema.Parser().parse(stream);
    } finally {
      close(stream);
    }
  }

  /**
   * If pathname is a file, this method returns a list with a single absolute Path
   * to that file. If pathname is a directory, this method returns a list of
   * Pathes to all the files within this directory. Only files inside that
   * directory are included, no subdirectories or files in subdirectories will be
   * added. If pathname is a glob pattern, all files matching the pattern are
   * included.
   *
   * The List is sorted alphabetically.
   *
   * @param fileOrDirName filename, directoryname or a glob pattern
   * @return A Path List
   * @throws IOException
   */
  static List<Path> getFiles(String fileOrDirName) throws IOException {
    List<Path> pathList = new ArrayList<>();
    Path path = new Path(fileOrDirName);
    FileSystem fs = path.getFileSystem(new Configuration());

    if (fs.isFile(path)) {
      pathList.add(path);
    } else if (fs.isDirectory(path)) {
      for (FileStatus status : fs.listStatus(path)) {
        if (!status.isDirectory()) {
          pathList.add(status.getPath());
        }
      }
    } else {
      FileStatus[] fileStatuses = fs.globStatus(path);
      if (fileStatuses != null) {
        for (FileStatus status : fileStatuses) {
          pathList.add(status.getPath());
        }
      } else {
        throw new FileNotFoundException(fileOrDirName);
      }
    }
    Collections.sort(pathList);
    return pathList;
  }

  /**
   * Concatenate the result of {@link #getFiles(String)} applied to all file or
   * directory names. The list is sorted alphabetically and contains no
   * subdirectories or files within those.
   *
   * The list is sorted alphabetically.
   *
   * @param fileOrDirNames A list of filenames, directorynames or glob patterns
   * @return A list of Paths, one for each file
   * @throws IOException
   */
  static List<Path> getFiles(List<String> fileOrDirNames) throws IOException {
    ArrayList<Path> pathList = new ArrayList<>(fileOrDirNames.size());
    for (String name : fileOrDirNames) {
      pathList.addAll(getFiles(name));
    }
    Collections.sort(pathList);
    return pathList;
  }

  /**
   * Converts a String JSON object into a generic datum.
   *
   * This is inefficient (creates extra objects), so should be used sparingly.
   */
  static Object jsonToGenericDatum(Schema schema, String jsonData) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
    Object datum = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonData));
    return datum;
  }

  /** Reads and returns the first datum in a data file. */
  static Object datumFromFile(Schema schema, String file) throws IOException {
    try (DataFileReader<Object> in = new DataFileReader<>(new File(file), new GenericDatumReader<>(schema))) {
      return in.next();
    }
  }

  static OptionSpec<String> compressionCodecOption(OptionParser optParser) {
    return optParser.accepts("codec", "Compression codec").withRequiredArg().ofType(String.class)
        .defaultsTo(DEFLATE_CODEC);
  }

  static OptionSpec<String> compressionCodecOptionWithDefault(OptionParser optParser, String s) {
    return optParser.accepts("codec", "Compression codec").withRequiredArg().ofType(String.class).defaultsTo(s);
  }

  static OptionSpec<Integer> compressionLevelOption(OptionParser optParser) {
    return optParser.accepts("level", "Compression level (only applies to deflate, xz, and zstandard)")
        .withRequiredArg().ofType(Integer.class).defaultsTo(Deflater.DEFAULT_COMPRESSION);
  }

  static CodecFactory codecFactory(OptionSet opts, OptionSpec<String> codec, OptionSpec<Integer> level) {
    return codecFactory(opts, codec, level, DEFLATE_CODEC);
  }

  static CodecFactory codecFactory(OptionSet opts, OptionSpec<String> codec, OptionSpec<Integer> level,
      String defaultCodec) {
    String codecName = opts.hasArgument(codec) ? codec.value(opts) : defaultCodec;
    if (codecName.equals(DEFLATE_CODEC)) {
      return CodecFactory.deflateCodec(level.value(opts));
    } else if (codecName.equals(DataFileConstants.XZ_CODEC)) {
      return CodecFactory.xzCodec(level.value(opts));
    } else if (codecName.equals(DataFileConstants.ZSTANDARD_CODEC)) {
      return CodecFactory.zstandardCodec(level.value(opts));
    } else {
      return CodecFactory.fromString(codec.value(opts));
    }
  }

  // Below copied from Apache commons-codec version 1.9
  // org.apache.commons.codec.binary.Hex, see NOTICE.
  /**
   * Used to build output as Hex
   */
  private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
      'e', 'f' };

  /**
   * Converts an array of bytes into an array of characters representing the
   * hexadecimal values of each byte in order. The returned array will be double
   * the length of the passed array, as it takes two characters to represent any
   * given byte.
   *
   * @param data     a byte[] to convert to Hex characters
   * @param toDigits the output alphabet
   * @return A char[] containing hexadecimal characters
   */
  static String encodeHex(final byte[] data) {
    final int l = data.length;
    final char[] out = new char[l << 1];
    // two characters form the hex value.
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = DIGITS_LOWER[(0xF0 & data[i]) >>> 4];
      out[j++] = DIGITS_LOWER[0x0F & data[i]];
    }
    return new String(out);
  }
  // end copied from Apache commons-codec
}
