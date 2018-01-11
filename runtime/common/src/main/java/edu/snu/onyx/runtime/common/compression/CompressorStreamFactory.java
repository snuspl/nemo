package edu.snu.onyx.runtime.common.compression;

import edu.snu.onyx.common.ir.vertex.executionproperty.CompressionProperty.Compressor;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressorStreamFactory {
  public CompressorStreamFactory() {

  }

  OutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    switch (compressor) {
      case Raw:
        return new BufferedOutputStream(out);
      case Gzip:
        return new GZIPOutputStream(out);
      case LZ4:
        return null; // added later
      default:
        return null;
    }
  }

  InputStream createInputStream(InputStream in, Compressor compressor) throws IOException {
    switch (compressor) {
      case Raw:
        return new BufferedInputStream(in);
      case Gzip:
        return new GZIPInputStream(in);
      case LZ4:
        return null; // added later
      default:
        return null;
    }
  }
}
