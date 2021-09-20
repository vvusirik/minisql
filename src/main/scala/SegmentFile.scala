package minisql.segment_file

import java.io.RandomAccessFile
import minisql.types.Bytes

// TODO: remove inheritance
class SegmentFile(path: String, mode: String) extends RandomAccessFile(path: String, mode: String) {

  def append(value: Bytes): Unit = {
    // 1. Write 8 bytes for the value length
    writeLong(value.length)
    // 2. Write the byte array to the file
    write(value)
  }

  def seek_value(byte_offset: Long): Bytes = {
    // 1. Read 8 bytes to get the value length
    seek(byte_offset)
    val len_byte: Long = readLong()

    // 2. Read the length of bytes specified
    val value_bytes: Bytes = (0L to len_byte).map(x => readByte()).toArray
    value_bytes
  }
}
