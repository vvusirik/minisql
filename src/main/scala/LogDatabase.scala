/* Implementation for an append only Log Database with KV getters and setters 
 * TODO
 */

import minisql.types.{HashIndex, Bytes}
import minisql.database.Database
import minisql.segment_file.SegmentFile
import scala.collection.immutable.List


class LogDatabase extends Database {

  def getImpl(key: String, index_segment_pairs: List[(HashIndex, SegmentFile)]): Option[Bytes] = {
    // Find the most recent segment that contains a reference to the key
    val index_segment = index_segment_pairs.reverse.find { 
      case (index, _) => index.contains(key) 
    }

    index_segment match {
      // Index is guaranteed to contain key if the option isn't None
      case Some((index, segment)) => Some(segment.seek_value(index.get(key).get))
      case None => None
    }
  }

  def get(key: String): Option[Bytes] = {
    // TODO: not implemented
    Some(Array())
  }

  def setImpl(key: String, value: Bytes, hash_index: HashIndex, segment_file: SegmentFile): Unit = {
    // 1. Set the key to the current byte offset in the segment file 
    hash_index += (key -> segment_file.length)
    
    // 2. Set the value in the segment file
    segment_file.append(value)
    
  }
  def set(key: String, value: Bytes): Unit = {
  }

  //def merge
  //def compact
}
