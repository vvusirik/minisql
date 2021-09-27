/* Implementation for an append only Log Database with KV getters and setters 
 * TODO
 */

package minisql.logdb

import minisql.types.{HashIndex, Bytes}
import collection.mutable.HashMap
import minisql.database.Database
import minisql.segment_file.SegmentFile
import scala.collection.immutable.List
import java.io.File
import scala.io.Source


// TODO: just single index for now, expand to more next
class LogDatabase(var index_segment: (HashIndex, SegmentFile)) extends Database {

  def get(key: String): Option[Bytes] = {
    LogDatabase.getImpl(key, List(index_segment))
  }

  def set(key: String, value: Bytes): Unit = {
    // TODO: create a new segment file and index based on some condition like size
    LogDatabase.setImpl(key, value, index_segment._1, index_segment._2)
  }

  // TODO
  //def merge
  //def compact
}

object LogDatabase {
  private def dirFileList(dir_path: String): List[File] = new File(dir_path).listFiles.filter(_.isFile).toList
  private def indexFileToHashIndex(index_file: File): Map[String, Long] = (
    Source.fromFile(index_file)
      .getLines
      .map(_.split(','))
      .map(l => l(0) -> l(1).toLong)
      .toMap[String, Long]
  )

  def apply(index_dir_path: String, segment_file_dir_path: String): LogDatabase = {
    val segment_files = dirFileList(segment_file_dir_path).map(f => new SegmentFile(f.getAbsolutePath, "rw"))
    val hash_indices = dirFileList(index_dir_path).map(indexFileToHashIndex)
    // TODO ordering
    // TODO handle empty db
    require(hash_indices.size == segment_files.size)
    new LogDatabase((hash_indices.last, segment_files.last))
 }

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

  def setImpl(key: String, value: Bytes, hash_index: HashIndex, segment_file: SegmentFile): HashIndex = {
    // 1. Set the key to the current byte offset in the segment file 
    val new_index = hash_index + (key -> segment_file.length)
    
    // 2. Set the value in the segment file
    segment_file.append(value)

    new_index
  }

}
