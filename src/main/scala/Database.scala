package minisql.database

import minisql.types.{Bytes}

// TODO: use bytes for key as well
trait Database {
  def get(key: String): Option[Bytes] 
  def set(key: String, value: Bytes): Unit 
}
