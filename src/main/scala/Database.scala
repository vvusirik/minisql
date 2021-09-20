package minisql.database

import minisql.types.{Bytes}

trait Database {
  def get(key: String): Option[Bytes] 
  def set(key: String, value: Bytes): Unit 
}
