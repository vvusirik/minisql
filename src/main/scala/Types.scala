package minisql

import scala.collection.mutable.HashMap

package object types {
  type HashIndex = HashMap[String, Long]
  type Bytes = Array[Byte]
}

