package minisql

import collection.immutable.Map

package object types {
  type HashIndex = Map[String, Long]
  type Bytes = Array[Byte]
}

