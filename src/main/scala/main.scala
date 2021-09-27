package minisql

import minisql.logdb.LogDatabase

object Application {
  def main(args: Array[String]): Unit = {
    require(args.size == 3)
    println(args(1))
    println(args(2).getBytes())

    // Load the log database 
    val db = LogDatabase("/home/vvusirik/Projects/minisql/data/hash_index/", "/home/vvusirik/Projects/minisql/data/segments/")
    
  } 
}

