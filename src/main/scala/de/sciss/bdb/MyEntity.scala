package de.sciss.bdb

import com.sleepycat.persist.model.{PrimaryKey, Entity}

@Entity class MyEntity {
   @PrimaryKey var id: java.lang.Long = _
   var name: String = _

   def this( _id: Long, _name: String ) {
      this()
      id = _id
      name = _name
   }

   override def toString = "MyEntity(" + id + ", " + name + ")"
}