package de.sciss.bdb

import java.io.File
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import collection.JavaConversions._

object DPLTest {
   sealed trait CMD
   case object CMD_CREATE  extends CMD
   case object CMD_LIST    extends CMD
   case object CMD_DELETE  extends CMD

   def main( args: Array[ String ]) {
      val cmd = args.headOption match {
         case Some( "-w" ) => CMD_CREATE
         case Some( "-d" ) => CMD_DELETE
         case _            => CMD_LIST
      }
      test( cmd )
   }

   def test( cmd: CMD ) {
      val envCfg = new EnvironmentConfig()
         .setAllowCreate( cmd == CMD_CREATE )
      val storeCfg = new StoreConfig()
         .setAllowCreate( cmd == CMD_CREATE )
      try {
         val dir  = new File( "db_dpl" )
         dir.mkdirs()
         val env  = new Environment( dir, envCfg )
         try {
            val store = new EntityStore( env, "EntityStore", storeCfg )
            try {
               cmd match {
                  case CMD_CREATE =>
                     writeSumdn( store )
                  case CMD_LIST =>
                     readSumdn( store )
                     reportMisses( env )
                     readSumdn( store )
                     reportMisses( env )
                  case CMD_DELETE =>
                     deleteSumdn( store )
               }
            } finally {
               store.close()
            }
         } finally {
//            env.cleanLog()
            env.close()
         }
      } catch {
         case e =>
            e.printStackTrace()
            System.exit( 1 )
      }
   }

   def reportMisses( env: Environment ) {
      println( "Misses = " + env.getStats( null ).getNCacheMiss )
   }

   def writeSumdn( store: EntityStore ) {
      val idx = getPrimaryIndex( store )
      List( "Alpha", "Beta", "Gamma", "Delta" ).zipWithIndex.foreach {
         case (name, id) =>
            val e = new MyEntity( id, name )
            idx.put( e )
            println( "Inserted: " + e )
      }
   }

   def getPrimaryIndex( store: EntityStore ) = store.getPrimaryIndex( classOf[ java.lang.Long ], classOf[ MyEntity ])

   def deleteSumdn( store: EntityStore ) {
      val idx = getPrimaryIndex( store )
      val csr = idx.entities()
      try {
         csr.headOption.foreach { e =>
            idx.delete( e.id )
            println( "Deleted: " + e )
         }
      } finally {
         csr.close()
      }
   }

   def readSumdn( store: EntityStore ) {
      val idx  = getPrimaryIndex( store )
//      (0L to 1L).foreach { id =>
//         val e =  idx.get( id )
//         println( "Found : " + e )
//      }
      val csr = idx.entities()
      try {
         csr.foreach( e => println( "Found: " + e ))
      } finally {
         csr.close()
      }
   }
}