package de.sciss.bdb

import java.io.File
import com.sleepycat.je.{Database, EnvironmentConfig, DatabaseConfig, Environment}
import Bind._

object BaseTest {
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
      val dbCfg = new DatabaseConfig()
         .setAllowCreate( cmd == CMD_CREATE )
      try {
         val dir  = new File( "db_base" )
         dir.mkdirs()
         val env  = new Environment( dir, envCfg )
         try {
            val db   = env.openDatabase( null, "Database", dbCfg )
            try {
               cmd match {
                  case CMD_CREATE =>
                     writeSumdn( db )
                  case CMD_LIST =>
                     readSumdn( db )
                  case CMD_DELETE =>
                     deleteSumdn( db )
               }
            } finally {
               db.close()
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

   def writeSumdn( db: Database ) {
      List( "Alpha", "Beta", "Gamma", "Delta" ).zipWithIndex.foreach {
         case (name, id) =>
            db.put( null, id.toLong, name )
            println( "Inserted: " + (id -> name) )
      }
   }

   def readSumdn( db: Database ) {
      val rcsr = new RichCursor[ Long, String ]( db.openCursor( null, null ))
      try {
         rcsr.foreach( e => println( "Found: " + e ))
      } finally {
         rcsr.close()
      }
   }

   def deleteSumdn( db: Database ) {
      val rcsr = new RichCursor[ Long, String ]( db.openCursor( null, null ))
      try {
         rcsr.headOption.foreach {
            case e @ (key, value) =>
               db.delete( null, key )
               println( "Deleted: " + e )
         }
      } finally {
         rcsr.close()
      }
   }
}