package de.sciss.bdb

import java.io.File
import com.sleepycat.je.{Database, EnvironmentConfig, DatabaseConfig, Environment}

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
            val db = env.openDatabase( null, "Database", dbCfg )
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
      error( "TODO" )
   }

   def readSumdn( db: Database ) {
      error( "TODO" )
   }

   def deleteSumdn( db: Database ) {
      error( "TODO" )
   }
}