package de.sciss.bdb

import java.io.File
import com.sleepycat.je.{Database, Transaction, Environment, EnvironmentConfig, DatabaseConfig}
import Bind._

object TxBaseTest {
   def main( args: Array[ String ]) {
      test
   }

   def test {
      val envCfg = new EnvironmentConfig()
         .setAllowCreate( true )
         .setTransactional( true )
      val dbCfg = new DatabaseConfig()
         .setAllowCreate( true )
         .setTransactional( true )
      try {
         val dir  = new File( "db_base" )
         dir.mkdirs()
         val env  = new Environment( dir, envCfg )
         try {
            val db   = env.openDatabase( null, "Database", dbCfg ) // auto-commit protected
            try {
               implicit val txn = env.beginTransaction( null, null )
               try {
                  writeSumdn( db )
                  readSumdn( db )
                  deleteSumdn( db )
                  readSumdn( db )
                  txn.commit()
               } catch {
                  case e =>
                     txn.abort()
                     throw e
               }
            } finally {
               db.close()
            }
         } finally {
            env.close()
         }
      } catch {
         case e =>
            e.printStackTrace()
            System.exit( 1 )
      }
   }

   def writeSumdn( db: Database )( implicit txn: Transaction ) {
      List( "Alpha", "Beta", "Gamma", "Delta" ).zipWithIndex.foreach {
         case (name, id) =>
            db.put( txn, id.toLong, name )
            println( "Inserted: " + (id -> name) )
      }
   }

   def readSumdn( db: Database )( implicit txn: Transaction ) {
      val rcsr = new RichCursor[ Long, String ]( db.openCursor( txn, null ))
      try {
         rcsr.foreach( e => println( "Found: " + e ))
      } finally {
         rcsr.close()
      }
   }

   def deleteSumdn( db: Database )( implicit txn: Transaction ) {
      val rcsr = new RichCursor[ Long, String ]( db.openCursor( txn, null ))
      try {
         rcsr.headOption.foreach {
            case e @ (key, value) =>
               db.delete( txn, key )
               println( "Deleted: " + e )
         }
      } finally {
         rcsr.close()
      }
   }
}