package de.sciss.bdb

import java.io.File
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import collection.JavaConversions._

object BDBTest {
   def main( args: Array[ String ]) {
      val write = args.headOption == Some( "-w" )
      test( write )
   }

   def test( write: Boolean ) {
      val envCfg = new EnvironmentConfig()
         .setAllowCreate( write )
      val storeCfg = new StoreConfig()
         .setAllowCreate( write )
      try {
         val env = new Environment( new File( "db" ), envCfg )
         try {
            val store = new EntityStore( env, "EntityStore", storeCfg )
            try {
               if( write ) writeSumdn( store ) else {
                  readSumdn( store )
                  reportMisses( env )
                  readSumdn( store )
                  reportMisses( env )
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
      val idx = store.getPrimaryIndex( classOf[ java.lang.Long ], classOf[ MyEntity ])
      List( "Alpha", "Beta", "Gamma", "Delta" ).zipWithIndex.foreach {
         case (name, id) => idx.put( new MyEntity( id, name ))
      }
   }

   def readSumdn( store: EntityStore ) {
      val idx  = store.getPrimaryIndex( classOf[ java.lang.Long ], classOf[ MyEntity ])
//      (0L to 1L).foreach { id =>
//         val e =  idx.get( id )
//         println( "Found : " + e )
//      }
      val csr = idx.entities()
      try {
         csr.iterator.foreach( e => println( "Found: " + e ))
      } finally {
         csr.close()
      }
   }

   def test2 {
      val envCfg = new EnvironmentConfig()
         .setAllowCreate( true )
         .setTransactional( true )
      try {
         val env = new Environment( new File( "db" ), envCfg )
         try {
            val misses = env.getStats( null ).getNCacheMiss
            println( "Done. Misses = " + misses )
         } finally {
            env.cleanLog()
            env.close()
         }
      } catch {
         case e =>
            e.printStackTrace()
            System.exit( 1 )
      }
   }
}