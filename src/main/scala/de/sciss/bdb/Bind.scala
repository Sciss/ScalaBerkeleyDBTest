package de.sciss.bdb

import com.sleepycat.je.{Cursor, OperationStatus, Transaction, LockMode, Database, DatabaseEntry}

/**
 * Scala is smarter than Java... Let's use compile time
 * checks for the binding, by importing the implicits in the object.
 * Plus: No nasty auto-boxing of primitives.
 */
object Bind {
   implicit def booleanToDatabaseEntry( b: Boolean ) = {
      val arr  = new Array[ Byte ]( 1 )
      arr( 0 ) = if( b ) 1.toByte else 0.toByte
      new DatabaseEntry( arr )
   }

   implicit def intToDatabaseEntry( i: Int ) = {
      val arr  = new Array[ Byte ]( 4 )
      arr( 0 ) = (i >> 24).toByte
      arr( 1 ) = (i >> 16).toByte
      arr( 2 ) = (i >> 8).toByte
      arr( 3 ) = i.toByte
      new DatabaseEntry( arr )
   }

   implicit def floatToDatabaseEntry( f: Float ) : DatabaseEntry = intToDatabaseEntry( java.lang.Float.floatToIntBits( f ))

   implicit def longToDatabaseEntry( n: Long ) = {
      val arr  = new Array[ Byte ]( 8 )
      arr( 0 ) = (n >> 56).toByte
      arr( 1 ) = (n >> 48).toByte
      arr( 3 ) = (n >> 40).toByte
      arr( 3 ) = (n >> 32).toByte
      arr( 4 ) = (n >> 24).toByte
      arr( 5 ) = (n >> 16).toByte
      arr( 6 ) = (n >> 8).toByte
      arr( 7 ) = n.toByte
      new DatabaseEntry( arr )
   }

   implicit def doubleToDatabaseEntry( d: Double ) : DatabaseEntry = longToDatabaseEntry( java.lang.Double.doubleToLongBits( d ))

//   private val csUTF    = Charset.forName( "UTF-8" )
//   private val encUTF   = csUTF.newEncoder()

   implicit def stringToDatabaseEntry( s: String ) = {
      new DatabaseEntry( s.getBytes( "UTF-8" ))
   }

//   def wrap( db: Database ) = new RichDatabase( db )

   implicit def databaseEntryToBoolean( e: DatabaseEntry ) : Boolean = {
      require( e.getSize() == 1, "Wrong entry data size (need 1, found " + e.getSize() + ")" )
      e.getData()( e.getOffset() ) == 1
   }

   implicit def databaseEntryToInt( e: DatabaseEntry ) : Int = {
      require( e.getSize() == 4, "Wrong entry data size (need 4, found " + e.getSize() + ")" )
      val arr = e.getData()
      val i = e.getOffset()
      ((arr( i     ).toInt & 0xFF) << 24) |
      ((arr( i + 1 ).toInt & 0xFF) << 16) |
      ((arr( i + 2 ).toInt & 0xFF) << 8) |
       (arr( i + 3 ).toInt & 0xFF)
   }

   implicit def databaseEntryToFloat( e: DatabaseEntry ) : Float = java.lang.Float.intBitsToFloat( databaseEntryToInt( e ))

   implicit def databaseEntryToLong( e: DatabaseEntry ) : Long = {
      require( e.getSize() == 8, "Wrong entry data size (need 8, found " + e.getSize() + ")" )
      val arr = e.getData()
      val i = e.getOffset()
      ((arr( i     ).toInt & 0xFF) << 56) |
      ((arr( i + 1 ).toInt & 0xFF) << 48) |
      ((arr( i + 2 ).toInt & 0xFF) << 40) |
      ((arr( i + 3 ).toInt & 0xFF) << 32) |
      ((arr( i + 4 ).toInt & 0xFF) << 24) |
      ((arr( i + 5 ).toInt & 0xFF) << 16) |
      ((arr( i + 6 ).toInt & 0xFF) << 8) |
       (arr( i + 7 ).toInt & 0xFF)
   }

   implicit def databaseEntryToDouble( e: DatabaseEntry ) : Double = java.lang.Double.longBitsToDouble( databaseEntryToLong( e ))

   implicit def databaseEntryToString( e: DatabaseEntry ) =
      new String( e.getData(), e.getOffset(), e.getSize(), "UTF-8" )

   implicit def cursorWrapper( csr: Cursor ) = new RichCursor( csr )
}

class RichDatabase( val db: Database ) {
   import Bind._

   def getBoolean[ @specialized K <% DatabaseEntry ]( txn: Transaction, key: K, lockMode: LockMode = LockMode.DEFAULT ) : Boolean = {
      val entry = new DatabaseEntry()
      if( db.get( txn, key, entry, lockMode ) == OperationStatus.SUCCESS ) {
         databaseEntryToBoolean( entry )
      } else error( "Key not found " + key )
   }

   def getString[ @specialized K <% DatabaseEntry ]( txn: Transaction, key: K, lockMode: LockMode = LockMode.DEFAULT ) : String = {
      val entry = new DatabaseEntry()
      if( db.get( txn, key, entry, lockMode ) == OperationStatus.SUCCESS ) {
         databaseEntryToString( entry )
      } else error( "Key not found " + key )
   }
}

object RichCursor {
//   trait Entry {
//      def getBooleanKey : Boolean
//      def getStringKey : Boolean
//      def getBooleanValue : Boolean
//      def getStringValue : Boolean
//   }
//
//   private class EntryImpl( key: DatabaseEntry, value: DatabaseEntry ) extends Entry {
//      import Bind._
//
//      def getBooleanKey : Boolean   = databaseEntryToBoolean( key )
//      def getStringKey : String     = databaseEntryToString( key )
//      def getBooleanValue : Boolean = databaseEntryToBoolean( value )
//      def getStringValue : String   = databaseEntryToString( value )
//   }

//   implicit val databaseEntryToBoolean : DatabaseEntry => Boolean = Bind.databaseEntryToBoolean( _ )
//   implicit val databaseEntryToString : DatabaseEntry => String = Bind.databaseEntryToString( _ )
//   implicit val databaseEntryToInt : DatabaseEntry => Int = Bind.databaseEntryToInt( _ )
//   implicit val databaseEntryToLong : DatabaseEntry => Long = Bind.databaseEntryToLong( _ )
//   implicit val databaseEntryToFloat : DatabaseEntry => Float = Bind.databaseEntryToFloat( _ )
//   implicit val databaseEntryToDouble : DatabaseEntry => Double = Bind.databaseEntryToDouble( _ )

   implicit def sink( rcsr: RichCursor[ _, _ ]) = rcsr.csr
}
class RichCursor[ K, V ]( val csr: Cursor )( implicit kView: DatabaseEntry => K, vView: DatabaseEntry => V ) extends Iterable[ (K, V) ] {
   import RichCursor._

   def iterator = new Iterator[ (K, V) ] {
      var key     = new DatabaseEntry()
      var value   = new DatabaseEntry()
      var status  = csr.getFirst( key, value, LockMode.DEFAULT )
//      var init    = true

      def hasNext : Boolean = status == OperationStatus.SUCCESS

      def next: (K, V) = {
         require( hasNext )
         val k = kView( key )
         val v = vView( value )
//         if( init ) {
//            init = false
//         } else {
            key     = new DatabaseEntry()
            value   = new DatabaseEntry()
            status  = csr.getNext( key, value, LockMode.DEFAULT )
//         }
         (k, v)
      }
   }
}