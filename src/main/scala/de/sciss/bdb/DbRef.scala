/*
 *  DbRef.scala
 *  (ScalaBerkeleyDBTest)
 *
 *  Copyright (c) 2011 Hanns Holger Rutz. All rights reserved.
 *
 *  This software is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  as published by the Free Software Foundation; either
 *  version 2, june 1991 of the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public
 *  License (gpl.txt) along with this software; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 *
 *  For further information, please contact Hanns Holger Rutz at
 *  contact@sciss.de
 */

package de.sciss.bdb

import reflect.OptManifest
import concurrent.stm.Ref.View
import concurrent.stm.{InTxnEnd, Txn, TxnLocal, InTxn, Ref}
import concurrent.stm.Txn.ExternalDecider
import collection.immutable.{Set => ISet}
import com.sleepycat.je.{LockMode, OperationStatus, DatabaseEntry, Environment, Transaction, Database}

// trait DbRef[ A ] extends Ref[ A ]

class DbRefFactory( env: Environment ) {
   def newRef[ K, V ]( db: Database, key: K, initialValue: V )
                     ( implicit om: OptManifest[ V ], txn: Transaction, kView: K => DatabaseEntry,
                       vView: V => DatabaseEntry, vDec: DatabaseEntry => V ) : Ref[ V ] = {
      val e = new DatabaseEntry()
      val v = if( db.get( txn, key, e, LockMode.DEFAULT ) == OperationStatus.SUCCESS ) vDec( e ) else initialValue
      new Impl( db, key, Ref( v ))( kView, vView )
   }

   private def errNotYet = error( "Not yet implemented" )

//   private val dbTxn          = TxnLocal( initialValue = initDBTxn )
   private val dbTxnWriteSet  = TxnLocal( initialValue = initWriteSet( _ ))

//   private def initDBTxn( implicit txn: InTxn ) : Transaction = {
//      val res = env.beginTransaction( null, null )
//      Txn.setExternalDecider( new ExternalDecider {
//          def shouldCommit( implicit txn: InTxnEnd ) : Boolean = {
//
//          }
//      })
//      res
//   }

   private def initWriteSet( implicit txn: InTxn ) : ISet[ Write[ _ ]] = {
      Txn.setExternalDecider( new ExternalDecider {
          def shouldCommit( implicit txn: InTxnEnd ) : Boolean = {
             val set = dbTxnWriteSet.get
             try {
                implicit val t = env.beginTransaction( null, null )
                try {
                   set.foreach( _.persist )
                   t.commit()
                   true
                } catch {
                   case e =>
                     t.abort()
                     false
                }
             } catch {
                case e => false
             }
          }
      })
      ISet.empty[ Write[ _ ]]
   }

   private case class Write[ V ]( impl: Impl[ _, V ])( value: V ) {
      def persist( implicit txn: Transaction ) { impl.persist( value )}
   }

   private class Impl[ @specialized K, @specialized V ]( db: Database, key: K, r: Ref[ V ])( implicit kView: K => DatabaseEntry, vView: V => DatabaseEntry )
   extends Ref[ V ] {

      me =>

//      val touched = TxnLocal( false )

      def persist( value: V )( implicit txn: Transaction ) {
         db.put( txn, key, value )
      }

      def get( implicit txn: InTxn ) : V = {
         r.get( txn )
      }

      def touch( v: V )( implicit txn: InTxn ) {
//         if( !touched.swap( true )) {
//            Txn.beforeCommit
//         }
//         dbTxnWriteSet.transformIfDefined {
//            case set if( !set.contains( me )) => set + me
//         }
         dbTxnWriteSet.transform( _ + Write( me )( v ))
      }

      def set( v: V )( implicit txn: InTxn ) {
         val old = r.get
         if( old != v ) {
            r.set( v )
            touch( v )
         }
      }

      def swap( v: V )( implicit txn: InTxn ) : V = {
         val old = r.swap( v )( txn )
         if( old != v ) touch( v )
         old
      }

      def transform( f: V => V )( implicit txn: InTxn ) {
//         r.transform( f )( txn )
         val old  = r.get
         val v    = f( old )
         if( old != v ) {
            r.set( v )
            touch( v )
         }
      }

      def single : View[ V ] = errNotYet
      def trySet( v: V )( implicit txn: InTxn ) : Boolean = errNotYet
      def transformIfDefined( pf: PartialFunction[ V, V ])( implicit txn: InTxn ) : Boolean = errNotYet
      def relaxedGet( equiv: (V, V) => Boolean )( implicit txn: InTxn ) : V = errNotYet
      def getWith[ Z ]( f: V => Z )( implicit txn: InTxn ) : Z = errNotYet
   }

//   def apply[ K, V ]( db: Database, key: K, initialValue: V )( implicit om: OptManifest[ V ]) : DbRef[ V ] =
//      new Impl( db, key, initialValue, Ref( new WeakReference( initialValue )))
//
//   private def errNotYet = error( "Not yet implemented" )
//
//   private class Impl[ @specialized K, @specialized V ]( db: Database, key: K, ival: V, r: Ref[ WeakReference[ V ]]) extends DbRef[ V ] {
//      def get( implicit txn: InTxn ) : V = r.get( txn )
//      def set( v: V )( implicit txn: InTxn ) : Unit = r.set( v )( txn )
//      def swap( v: V )( implicit txn: InTxn ) : V = r.swap( v )( txn )
//      def transform( f: V => V )( implicit txn: InTxn ) : Unit = r.transform( f )( txn )
//
//      def single : View[ V ] = errNotYet
//      def trySet( v: V )( implicit txn: InTxn ) : Boolean = errNotYet
//      def transformIfDefined( pf: PartialFunction[ V, V ])( implicit txn: InTxn ) : Boolean = errNotYet
//      def relaxedGet( equiv: (V, V) => Boolean )( implicit txn: InTxn ) : V = errNotYet
//      def getWith[ Z ]( f: V => Z )( implicit txn: InTxn ) : Z = errNotYet
//   }
}