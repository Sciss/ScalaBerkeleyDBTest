package de.sciss.bdb

import reflect.OptManifest
import concurrent.stm.Ref.View
import concurrent.stm.{InTxn, Ref}

trait DbRef[ A ] extends Ref[ A ]

object DbRef {
   def apply[ A ]( initialValue: A )( implicit om: OptManifest[ A ]) : DbRef[ A ] = new Impl( Ref( initialValue ))

   private def errNotYet = error( "Not yet implemented" )

   private class Impl[ @specialized A ]( r: Ref[ A ]) extends DbRef[ A ] {
      def get( implicit txn: InTxn ) : A = r.get( txn )
      def set( v: A )( implicit txn: InTxn ) : Unit = r.set( v )( txn )
      def swap( v: A )( implicit txn: InTxn ) : A = r.swap( v )( txn )
      def transform( f: A => A )( implicit txn: InTxn ) : Unit = r.transform( f )( txn )

      def single : View[ A  ] = errNotYet
      def trySet( v: A )( implicit txn: InTxn ) : Boolean = errNotYet
      def transformIfDefined( pf: PartialFunction[ A, A ])( implicit txn: InTxn ) : Boolean = errNotYet
      def relaxedGet( equiv: (A, A) => Boolean )( implicit txn: InTxn ) : A = errNotYet
      def getWith[ Z ]( f: A => Z )( implicit txn: InTxn ) : Z = errNotYet
   }
}