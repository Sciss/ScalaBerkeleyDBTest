package de.sciss.bdb

import java.awt.EventQueue

/*

"The basic outline would be to make an association
between the STM transaction and a database transaction:

- use a TxnLocal[DBTxn] to bind a DB txn to an STM txn
- from TxnLocal's initialValue function: create the DB txn and
register an ExternalDecider that calls DB commit
- from TxnLocal's afterRollback function: roll back the DB txn

The idea is that the call to the DB commit function becomes the
decision point for the STM transaction.

If you delay the DB updates until the beforeCommit stage (either using
Txn.beforeCommit or with TxnLocal) then there will be fewer DB
rollbacks, which will probably be a good thing.

- Nathan"

 */
object STMTest extends Runnable {
   def main( args: Array[ String ]) {
      EventQueue.invokeLater( this )
   }

   def run {

   }
}