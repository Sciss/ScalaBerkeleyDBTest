/*
 *  STMTest.scala
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

import java.awt.image.BufferedImage
import java.io.File
import collection.immutable.{IndexedSeq => IIdxSeq}
import com.sleepycat.je.{Transaction, Database, Environment, EnvironmentConfig, DatabaseConfig}
import java.awt.{BorderLayout, Graphics2D, Dimension, Graphics, EventQueue}
import java.awt.event.{ActionEvent, MouseEvent, MouseAdapter, WindowEvent, WindowAdapter}
import java.util.TimerTask
import concurrent.stm.{TxnUnknown, MaybeTxn, atomic, Txn, Ref}
import javax.swing.{BorderFactory, SwingConstants, JLabel, AbstractButton, AbstractAction, JToggleButton, WindowConstants, JFrame, JComponent}

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
      val envCfg = new EnvironmentConfig()
         .setAllowCreate( true )
         .setTransactional( true )
      val dbCfg = new DatabaseConfig()
         .setAllowCreate( true )
         .setTransactional( true )
      try {
         val dir  = new File( "db_stm" )
         dir.mkdirs()
         val env  = new Environment( dir, envCfg )
         try {
            implicit val txn  = env.beginTransaction( null, null )
            try {
               val db   = env.openDatabase( txn, "GOL", dbCfg ) // auto-commit protected
               initGUI( db )
            } finally {
               txn.commit()
            }
         } catch { case e =>
            env.close()
            throw e
         }
      } catch {
         case e =>
            e.printStackTrace()
            System.exit( 1 )
      }
   }

   private def initGUI( db: Database )( implicit txn: Transaction ) {
      import Bind._

      val fact    = new DbRefFactory( db.getEnvironment() )
      val rows    = 32
      val cols    = 32
      val period  = 500L
      val msize   = rows * cols
      val matrix  = IIdxSeq.tabulate( msize )( i => fact.newRef( db, i, false ))

      val f       = new JFrame( "Game of Life -- persistent STM" )
      val cp      = f.getContentPane()
      val gol     = new GOL( matrix, rows, cols )
      cp.add( gol, BorderLayout.CENTER )
      val butRun  = new JToggleButton( new AbstractAction( "Run" ) {
         val timer   = new java.util.Timer( "GOL", true )
         var tt      = Option.empty[ TimerTask ]
         def actionPerformed( e: ActionEvent ) {
            e.getSource match {
               case b: AbstractButton =>
                  tt.foreach { task =>
                     task.cancel()
                     timer.purge()
                     tt = None
                  }
                  if( b.isSelected ) {
                     val task = new TimerTask {
                        val offs = List( -cols - 1, -cols, -cols + 1, -1, 1, cols - 1, cols, cols + 1 )
                        def run {
                           atomic { implicit txn =>
                              val analysis = matrix.zipWithIndex.map {
                                 case (cell, i) =>
                                    val alive   = cell.get
                                    val neigh   = offs.count( off => matrix( (i + off + msize) % msize ).get )
                                    (alive, neigh)
                              }
                              analysis.zipWithIndex.foreach {
                                 case ((alive, neigh), i) =>
                                    matrix( i ).set( if( alive ) neigh >= 2 && neigh <= 3 else neigh == 3 )
                              }
                              gol.updateAll
                           }
                        }
                     }
                     tt = Some( task )
                     timer.schedule( task, 0L, period )
                  }
            }
         }
      })
      butRun.setFocusable( false )
      butRun.putClientProperty( "JButton.buttonType", "square" )
      val lbInfo = new JLabel( "Click to toggle cell. Alt-Click to test txn abortion", SwingConstants.CENTER )
      lbInfo.setBorder( BorderFactory.createCompoundBorder( BorderFactory.createEmptyBorder( 2, 4, 2, 4 ), lbInfo.getBorder ))
      cp.add( lbInfo, BorderLayout.NORTH )
      cp.add( butRun, BorderLayout.SOUTH )

      f.addWindowListener( new WindowAdapter {
         override def windowClosed( w: WindowEvent ) {
            var success = false
            try {
               db.close()
            } finally {
               try {
                  db.getEnvironment().close()
                  success = true
               } finally {
                  System.exit( if( success ) 0 else 1 )
               }
            }
         }
      })
      f.pack()
      f.setLocationRelativeTo( null )
      f.setDefaultCloseOperation( WindowConstants.DISPOSE_ON_CLOSE )
      f.setVisible( true )
   }

   class GOL( matrix: IIdxSeq[ Ref[ Boolean ]], rows: Int, cols: Int ) extends JComponent {
      require( matrix.size == rows * cols )

      override def getPreferredSize = new Dimension( cols * 10, rows * 10 )
      private val img               = new BufferedImage( cols, rows, BufferedImage.TYPE_INT_ARGB )

      updateAll( TxnUnknown )

      addMouseListener( new MouseAdapter {
         override def mousePressed( e: MouseEvent ) {
            val col  = (e.getX().toFloat / getWidth() * cols).toInt
//            val row  = ((1.0f - (e.getY().toFloat / getHeight())) * rows + 0.5f).toInt
            val row  = (e.getY().toFloat / getHeight() * rows).toInt
            if( col >= 0 && col < cols && row >= 0 && row < rows ) {
               toggle( row, col, e.isAltDown )
            }
         }
      })

      def updateAll( implicit txn: MaybeTxn ) {
         val vs = atomic { implicit txn => matrix.map( _.get )}
         img.synchronized {
            vs.zipWithIndex.foreach {
               case (state, i) =>
                  val row  = i / cols
                  val col  = i % cols
                  val rgb  = if( state ) 0xFFFFFFFF else 0xFF000000
                  img.setRGB( col, row, rgb )
            }
         }
         repaint()
      }

      def toggle( row: Int, col: Int, fail: Boolean ) {
         require( col >= 0 && col < cols && row >= 0 && row < rows )
         atomic { implicit txn =>
            val i = row * cols + col
            val state = !matrix( i ).get
            matrix( i ).set( state )
            if( fail ) error( "Deliberate crash" )
            Txn.afterCommit { _ =>
               img.synchronized {
                  val rgb  = if( state ) 0xFFFFFFFF else 0xFF000000
                  img.setRGB( col, row, rgb )
                  val rx = (col.toFloat / cols * getWidth()).toInt - 1
                  val ry = (row.toFloat / rows * getHeight()).toInt - 1
                  val rw = ((col + 1).toFloat / cols * getWidth()).toInt + 1 - rx
                  val rh = ((row + 1).toFloat / rows * getHeight()).toInt + 1 - ry
                  repaint( rx, ry, rw, rh )
               }
            }
         }
      }

      override def paintComponent( g: Graphics ) {
         super.paintComponent( g )
//         val g2 = g.asInstanceOf[ Graphics2D ]
//         g2.setRenderingHint()
         img.synchronized( g.drawImage( img, 0, 0, getWidth(), getHeight(), this ))
      }
   }
}