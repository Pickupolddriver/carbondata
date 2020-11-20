package org.apache.spark.sql

import org.apache.model.TmpTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.functions.col

case class MergeIntoSQLCommand(sourceTable: TmpTable,
                               targetTable: TmpTable,
                               mergeCondition: Expression,
                               mergeExpression: Seq[Expression],
                               mergeActions: Seq[MergeAction])
  extends AtomicRunnableCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {

    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {

    val srcDf = sparkSession.sql(s"""SELECT * FROM ${sourceTable.getTable}""")
    val tgDf = sparkSession.sql(s"""SELECT * FROM ${targetTable.getTable}""")

    var matches = Seq.empty[MergeMatch]
    val mel: Int = mergeExpression.length
    for (x <- 0 until mel) {
      val currExpression: Expression = mergeExpression.apply(x)
      val currAction: MergeAction = mergeActions.apply(x)
      //Convert the current Actions to Map

      if (currAction.isInstanceOf[UpdateAction]) {
        if (currAction.asInstanceOf[UpdateAction].isStar) {
          val srcCols = srcDf.columns
          val tgCols = tgDf.columns
          currAction.asInstanceOf[UpdateAction].updateMap = Map[Column, Column]()
          for (i <- 0 until srcCols.length) {
            currAction.asInstanceOf[UpdateAction].updateMap.+=(col(tgCols.apply(i)) -> col(sourceTable.getTable + "." + srcCols.apply(i)))
          }
        } else {
          // if not star, it may contains fewer cols and may be expression rather than col
        }
      } else if (currAction.isInstanceOf[InsertAction]) {
        if (currAction.asInstanceOf[InsertAction].isStar) {
          val srcCols = srcDf.columns
          val tgCols = tgDf.columns
          for (i <- 0 until srcCols.length) {
            currAction.asInstanceOf[UpdateAction].updateMap.+= (col(tgCols.apply(i)) -> col(sourceTable.getTable + "." + srcCols.apply(i)))
          }
        } else {
          // if not star, it may contains fewer cols and may be expression rather than col
        }
      }

      if (currExpression == null) {
        // According to the MergeAction to reGenerate the
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          matches ++= Seq(WhenMatched().addAction(currAction))
        } else {
          matches ++= Seq(WhenNotMatched().addAction(currAction))
        }
      } else {
        //todo: Build the map of insert/update from currExpression
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          WhenMatched().addAction(currAction)
        } else {
          WhenNotMatched().addAction(currAction)
        }
        matches ++= Seq()
      }
    }

    val scMC = Column(mergeCondition)


    //todo: Build the mergeColumn Map from mergeCondition
    val mergeDataSetMatches: MergeDataSetMatches = MergeDataSetMatches(scMC, matches.toList)

    CarbonMergeDataSetCommand(tgDf, srcDf, mergeDataSetMatches).run(sparkSession)
    Seq.empty
  }

  override protected def opName: String = "MERGE SQL COMMAND"


}
