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
    // Length of MergeExpression
    val mel: Int = mergeExpression.length
    for (x <- 0 until mel) {
      val currExpression: Expression = mergeExpression.apply(x)
      val currAction: MergeAction = mergeActions.apply(x)
      // Use Pattern Matching
      // Convert the current Actions to Map
      // Since the delete action will delete the whole line, we don't need to build map here
      currAction match {
        case action: UpdateAction =>
          if (action.isStar) {
            val srcCols = srcDf.columns
            val tgCols = tgDf.columns
            action.updateMap = Map[Column, Column]()
            for (i <- srcCols.indices) {
              action.updateMap.+=(col(tgCols.apply(i)) -> col(sourceTable.getTable + "." + srcCols.apply(i)))
            }
          } else {
            // if not star, it may contains fewer cols and may be expression rather than col
          }
        case action: InsertAction =>
          if (action.isStar) {
            val srcCols = srcDf.columns
            val tgCols = tgDf.columns
            action.insertMap =Map[Column,Column]()
            for (i <- srcCols.indices) {
              action.insertMap.+=(col(tgCols.apply(i)) -> col(sourceTable.getTable + "." + srcCols.apply(i)))
            }
          } else {
            // if not star, it may contains fewer cols and may be expression rather than col
          }
        case _ =>
      }

      // todo: Extract Method
      if (currExpression == null) {
        // According to the MergeAction to reGenerate the
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          matches ++= Seq(WhenMatched().addAction(currAction))
        } else {
          matches ++= Seq(WhenNotMatched().addAction(currAction))
        }
      } else {
        // Since the mergeExpression is not null, we need to Initialize the WhenMatched/WhenNotMatched with the Expression
        // Check the example to see how they initialize the matches
        val carbonMergeExpression:Option[Column]=Option(Column(currExpression))
        if (currAction.isInstanceOf[DeleteAction] || currAction.isInstanceOf[UpdateAction]) {
          matches ++= Seq(WhenMatched(carbonMergeExpression).addAction(currAction))
        } else {
          matches ++= Seq(WhenNotMatched(carbonMergeExpression).addAction(currAction))
        }
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
