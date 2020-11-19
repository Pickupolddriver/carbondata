package org.apache.spark.sql

import org.apache.model.TmpTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.test.TestQueryExecutor


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
    //Get CarbonTables form Spark Session
    //Create DataSet with spark session

    //    val srcCarbonTable = CarbonEnv.getCarbonTable(Some("default"), sourceTable.getTable)(sparkSession)
    //    val tgCarbonTable = CarbonEnv.getCarbonTable(Some("default"), targetTable.getTable)(sparkSession)
    //    val srcDf = sparkSession.sql(s"""SELECT * FROM ${srcCarbonTable.getTableName}""")
    //    val tgDf = sparkSession.sql(s"""SELECT * FROM ${tgCarbonTable.getTableName}""")

    val sqlContext: SQLContext = TestQueryExecutor.INSTANCE.sqlContext
    val srcDf = sqlContext.read.format("carbondata").option("tableName", sourceTable.getTable).load()
    val tgDf = sqlContext.read.format("carbondata").option("tableName", targetTable.getTable).load()


    var matches = Seq.empty[MergeMatch]
    val mel: Int = mergeExpression.length
    for (x <- 0 until mel) {
      var currExpression: Expression = mergeExpression.apply(x)
      var currAction: MergeAction = mergeActions.apply(x)
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
    // mergeCondition is an EqualTo Filter
    //    val left: String = mergeCondition.children.apply(0).toString()
    //    val right: String = mergeCondition.children.apply(1).toString()
    val scMC = Column(mergeCondition)



    //todo: Build the mergeColumn Map from mergeCondition
    val mergeDataSetMatches: MergeDataSetMatches = MergeDataSetMatches(scMC, matches.toList)

    //Generate the MerDataSetMatches
    CarbonMergeDataSetCommand(srcDf, tgDf, mergeDataSetMatches).run(sparkSession)
    //after calling this runnable commnad
    // check the two Dataset
    srcDf.collect()
    tgDf.collect()
    Seq.empty
  }

  override protected def opName: String = "MERGE SQL COMMAND"


}
