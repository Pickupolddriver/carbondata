package org.apache.spark.sql

import CarbonSqlCodeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.mutation.merge.MergeMatch
import org.apache.spark.sql.sources.EqualTo

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder.master("local").appName("JavaCarbonSessionExample").config("spark.driver.host", "localhost").config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
    val sparkSession = builder.getOrCreate

    val sqlText = "MERGE INTO LOGS USING NEWDEDUPEDLOGS ON LOGS.UNIQUEID = NEWDEDUPEDLOGS.UNIQUEID WHEN NOT MATCHED THEN INSERT *;"
    val sparkParser = new SparkSqlParser(new SQLConf)
    val visitor = new SimpleSqlVisitor(sparkParser)
    val lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CarbonSqlBaseParser(tokenStream)
    val mergeInto = visitor.visitMergeInto(parser.mergeInto)
//    val sourceTable = CarbonEnv.getCarbonTable(Option(mergeInto.getSource.getDatabase), mergeInto.getSource.getTable)(sparkSession)
//    val targetTable = CarbonEnv.getCarbonTable(Option(mergeInto.getSource.getDatabase), mergeInto.getSource.getTable)(sparkSession)

//    sparkSession.sql(s"SELECT * FROM $sourceTable").collect()
//    sparkSession.sql(s"SELECT * FROM $targetTable").collect()
    mergeInto.getMergeCondition



    var matches = Seq.empty[MergeMatch]


    //  case class CarbonMergeDataSetCommand(
    //                                        targetDsOri: Dataset[Row],
    //                                        srcDS: Dataset[Row],
    //                                        var mergeMatches: MergeDataSetMatches)


  }
}
