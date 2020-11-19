package org.apache.spark.sql

import CarbonSqlCodeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.util.SparkUtil.{convertExpressionList, convertMergeActionList}


object ScalaDemo {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder.master("local").appName("JavaCarbonSessionExample").config("spark.driver.host", "localhost").config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
    val sparkSession = builder.getOrCreate

    val sqlText = "MERGE INTO TARGET USING SOURCE ON TARGET.UNIQUEID = SOURCE.UNIQUEID WHEN MATCHED THEN DELETE"
    val sparkParser = new SparkSqlParser(new SQLConf)
    val visitor = new SimpleSqlVisitor(sparkParser)
    val lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText))
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new CarbonSqlBaseParser(tokenStream)
    val mergeInto = visitor.visitMergeInto(parser.mergeInto)



    MergeIntoSQLCommand.apply(mergeInto.getSource,
      mergeInto.getTarget,
      mergeInto.getMergeCondition,
      convertExpressionList(mergeInto.getMergeExpressions),
      convertMergeActionList(mergeInto.getMergeActions)
    ).processData(sparkSession)
  }


}
