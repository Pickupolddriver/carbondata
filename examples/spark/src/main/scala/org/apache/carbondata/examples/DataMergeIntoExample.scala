package org.apache.carbondata.examples

import java.io.File

import CarbonSqlCodeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.{MergeIntoSQLCommand, SQLConf, SimpleSqlVisitor, SparkSession}
import org.apache.spark.util.SparkUtil.{convertExpressionList, convertMergeActionList}

object DataMergeIntoExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("DataManagementExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS TARGET_TABLE")
    spark.sql("DROP TABLE IF EXISTS SOURCE_TABLE")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS TARGET_TABLE(
         |   ID Int,
         |   PRICE Int,
         |   STATE String
         | )
         | STORED AS carbondata
       """.stripMargin)

    spark.sql(s"""INSERT INTO TARGET_TABLE VALUES (1,10,"MA")""")
    spark.sql(s"""INSERT INTO TARGET_TABLE VALUES (2,20,"NY")""")
    spark.sql(s"""INSERT INTO TARGET_TABLE VALUES (3,30,"NH")""")

    spark.sql(s"""SELECT count(*) FROM TARGET_TABLE""").show()
    spark.sql(s"""SELECT * FROM TARGET_TABLE""").show()

    // Create table2
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS SOURCE_TABLE(
         |   ID Int,
         |   PRICE Int,
         |   STATE String
         | )
         | STORED AS carbondata
       """.stripMargin)

    spark.sql(s"""INSERT INTO SOURCE_TABLE VALUES (1,1,"MA")""")
    spark.sql(s"""INSERT INTO SOURCE_TABLE VALUES (2,3,"NY")""")
    spark.sql(s"""INSERT INTO SOURCE_TABLE VALUES (3,3,"NH")""")

    spark.sql(s"""SELECT count(*) FROM SOURCE_TABLE""").show()
    spark.sql(s"""SELECT * FROM SOURCE_TABLE""").show()

    val sqlText = "MERGE INTO TARGET_TABLE USING SOURCE_TABLE ON TARGET_TABLE.ID=SOURCE_TABLE.ID WHEN MATCHED THEN DELETE"
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
    ).processData(spark)

    spark.sql(s"""SELECT * FROM TARGET_TABLE""").show()

    spark.sql("DROP TABLE IF EXISTS TARGET_TABLE")
    spark.sql("DROP TABLE IF EXISTS SOURCE_TABLE")
  }
}
