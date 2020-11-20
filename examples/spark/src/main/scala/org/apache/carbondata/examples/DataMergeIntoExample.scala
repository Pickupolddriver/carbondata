package org.apache.carbondata.examples

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
    spark.sql("DROP TABLE IF EXISTS A")
    spark.sql("DROP TABLE IF EXISTS B")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS A(
         |   id Int,
         |   price Int,
         |   state String
         | )
         | STORED AS carbondata
       """.stripMargin)

    spark.sql(s"""INSERT INTO A VALUES (1,10,"MA")""")
    spark.sql(s"""INSERT INTO A VALUES (2,20,"NY")""")
    spark.sql(s"""INSERT INTO A VALUES (3,30,"NH")""")

    spark.sql(s"""SELECT count(*) FROM A""").show()
    spark.sql(s"""SELECT * FROM A""").show()

    // Create table2
    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS B(
         |   id Int,
         |   price Int,
         |   state String
         | )
         | STORED AS carbondata
       """.stripMargin)

    spark.sql(s"""INSERT INTO B VALUES (1,1,"MA")""")
    spark.sql(s"""INSERT INTO B VALUES (2,3,"NY")""")
    spark.sql(s"""INSERT INTO B VALUES (3,3,"NH")""")

    spark.sql(s"""SELECT count(*) FROM B""").show()
    spark.sql(s"""SELECT * FROM B""").show()

    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED THEN DELETE"
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

    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()

    spark.sql("DROP TABLE IF EXISTS A")
    spark.sql("DROP TABLE IF EXISTS B")
  }
}
