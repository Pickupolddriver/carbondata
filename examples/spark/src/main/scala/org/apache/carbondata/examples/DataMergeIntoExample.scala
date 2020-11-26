/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples

import CarbonSqlCodeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.{MergeIntoSQLCommand, AntlrSqlVisitor, SparkSession, SQLConf}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.util.SparkUtil.{convertExpressionList, convertMergeActionList}

import org.apache.carbondata.examples.util.ExampleUtils

object DataMergeIntoExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("DataManagementExample")
    deleteExampleBody(spark)
    deleteWithExpressionExample(spark)
    updateExampleBody(spark)
    updateWithExpressionExample(spark)
    updateSpecificColWithExpressionExample(spark)
    insertExampleBody(spark)
    insertWithExpressionExample(spark)
//    insertSpecificColWithExpressionExample(spark)
    spark.close()
  }

  def initTable(spark: SparkSession): Unit = {
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

    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS B(
         |   id Int,
         |   price Int,
         |   state String
         | )
         | STORED AS carbondata
       """.stripMargin)

    spark.sql(s"""INSERT INTO A VALUES (1,100,"MA")""")
    spark.sql(s"""INSERT INTO A VALUES (2,200,"NY")""")
    spark.sql(s"""INSERT INTO A VALUES (3,300,"NH")""")
    spark.sql(s"""INSERT INTO A VALUES (4,400,"FL")""")

    spark.sql(s"""SELECT count(*) FROM A""").show()
    spark.sql(s"""SELECT * FROM A""").show()

    spark.sql(s"""INSERT INTO B VALUES (1,1,"MA (updated)")""")
    spark.sql(s"""INSERT INTO B VALUES (2,3,"NY (updated)")""")
    spark.sql(s"""INSERT INTO B VALUES (3,3,"CA (updated)")""")
    spark.sql(s"""INSERT INTO B VALUES (5,5,"TX (updated)")""")
    spark.sql(s"""INSERT INTO B VALUES (7,7,"LO (updated)")""")

    spark.sql(s"""SELECT count(*) FROM B""").show()
    spark.sql(s"""SELECT * FROM B""").show()
  }

  def dropTables(spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS A")
    spark.sql("DROP TABLE IF EXISTS B")
  }

  def executeMergeSqlText(spark: SparkSession, sqlText: String): Unit = {
    val sparkParser = new SparkSqlParser(new SQLConf)
    val visitor = new AntlrSqlVisitor(sparkParser)
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
  }

  def deleteExampleBody(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED THEN DELETE"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def deleteWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED AND B.ID=2 THEN DELETE"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def updateExampleBody(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED THEN UPDATE SET *"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def updateWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED AND A.ID=2 THEN UPDATE SET *"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def updateSpecificColWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    // In this example, it will only update the state
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED AND A.ID=2 THEN UPDATE SET " +
                  "STATE=B.STATE"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def updateSpecificMultiColWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN MATCHED AND A.ID=2 THEN UPDATE SET A" +
                  ".STATE=B.STATE, A.PRICE=B.PRICE"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def insertExampleBody(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED THEN INSERT *"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def insertWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED AND B.ID=7 THEN INSERT *"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }

  def insertSpecificColWithExpressionExample(spark: SparkSession): Unit = {
    dropTables(spark)
    initTable(spark)
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED AND B.ID=7 THEN INSERT (A" +
                  ".ID,A.PRICE) VALUES (B.ID,B.PRICE)"
    executeMergeSqlText(spark, sqlText)
    println("Show table A")
    spark.sql(s"""SELECT * FROM A""").show()
    dropTables(spark)
  }
}
