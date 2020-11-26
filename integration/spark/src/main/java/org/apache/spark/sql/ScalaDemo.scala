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

package org.apache.spark.sql

import CarbonSqlCodeGen.{CarbonSqlBaseLexer, CarbonSqlBaseParser}
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.util.SparkUtil.{convertExpressionList, convertMergeActionList}

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder
      .master("local")
      .appName("JavaCarbonSessionExample")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
    val sparkSession = builder.getOrCreate

    val sqlText = "MERGE INTO TARGET USING SOURCE ON TARGET.UNIQUEID = SOURCE.UNIQUEID WHEN " +
                  "MATCHED THEN DELETE"
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
    ).processData(sparkSession)
  }
}
