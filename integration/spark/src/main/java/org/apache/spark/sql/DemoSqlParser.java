package org.apache.spark.sql;

import CarbonSqlCodeGen.CarbonSqlBaseLexer;
import CarbonSqlCodeGen.CarbonSqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.model.MergeInto;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.apache.spark.sql.execution.command.mutation.merge.CarbonMergeDataSetCommand;
import org.apache.spark.sql.execution.command.mutation.merge.MergeDataSetMatches;
import org.apache.spark.sql.internal.SQLConf;
import scala.Option;
import scala.Some;

import java.util.Optional;

public class DemoSqlParser {

    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder()
                .master("local")
                .appName("JavaCarbonSessionExample")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions");

        SparkSession sparkSession = builder.getOrCreate();
        sparkSession.sql("SHOW TABLES");

        System.out.print("> BEGIN TO VISIT MERGE INTO COMMAND");
        String sqlText = "MERGE INTO TARGET\n" +
                "USING SOURCE\n" +
                "ON TARGET.UNIQUEID = SOURCE.UNIQUEID\n" +
                "WHEN NOT MATCHED AND (TARGET.COL2='insert') THEN INSERT (TARGET.COL1, TARGET.COL2) VALUES (SOURCE.COL1, SOURCE.COL2);";

        SparkSqlParser sparkParser = new SparkSqlParser(new SQLConf());
        SimpleSqlVisitor visitor = new SimpleSqlVisitor(sparkParser);
        CarbonSqlBaseLexer lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        CarbonSqlBaseParser parser = new CarbonSqlBaseParser(tokenStream);
        MergeInto mergeInto = visitor.visitMergeInto(parser.mergeInto());

        //After visit all the nodes in the tree, we have get all the Expression
        //From here we need to convert the
//
//
//        CarbonTable targetTable = CarbonEnv.getCarbonTable(targetDB,mergeInto.getTarget().getTable(), sparkSession);
//        CarbonTable sourceTable = CarbonEnv.getCarbonTable(mergeInto.getSource().getDatabase(), mergeInto.getSource().getTable(), sparkSession);
//
//        //How to create the dataset from CarbonTable??
//        Expression d1 = sparkParser.parseExpression("select * from $targetTable");
//        Expression d2 = sparkParser.parseExpression("select * from $sourceTable");
//
//
//        //Generate Join Expression and Matched Action
//        MergeDataSetMatches mds = new MergeDataSetMatches(mergeInto.getMergeCondition());
//        new CarbonMergeDataSetCommand(d1, d2, mergeInto.getMergeDataSetMatches()).processData(SparkSession.builder().getOrCreate());

    }
}

