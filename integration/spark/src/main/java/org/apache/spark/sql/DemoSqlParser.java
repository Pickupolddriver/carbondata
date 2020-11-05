package org.apache.spark.sql;

import CarbonSqlCodeGen.CarbonSqlBaseLexer;
import CarbonSqlCodeGen.CarbonSqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.model.MergeInto;


public class DemoSqlParser {

    public static void main(String[] args) {
        System.out.print(">");
        String sqlText = "MERGE INTO LOGS\n" +
                "USING NEWDEDUPEDLOGS\n" +
                "ON LOGS.UNIQUEID = NEWDEDUPEDLOGS.UNIQUEID\n" +
                "WHEN NOT MATCHED\n" +
                "  THEN INSERT *";

        SimpleSqlVisitor visitor = new SimpleSqlVisitor();
        CarbonSqlBaseLexer lexer = new CarbonSqlBaseLexer(CharStreams.fromString(sqlText));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        CarbonSqlBaseParser parser = new CarbonSqlBaseParser(tokenStream);
        MergeInto mergeInto = (MergeInto) visitor.visitMergeInto(parser.mergeInto());
        mergeInto.getTarget();
    }
}

