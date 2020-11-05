package org.apache.spark.sql;

import CarbonSqlCodeGen.CarbonSqlBaseBaseVisitor;
import CarbonSqlCodeGen.CarbonSqlBaseParser;
import org.apache.model.MergeInto;
import org.apache.model.TmpTable;

import java.util.List;

public class SimpleSqlVisitor extends CarbonSqlBaseBaseVisitor {

    @Override
    public MergeInto visitMergeInto(CarbonSqlBaseParser.MergeIntoContext ctx) {
        return new MergeInto(visitMultipartIdentifier(ctx.target), visitMultipartIdentifier(ctx.source));
    }


    @Override
    public TmpTable visitMultipartIdentifier(CarbonSqlBaseParser.MultipartIdentifierContext ctx) {
        TmpTable table = new TmpTable();
        List<CarbonSqlBaseParser.ErrorCapturingIdentifierContext> parts = ctx.parts;
        if (parts.size() == 2) {
            table.setDatabase(parts.get(0).getText());
            table.setTable(parts.get(1).getText());
        }
        if (parts.size() == 1) {
            table.setTable(parts.get(0).getText());
        }
        return table;
    }

    @Override
    public String visitUnquotedIdentifier(CarbonSqlBaseParser.UnquotedIdentifierContext ctx) {
        String res = ctx.getChild(0).getText();
        System.out.println("ColName; " + res);
        return res;
    }

    @Override
    public String visitFromClause(CarbonSqlBaseParser.FromClauseContext ctx) {
        String tableName = visitRelation(ctx.relation(0));
        System.out.println("SQL table name: " + tableName);
        return tableName;
    }

    @Override
    public String visitRelation(CarbonSqlBaseParser.RelationContext ctx) {
        if (ctx.relationPrimary() instanceof CarbonSqlBaseParser.TableNameContext) {
            return (String) visitTableName((CarbonSqlBaseParser.TableNameContext) ctx.relationPrimary());
        }
        return "";
    }

    @Override
    public String visitComparisonOperator(CarbonSqlBaseParser.ComparisonOperatorContext ctx) {
        String res = ctx.getChild(0).getText();
        System.out.println("ComparisonOperator: " + res);
        return res;
    }

    @Override
    public String visitTableIdentifier(CarbonSqlBaseParser.TableIdentifierContext ctx) {
        return ctx.getChild(0).getText();
    }
}
