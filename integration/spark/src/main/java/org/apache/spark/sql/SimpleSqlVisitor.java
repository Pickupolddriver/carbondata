package org.apache.spark.sql;

import CarbonSqlCodeGen.CarbonSqlBaseBaseVisitor;
import CarbonSqlCodeGen.CarbonSqlBaseParser;
import org.apache.model.CarbonJoinExpression;
import org.apache.model.MergeInto;
import org.apache.model.TmpColumn;
import org.apache.model.TmpTable;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.execution.command.mutation.merge.DeleteAction;
import org.apache.spark.sql.execution.command.mutation.merge.InsertAction;
import org.apache.spark.sql.execution.command.mutation.merge.MergeAction;

import java.util.ArrayList;
import java.util.List;

public class SimpleSqlVisitor extends CarbonSqlBaseBaseVisitor {

    private ParserInterface sparkParser;

    public SimpleSqlVisitor(ParserInterface sparkParser) {
        this.sparkParser = sparkParser;
    }

    @Override
    public String visitTableAlias(CarbonSqlBaseParser.TableAliasContext ctx) {
        if (null == ctx.children) {
            return null;
        }
        String res = ctx.getChild(1).getText();
        System.out.println(res);
        return res;
    }

    @Override
    public MergeAction visitAssignmentList(CarbonSqlBaseParser.AssignmentListContext ctx) {
        //  UPDATE SET * /
        //  UPDATE SET assignmentList
        // This can only be UpdateAction
        //UpdateAction updateAction = new UpdateAction();
        return null;
    }

    @Override
    public MergeAction visitMatchedAction(CarbonSqlBaseParser.MatchedActionContext ctx) {
        //  For matched action, it can be
        //  DELETE /
        //  UPDATE SET * /
        //  UPDATE SET assignmentList
        int childCount = ctx.getChildCount();
        if (childCount == 1) {
            // when matched ** delete
            return new DeleteAction();
        } else {
            System.out.println("\n VisitMathedAction need to be implement");
            //todo put update map into updateAction constructor
            // Update set ()
            return visitAssignmentList((CarbonSqlBaseParser.AssignmentListContext) ctx.getChild(ctx.getChildCount() - 1));
        }
    }

    @Override
    public InsertAction visitNotMatchedAction(CarbonSqlBaseParser.NotMatchedActionContext ctx) {
        // FOR NOT MATCH ACTION,
        // INSERT *
        // INSERT '(' columns=multipartIdentifierList ')'
        //        VALUES '(' expression (',' expression)* ')'

        //This function seems harder than visit matchedAction
        if (ctx.getChildCount() <= 2) {
            //INSERT *
            //Build Maps
//            return InsertAction.apply();
            return null;
        } else {
            //Iterate the children, get the multiPartIdentifierList and Expression Lists
            // Construct Insert Map --> get the correspond col name
            // One Multipart Identifier <==> One Expression
            int all = ctx.getChildCount();
            List<TmpColumn> multipartIdentifiers = new ArrayList();
            List<Expression> expressions = new ArrayList<>();
            for (int i = 0; i < all; i++) {
                if (ctx.getChild(i) instanceof CarbonSqlBaseParser.MultipartIdentifierListContext) {
                    //Iterate the whole multipartIdentifierList
                    for (int currIdx = 0; currIdx < ctx.getChild(i).getChildCount(); currIdx++) {
                        multipartIdentifiers.add(visitMultipartIdentifier((CarbonSqlBaseParser.MultipartIdentifierContext) ctx.getChild(i).getChild(currIdx), ""));
                    }
                } else if (ctx.getChild(i) instanceof CarbonSqlBaseParser.ExpressionContext) {
                    try {
                        expressions.add(sparkParser.parseExpression(ctx.getChild(i).getText()));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
            //Build Maps?
//            return InsertAction.apply();
            return null;
        }
    }


    @Override
    public MergeAction visitNotMatchedClause(CarbonSqlBaseParser.NotMatchedClauseContext ctx) {
        // INSERT ASTERISK
        // INSERT '(' columns=multipartIdentifierList ')' --> multipartIdentifierList
        // VALUES '(' expression (',' expression)* ')' --> multiple expression

        //This may have a complex not matched action

        int currIdx = 0;
        for (; currIdx < ctx.getChildCount(); currIdx++) {
            if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.NotMatchedActionContext) {
                visitNotMatchedAction((CarbonSqlBaseParser.NotMatchedActionContext) ctx.getChild(currIdx));
            } else {
                // Do nothing
            }
        }
        return null;
    }

    @Override
    public MergeAction visitMatchedClause(CarbonSqlBaseParser.MatchedClauseContext ctx) {
        //There will be lots of childs at ctx,
        // we need to find the predicate
        int currIdx = 0;
        for (; currIdx < ctx.getChildCount(); currIdx++) {

            if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.MatchedActionContext) {
                visitMatchedAction((CarbonSqlBaseParser.MatchedActionContext) ctx.getChild(currIdx));
            } else {
                // Do nothing
            }
        }
        return null;
    }

    @Override
    public MergeInto visitMergeInto(CarbonSqlBaseParser.MergeIntoContext ctx) {
        // Get the join expression
        Expression joinExpression = null;

        try {
            // The join Expression is PredicatedNode
            joinExpression = sparkParser.parseExpression(ctx.mergeCondition.getText());
        } catch (ParseException e) {
            e.printStackTrace();
        }


        //Build a matched clause list to store the when matched and when not matched clause
        int size = ctx.getChildCount();
        int currIdx = 0;
        ArrayList<Expression> matchedExpr = new ArrayList<>();
        ArrayList<MergeAction> matchedAct = new ArrayList<>();
        ArrayList<Expression> notMatchedExpr = new ArrayList<>();
        ArrayList<MergeAction> notMatchedAct = new ArrayList<>();

        // ArrayList<MergeMatch> matches=new ArrayList<>();
        // There should be two List to store the result retrieve from when matched / when not matched context
        while (currIdx < size) {
            if(ctx.getChild(currIdx) instanceof  CarbonSqlBaseParser.PredicatedContext){
                //This branch will visit the Join Expression
                ctx.getChild(currIdx).getChildCount();
            }else if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.MatchedClauseContext) {
                //Get the whenMatched expression
                try {
                    // We need to make sure which data structure to use to store the matchedExpr and matchedAction
                    // WHEN MATCHED THEN DELETE
                    // It need to check if boolean Expression exist
                    if (ctx.getChild(currIdx).getChildCount() > 4) {
                        Expression whenMatchedExpression = sparkParser.parseExpression(((CarbonSqlBaseParser.MatchedClauseContext) ctx.getChild(currIdx)).booleanExpression().getText());
                        matchedExpr.add(whenMatchedExpression);

                    } else {
                        //Should we add something here??
                        System.out.println("No booleanExpression" + ctx.getChildCount());
                        //Add a empty When not matched
//                        WhenMatched whenMatched=new WhenMatched().addAction();
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                matchedAct.add(visitMatchedClause((CarbonSqlBaseParser.MatchedClauseContext) ctx.getChild(currIdx)));
            } else if (ctx.getChild(currIdx) instanceof CarbonSqlBaseParser.NotMatchedClauseContext) {
                //Get the whenNotMatched expression
                try {
                    // WHEN NOT MATCHED THEN XXXX
                    if (ctx.getChild(currIdx).getChildCount() > 5) {
                        Expression whenNotMatchedExpression = sparkParser.parseExpression(((CarbonSqlBaseParser.NotMatchedClauseContext) ctx.getChild(currIdx)).booleanExpression().getText());
                        notMatchedExpr.add(whenNotMatchedExpression);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                notMatchedAct.add(visitNotMatchedClause((CarbonSqlBaseParser.NotMatchedClauseContext) ctx.getChild(currIdx)));
            } else {
                // Do nothing
            }
            currIdx++;
        }
        //After Iteration, we can construct the KV of the expression and mergeActions
        System.out.println("Check the not/matchExpr and not/matchActs");
        //todo add the Expression to MERGEINT
        //MergeInto

        return new MergeInto(visitMultipartIdentifier(ctx.target), visitMultipartIdentifier(ctx.source), joinExpression);
    }

    @Override
    public CarbonJoinExpression visitComparison(CarbonSqlBaseParser.ComparisonContext ctx) {
        ctx.getText();

        return new CarbonJoinExpression();
    }


    @Override
    public Object visitValueExpressionDefault(CarbonSqlBaseParser.ValueExpressionDefaultContext ctx) {
        ctx.getText();
        return super.visitValueExpressionDefault(ctx);
    }

    @Override
    public CarbonJoinExpression visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx) {
        CarbonJoinExpression mc = visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0));
        return mc;
    }


    public CarbonJoinExpression visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx, String type) {
        CarbonJoinExpression mc = visitComparison((CarbonSqlBaseParser.ComparisonContext) ctx.getChild(0).getChild(0).getChild(1).getChild(0).getChild(0));
        return mc;
    }

    @Override
    public TmpColumn visitDereference(CarbonSqlBaseParser.DereferenceContext ctx) {
        // In this part, it will return two colunm name
        int count = ctx.getChildCount();
        TmpColumn col = new TmpColumn();
        if (count == 3) {
            String tableName = ctx.getChild(0).getText();
            String colName = ctx.getChild(2).getText();
            col = new TmpColumn(tableName, colName);
        }
        return col;
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


    public TmpColumn visitMultipartIdentifier(CarbonSqlBaseParser.MultipartIdentifierContext ctx, String x) {
        TmpColumn column = new TmpColumn();
        List<CarbonSqlBaseParser.ErrorCapturingIdentifierContext> parts = ctx.parts;
        if (parts.size() == 2) {
            column.setTable(parts.get(0).getText());
            column.setColName(parts.get(1).getText());
        }
        if (parts.size() == 1) {
            column.setColName(parts.get(0).getText());
        }
        return column;
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
