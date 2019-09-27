package com.facebook.presto.expressiontranslationtoolkit.translator;

import com.facebook.presto.expressiontranslationtoolkit.TargetExpression;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RowExpressionTranslator
{
//    public static final int MAX_CLAUSES_TO_PUSHDOWN = 10;
//    public static final int MAX_TERMS_PER_CLAUSE_TO_PUSHDOWN = 6;

    private final Visitor visitor;
    private final LogicalRowExpressions logicalRowExpressions;

    public RowExpressionTranslator(
            FunctionTranslator functionTranslator, FunctionMetadataManager functionMetadataManager, StandardFunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
    {
        this.visitor = new Visitor(functionTranslator, functionMetadataManager);
        this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, functionResolution, functionMetadataManager);
    }

    public TargetExpression translate(
            RowExpression expression,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        return logicalRowExpressions.convertToConjunctiveNormalForm(expression).accept(visitor, assignments);
    }

    private static class Visitor
            implements RowExpressionVisitor<TargetExpression, Map<VariableReferenceExpression, ColumnHandle>>
    {
        private final FunctionTranslator functionTranslator;
        private final FunctionMetadataManager functionMetadataManager;

        private Visitor(FunctionTranslator functionTranslator, FunctionMetadataManager functionMetadataManager)
        {
            this.functionTranslator = requireNonNull(functionTranslator);
            this.functionMetadataManager = requireNonNull(functionMetadataManager);
        }

        @Override
        public TargetExpression visitInputReference(InputReferenceExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TargetExpression visitConstant(ConstantExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
            return functionTranslator.translate(node);
        }

        @Override
        public TargetExpression visitVariableReference(VariableReferenceExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
//            CubrickColumnHandle cubrickColumnHandle = (CubrickColumnHandle) context.get(node);
//
//            checkArgument(cubrickColumnHandle != null, "cubrickColumnHandle is null");
//            return new TargetExpression(PUSHABLE, Optional.of("`" + cubrickColumnHandle.getColumnName() + "`"));
            return null;
        }

        @Override
        public TargetExpression visitCall(CallExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
            FunctionMetadata metadata = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle());
            ImmutableList.Builder<TargetExpression> argumentSqls = ImmutableList.builder();

            for (RowExpression expression : node.getArguments()) {
                TargetExpression sql = expression.accept(this, context);
                argumentSqls.add(sql);
            }

            return functionTranslator.translate(metadata, argumentSqls.build());
        }

        @Override
        public TargetExpression visitLambda(LambdaDefinitionExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
            return TargetExpression.empty();
        }

        /**
         * Refer to class TargetExpression.PushdownState for push definition
         * *A AND B:*
         * - if both A and B are fully pushable, A AND B is *fully pushable*
         * - if neither A nor B is pushable, A AND B is *not pushable*
         * <p>
         * *A OR B:*
         * - if both A and B are fully pushable, A OR B is *fully pushable* (same constraint)
         * - w.l.o.g, if A is *not pushable*, no matter B is *pushable (fully or partially) or not*
         * B can not be pushed down, A OR B *not pushable* (harden the constraint)
         */
        @Override
        public TargetExpression visitSpecialForm(SpecialFormExpression node, Map<VariableReferenceExpression, ColumnHandle> context)
        {
//            // TODO: We only support AND OR pushdown for now, but we may support other special form in the future.
//            if (node.getForm() == AND || node.getForm() == OR) {
//                // Expression should be in CNF form now - meaning all ANDs are brought top top and ORs are at the bottom
//                if (node.getForm() == AND) {
//                    // Collect all clauses. Recursively call on each term in the clause and filter for valid pushable
//                    // ones. Enforce the MAX NUMBER OF CLAUSES constraint and combine to a single Cubrick SQL statement
//                    List<RowExpression> clauses = ImmutableList.copyOf(extractConjuncts(node));
//                    List<TargetExpression> pushdownCubrickSQLs = clauses
//                            .stream()
//                            .map(expression -> expression.accept(this, context))
//                            .filter(TargetExpression::isPushable)
//                            .collect(toImmutableList());
//
//                    if (!pushdownCubrickSQLs.isTranslatedExpressionPresent()) {
//                        return combinePredicates(AND, pushdownCubrickSQLs.subList(0, Math.min(pushdownCubrickSQLs.size(), MAX_CLAUSES_TO_PUSHDOWN)));
//                    }
//                }
//                else {
//                    // Collect all terms. Recursively call on each term in the clause. If they are all pushable and
//                    // honour the MAX TERMS PER CLAUSE constraint then combine to a single Cubrick SQL statement
//                    List<RowExpression> terms = ImmutableList.copyOf(extractDisjuncts(node));
//                    List<TargetExpression> pushdownCubrickSQLs = terms
//                            .stream()
//                            .map(expression -> expression.accept(this, context))
//                            .filter(TargetExpression::isPushable)
//                            .collect(toImmutableList());
//
//                    if (!pushdownCubrickSQLs.isTranslatedExpressionPresent()
//                            && pushdownCubrickSQLs.size() == terms.size()
//                            && pushdownCubrickSQLs.size() <= MAX_TERMS_PER_CLAUSE_TO_PUSHDOWN) {
//                        return combinePredicates(OR, pushdownCubrickSQLs);
//                    }
//                }
//            }
//            return notPushable();
            return null;
        }

        /**
         * Takes pushable SQL statements collection and joins them with AND or OR.
         * @param form AND or OR
         * @param collection of pushable cubrick sql statements we must combine using `form`
         * @return new pushable AND or OR expression as cubrick sql
         */
        private TargetExpression combinePredicates(
                SpecialFormExpression.Form form,
                Collection<TargetExpression> collection)
        {
//            if (form != AND && form != OR) {
//                throw new IllegalArgumentException(String.format("Form must be AND or OR. Found: %s", form));
//            }
//
//            if (collection.size() == 0) {
//                throw new IllegalArgumentException("expressions passed in must have size >= 1");
//            }
//
//            return collection
//                    .stream()
//                    .reduce((cSql1, cSql2) -> new TargetExpression(
//                            PUSHABLE,
//                            Optional.of(
//                                    format("(%s) %s (%s)",
//                                            cSql1.getCubrickSQL(),
//                                            form == AND ? "AND" : "OR",
//                                            cSql2.getCubrickSQL()))))
//                    .get();
            return null;
        }
    }
}
