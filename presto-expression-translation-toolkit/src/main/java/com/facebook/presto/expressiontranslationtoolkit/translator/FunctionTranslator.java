package com.facebook.presto.expressiontranslationtoolkit.translator;

import com.facebook.presto.expressiontranslationtoolkit.TargetExpression;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.ConstantExpression;

import java.util.List;

public interface FunctionTranslator
{
    TargetExpression tanslateVarchar(ConstantExpression expression);

    TargetExpression tanslateReal(ConstantExpression expression);

    TargetExpression tanslateConstant(ConstantExpression expression);

    TargetExpression translate(ConstantExpression expression);

    TargetExpression translate(FunctionMetadata functionSignature, List<TargetExpression> arguments);
}
