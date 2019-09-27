package com.facebook.presto.expressiontranslationtoolkit.translator;

import com.facebook.presto.expressiontranslationtoolkit.TargetExpression;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.expressiontranslationtoolkit.ScalarFromAnnotationsParser.parseFunctionDefinitions;
import static com.facebook.presto.expressiontranslationtoolkit.ScalarFromAnnotationsParser.removeTypeParameters;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public abstract class AbstractFunctionTranslator
        implements FunctionTranslator
{
    private final Map<FunctionMetadata, MethodHandle> translators;
    private final Set<TypeSignature> constantTypes;

    public AbstractFunctionTranslator(List<Class<?>> functions, List<Type> constants)
    {
        this.translators = registerFunctions(functions);
        this.constantTypes = registerConstantTypes(constants);
    }

    public TargetExpression translate(ConstantExpression expression)
    {
        TypeSignature typeSignature = removeTypeParameters(expression.getType().getTypeSignature());

        if (!constantTypes.contains(typeSignature)) {
            return TargetExpression.empty();
        }

        Object value = expression.getValue();
        if (typeSignature.equals(removeTypeParameters(VARCHAR.getTypeSignature()))) {
            return tanslateVarchar(expression);
        }

        if (typeSignature.equals(removeTypeParameters(REAL.getTypeSignature()))) {
            return tanslateReal(expression);
        }

        return tanslateConstant(expression);
    }

    public TargetExpression translate(FunctionMetadata functionSignature, List<TargetExpression> arguments)
    {
        FunctionMetadata functionMetadata = removeTypeParameters(functionSignature);
        MethodHandle methodHandle = translators.get(functionMetadata);

        if (methodHandle == null) {
            return TargetExpression.empty();
        }

        try {
            return (TargetExpression) methodHandle.invokeWithArguments(arguments);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(t);
        }
    }

    private Map<FunctionMetadata, MethodHandle> registerFunctions(List<Class<?>> classes)
    {
        ImmutableMap.Builder<FunctionMetadata, MethodHandle> functions = ImmutableMap.builder();
        classes.forEach(clazz -> functions.putAll(parseFunctionDefinitions(clazz)));
        return functions.build();
    }

    private Set<TypeSignature> registerConstantTypes(List<Type> classes)
    {
        ImmutableSet.Builder<TypeSignature> constantTypes = ImmutableSet.builder();
        classes.forEach(clazz -> constantTypes.add(removeTypeParameters(clazz.getTypeSignature())));
        return constantTypes.build();
    }
}
