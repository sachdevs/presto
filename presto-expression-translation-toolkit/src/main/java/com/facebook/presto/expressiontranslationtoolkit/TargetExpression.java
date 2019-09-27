package com.facebook.presto.expressiontranslationtoolkit;

import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class TargetExpression<T>
{
    private RowExpression originalExpression;
    private List<TargetExpression> params;
    private Optional<T> translatedExpression;

    public TargetExpression(Optional<T> translatedExpression)
    {
        this.translatedExpression = translatedExpression;
    }

    public static TargetExpression empty()
    {
        return new TargetExpression(Optional.empty());
    }

    public boolean isTranslatedExpressionPresent()
    {
        return translatedExpression.isPresent();
    }

    public RowExpression getOriginalExpression()
    {
        return originalExpression;
    }

    public void setOriginalExpression(RowExpression originalExpression)
    {
        this.originalExpression = originalExpression;
    }

    public List<TargetExpression> getParams()
    {
        return ImmutableList.copyOf(params);
    }

    public void addParam(TargetExpression param)
    {
        this.params.add(param);
    }

    public Optional<T> getTranslatedExpression()
    {
        return translatedExpression;
    }

    public void setTranslatedExpression(Optional<T> translatedExpression)
    {
        this.translatedExpression = translatedExpression;
    }
}
