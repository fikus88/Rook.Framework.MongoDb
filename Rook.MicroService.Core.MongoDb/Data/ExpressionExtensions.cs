using System;
using System.Linq.Expressions;

namespace Rook.MicroService.Core.MongoDb.Data
{
    public static class ExpressionExtensions
    {
        public static Expression<Func<T, bool>> OrElse<T>(this Expression<Func<T, bool>> left, Expression<Func<T, bool>> right)
        {
            var parameterSubstitutedRightExpression = new ExpressionModifier(right.Parameters[0], left.Parameters[0]).Visit(right.Body);

            return parameterSubstitutedRightExpression == null ? null : Expression.Lambda<Func<T, bool>>(Expression.OrElse(left.Body, parameterSubstitutedRightExpression), left.Parameters);
        }
        public static Expression<Func<T, bool>> AndAlso<T>(this Expression<Func<T, bool>> left, Expression<Func<T, bool>> right)
        {
            var parameterSubstitutedRightExpression = new ExpressionModifier(right.Parameters[0], left.Parameters[0]).Visit(right.Body);

            return parameterSubstitutedRightExpression == null ? null : Expression.Lambda<Func<T, bool>>(Expression.AndAlso(left.Body, parameterSubstitutedRightExpression), left.Parameters);
        }
    }
}
