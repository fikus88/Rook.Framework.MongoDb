using System;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rook.Framework.MongoDb.Data;

namespace Rook.Framework.MongoDb.Tests.Unit.Data
{
    [TestClass]
    public class ExpressionExtensionsTests
    {
        [TestMethod]
        public void OrElse_WithTwoExpressions_ReturnsSingleExpressionCombiningTheSuppliedExpressionsWithLogicalOr()
        {
            var testData = new[] { 2, 3, 4, 5, 6, 7, 8 };

            Expression<Func<int, bool>> expression1 = exp1Param => exp1Param % 2 == 0; //Even number
            Expression<Func<int, bool>> expression2 = exp2Param => exp2Param > 5;

            var result = testData.Where(expression1.OrElse(expression2).Compile()).ToList();

            CollectionAssert.AreEquivalent(new[] { 2, 4, 6, 7, 8 }, result);
        }

        [TestMethod]
        public void AndAlso_WithTwoExpressions_ReturnsSingleExpressionCombiningTheSuppliedExpressionsWithLogicalAnd()
        {
            var testData = new[] { 2, 3, 4, 5, 6, 7, 8 };

            Expression<Func<int, bool>> expression1 = exp1Param => exp1Param % 2 == 0; //Even number
            Expression<Func<int, bool>> expression2 = exp2Param => exp2Param > 5;

            var result = testData.Where(expression1.AndAlso(expression2).Compile()).ToList();

            CollectionAssert.AreEquivalent(new[] { 6, 8 }, result);
        }
    }
}
