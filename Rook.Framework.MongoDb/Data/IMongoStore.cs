using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using MongoDB.Driver;

#pragma warning disable 618

namespace Rook.Framework.MongoDb.Data
{
	public interface IMongoStore
    {
        long Count<T>() where T : DataEntityBase;

        long Count<T>(Expression<Func<T, bool>> expression, Collation collation = null) where T : DataEntityBase;
		IEnumerable<TField> Distinct<TField, TCollection>(Expression<Func<TCollection, TField>> fieldSelector, FilterDefinition<TCollection> filterExpression, Collation collation = null) where TCollection : DataEntityBase;		
		T Get<T>(object id) where T : DataEntityBase;
		IEnumerable<T> Get<T>(Expression<Func<T, bool>> filter, Collation collation = null) where T : DataEntityBase;
        IMongoCollection<T> GetCollection<T>() where T : DataEntityBase;		
		IList<T> GetList<T>(Expression<Func<T, bool>> filter, Collation collation = null) where T : DataEntityBase;
        void Put<T>(T entityToStore) where T : DataEntityBase;
		void Put<T>(T entityToStore, Expression<Func<T, bool>> filter, Collation collation = null) where T : DataEntityBase;
		IQueryable<T> QueryableCollection<T>() where T : DataEntityBase;
		void Remove<T>(object id) where T : DataEntityBase;
		void RemoveEntity<T>(T entityToRemove) where T : DataEntityBase;
		void Remove<T>(Expression<Func<T, bool>> filter, Collation collation = null) where T : DataEntityBase;
		void Update<T>(Expression<Func<T, bool>> filter, UpdateDefinition<T> updates, Collation collation = null) where T : DataEntityBase;
        bool Ping();
    }
}
