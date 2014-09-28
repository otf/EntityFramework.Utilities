using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace EntityFramework.Utilities
{
    public static class EFQueryHelpers
    {
        /// <summary>
        /// Loads a child collection in a more efficent way than the standard Include. Will run all involved queries as NoTracking
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TChild"></typeparam>
        /// <param name="query"></param>
        /// <param name="context"></param>
        /// <param name="collectionSelector">The navigation property. It can be filtered and sorted with the methods Where,OrderBy(Descending),ThenBy(Descending) </param>
        /// <returns></returns>
        public static EFUQueryable<T> IncludeEFU<T, TChild>(this IQueryable<T> query, DbContext context, Expression<Func<T, IEnumerable<TChild>>> collectionSelector)
            where T : class
            where TChild : class
        {
            MemberExpression collectionSelector2 = null;
            LambdaExpression collectionSelector3 = null;
            var mce = collectionSelector.Body as MethodCallExpression;
            if (mce != null && mce.Method.Name == "SelectMany")
            {
                collectionSelector2 = (MemberExpression)mce.Arguments[0];
                collectionSelector3 = (LambdaExpression)mce.Arguments[1];
            }

            var octx = (context as IObjectContextAdapter).ObjectContext;
            var cSpaceTables = octx.MetadataWorkspace.GetItems<EntityType>(DataSpace.CSpace);

            Type intermidiateType = collectionSelector2 != null ? collectionSelector2.Type.GetGenericArguments().First() : typeof(TChild);

            Delegate fkGetter =  GetForeignKeyGetter(typeof(T), intermidiateType, cSpaceTables);
            Delegate fkGetter2 = collectionSelector2 != null ? GetForeignKeyGetter(intermidiateType, typeof(TChild), cSpaceTables) : null;

            var cSpaceType = cSpaceTables.Single(t => t.Name == typeof(T).Name); //Use single to avoid any problems with multiple tables using the same type
            var cSpaceType2 = cSpaceTables.Single(t => t.Name == intermidiateType.Name); //Use single to avoid any problems with multiple tables using the same type
            var keys = cSpaceType.KeyProperties;
            if (keys.Count > 1)
            {
                throw new InvalidOperationException("The include method only works on single key entities");
            }
            var keys2 = cSpaceType2.KeyProperties;
            if (keys2.Count > 1)
            {
                throw new InvalidOperationException("The include method only works on single key entities");
            }
            var pkInfo = typeof(T).GetProperty(keys.First().Name);
            var pkInfo2 = intermidiateType.GetProperty(keys2.First().Name);
            var pkGetter = MakeGetterDelegate(typeof(T), pkInfo);
            var pkGetter2 = MakeGetterDelegate(intermidiateType, pkInfo2);

            var childCollectionModifiers = new List<MethodCallExpression>();
            var childProp = SetCollectionModifiersAndGetChildProperty(typeof(T), typeof(TChild), collectionSelector, childCollectionModifiers);
            var setter = MakeSetterDelegate(typeof(T), childProp);
            Delegate setter2 = null;
            PropertyInfo childProp2 = null;
            if (collectionSelector2 != null)
            {
                childProp2 = SetCollectionModifiersAndGetChildProperty(intermidiateType, typeof(TChild), collectionSelector3, childCollectionModifiers);
                setter2 = MakeSetterDelegate(intermidiateType, childProp2);
            }

            var e = new IncludeExecuter<T>
            {
                ElementType = typeof(TChild),
                SingleItemLoader = (parent) =>
                {
                    if (parent == null)
                    {
                        return;
                    }
                    var children = octx.CreateObjectSet<TChild>();
                    var lambdaExpression = GetRootEntityToChildCollectionSelector(typeof(T), typeof(TChild), cSpaceType);

                    var q = ApplyChildCollectionModifiers<TChild>(children, childCollectionModifiers);

                    var rootPK = pkGetter.DynamicInvoke(parent);
                    var param = Expression.Parameter(typeof(TChild), "x");
                    var fk = GetFKProperty(typeof(T), typeof(TChild), cSpaceTables);
                    var body = Expression.Equal(Expression.Property(param, fk), Expression.Constant(rootPK));
                    var where = Expression.Lambda<Func<TChild, bool>>(body, param);

                    q = q.AsNoTracking().Where(where);

                    setter.DynamicInvoke(parent, q.ToList());
                },
                Loader = (rootFilters, parents) =>
                {
                    var baseType = typeof(T).BaseType != typeof(object) ? typeof(T).BaseType : typeof(T);

                    dynamic dynamicSet = octx.GetType()
                                    .GetMethod("CreateObjectSet", new Type[] { })
                                    .MakeGenericMethod(baseType)
                                    .Invoke(octx, new Object[] { });

                    var set = dynamicSet.OfType<T>() as ObjectQuery<T>;
                    IQueryable<T> q = set;

                    foreach (var item in rootFilters)
                    {
                        var newSource = Expression.Constant(q);
                        var arguments = Enumerable.Repeat(newSource, 1).Concat(item.Arguments.Skip(1)).ToArray();
                        var newMethods = Expression.Call(item.Method, arguments);
                        q = q.Provider.CreateQuery<T>(newMethods);
                    }
                    var lambdaExpression = GetRootEntityToChildCollectionSelector(typeof(T), intermidiateType, cSpaceType);
                    var selectMany = typeof(Queryable).GetMethods().First(m => m.Name == "SelectMany").MakeGenericMethod(new[] { typeof(T), intermidiateType });
                    var childQ = (IEnumerable<object>)selectMany.Invoke(null, new object[] { q, lambdaExpression });
                    {
                        //childQ = ApplyChildCollectionModifiers<TChild>(childQ, childCollectionModifiers);

                        var toLookup = typeof(Enumerable).GetMethods().First(m => m.Name == "ToLookup").MakeGenericMethod(new[] { intermidiateType, typeof(object) });
                        var dict = toLookup.Invoke(null, new object[] { childQ, fkGetter });
                        var list = parents.Cast<T>().ToList();

                        foreach (var parent in list)
                        {
                            var prop = pkGetter.DynamicInvoke(parent);
                            var contains = typeof(ILookup<,>).MakeGenericType(typeof(object), intermidiateType).GetMethod("Contains");
                            var at = typeof(ILookup<,>).MakeGenericType(typeof(object), intermidiateType).GetProperty("Item").GetGetMethod();
                            var toList = typeof(Enumerable).GetMethod("ToList").MakeGenericMethod(intermidiateType);
                            var childs = ((bool)contains.Invoke(dict, new []{ prop})) ? toList.Invoke(null, new []{ (object) (at.Invoke(dict, new [] { prop }))}) : (object)new List<object>();
                            setter.DynamicInvoke(parent, childs);
                        }
                    }
                    if (collectionSelector2 != null)
                    {
                        var lambdaExpression2 = GetRootEntityToChildCollectionSelector(intermidiateType, typeof(TChild), cSpaceType2);

                        var selectMany2 = typeof(Queryable).GetMethods().First(m => m.Name == "SelectMany").MakeGenericMethod(new[] { intermidiateType, typeof(TChild) });
                        var childQ2 = (IEnumerable<object>)selectMany2.Invoke(null, new object[] { childQ, lambdaExpression2 });
                        var toLookup = typeof(Enumerable).GetMethods().First(m => m.Name == "ToLookup").MakeGenericMethod(new[] { typeof(TChild), typeof(object) });
                        var dict = (ILookup<object, TChild>)toLookup.Invoke(null, new object[] { childQ2, fkGetter2 });
                        //childQ = ApplyChildCollectionModifiers<TChild>(childQ, childCollectionModifiers);
                        foreach (var parent in childQ)
                        {
                            var prop = (object)pkGetter2.DynamicInvoke(parent);
                            var childs = dict.Contains(prop) ? dict[prop].ToList() : new List<TChild>();
                            setter2.DynamicInvoke(parent, childs);
                        }
                    }

                }
            };

            return new EFUQueryable<T>(query.AsNoTracking()).Include(e);
        }

        private static IQueryable<TChild> ApplyChildCollectionModifiers<TChild>(IQueryable<TChild> childQ, List<MethodCallExpression> childCollectionModifiers) where TChild : class
        {
            foreach (var item in childCollectionModifiers)
            {
                switch (item.Method.Name)
                {
                    case "Where":
                        childQ = childQ.Where((Expression<Func<TChild, bool>>)item.Arguments[1]);
                        break;
                    case "OrderBy":
                    case "ThenBy":
                    case "OrderByDescending":
                    case "ThenByDescending":
                        childQ = SortQuery(childQ, item, item.Method.Name);
                        break;
                    default:
                        throw new NotSupportedException("The method " + item.Method.Name + " is not supported in the child query");
                }
            }
            return childQ;
        }

        private static PropertyInfo SetCollectionModifiersAndGetChildProperty(Type parentType, Type childType, LambdaExpression collectionSelector, List<MethodCallExpression> childCollectionModifiers)
        {
            var temp = collectionSelector.Body;
            while (temp is MethodCallExpression)
            {
                var mce = temp as MethodCallExpression;
                childCollectionModifiers.Add(mce);
                temp = mce.Arguments[0];
            }
            childCollectionModifiers.Reverse(); //We parse from right to left so reverse it
            if (!(temp is MemberExpression))
            {
                throw new ArgumentException("Could not find a MemberExpression", "collectionSelector");
            }

            var childProp = (temp as MemberExpression).Member as PropertyInfo;
            return childProp;
        }

        private static Delegate GetForeignKeyGetter(Type parentType, Type childType , System.Collections.ObjectModel.ReadOnlyCollection<EntityType> cSpaceTables)
        {
            var fkInfo = GetFKProperty(parentType, childType, cSpaceTables);
            var fkGetter = MakeGetterDelegate(childType, fkInfo);
            return fkGetter;
        }

        private static PropertyInfo GetFKProperty(Type parentType, Type childType, System.Collections.ObjectModel.ReadOnlyCollection<EntityType> cSpaceTables)
        {
            var cSpaceChildType = cSpaceTables.Single(t => t.Name == childType.Name); //Use single to avoid any problems with multiple tables using the same type
            var fk = cSpaceChildType.NavigationProperties.First(n => n.ToEndMember.GetEntityType().Name == parentType.Name).GetDependentProperties().First();
            var fkInfo = childType.GetProperty(fk.Name);
            return fkInfo;
        }

        private static IQueryable<TChild> SortQuery<TChild>(IQueryable<TChild> query, MethodCallExpression item, string method)
        {
            var body = (item.Arguments[1] as LambdaExpression);

            MethodCallExpression call = Expression.Call(
                typeof(Queryable),
                method,
                new[] { typeof(TChild), body.Body.Type },
                query.Expression,
                Expression.Quote(body));

            return (IOrderedQueryable<TChild>)query.Provider.CreateQuery<TChild>(call);
        }

        private static Expression GetRootEntityToChildCollectionSelector(Type parentType, Type childType, EntityType cSpaceType)
        {
            var parameter = Expression.Parameter(parentType, "t");
            var memberExpression = Expression.Property(parameter, cSpaceType.NavigationProperties.First(p => p.ToEndMember.GetEntityType().Name == childType.Name).Name);
            var fType = typeof(Func<,>);
            var fTypeInstance = fType.MakeGenericType(parentType, typeof(IEnumerable<>).MakeGenericType(childType));

            var lambdaExpression = Expression.Lambda(fTypeInstance, memberExpression, parameter);
            return lambdaExpression;
        }

        static Delegate MakeSetterDelegate(Type parentType, PropertyInfo property)
        {
            MethodInfo setMethod = property.GetSetMethod();
            if (setMethod != null && setMethod.GetParameters().Length == 1)
            {
                var target = Expression.Parameter(parentType);
                var value = Expression.Parameter(typeof(object));
                var body = Expression.Call(target, setMethod,
                    Expression.Convert(value, property.PropertyType));
                return Expression.Lambda(body, target, value)
                    .Compile();
            }
            else
            {
                return null;
            }
        }

        static Delegate MakeGetterDelegate(Type childType, PropertyInfo property)
        {
            MethodInfo getMethod = property.GetGetMethod();
            if (getMethod != null)
            {
                var target = Expression.Parameter(childType);
                var body = Expression.Call(target, getMethod);
                Expression conversion = Expression.Convert(body, typeof(object));
                return Expression.Lambda(conversion, target).Compile();
            }
            else
            {
                return null;
            }
        }
    }
}
