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
        class Path
        {
            public LambdaExpression CollectionSelector;
            public Type FromType;
            public Type ToType;
            public Delegate FkGetter;
            public EntityType CSpaceType;
            public Delegate PkGetter;
            public Delegate Setter;
            public List<MethodCallExpression> CollectionModifiers = new List<MethodCallExpression>();
        }

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
            var pathes = new List<Path>();
            var octx = (context as IObjectContextAdapter).ObjectContext;
            var cSpaceTables = octx.MetadataWorkspace.GetItems<EntityType>(DataSpace.CSpace);
            var mce = collectionSelector.Body as MethodCallExpression;
            if (mce != null && mce.Method.Name == "SelectMany")
            {
                var intermediateAccess = (MemberExpression)mce.Arguments[0];
                var intermediateType = intermediateAccess.Type.GetGenericArguments().First();
                pathes.Add(new Path() { CollectionSelector = collectionSelector, FromType = typeof(T), ToType = intermediateType });
                Hoge(cSpaceTables, pathes[0]);
                pathes.Add(new Path() { CollectionSelector = (LambdaExpression)mce.Arguments[1], FromType = intermediateType, ToType = typeof(TChild) });
                Hoge(cSpaceTables, pathes[1]);
            }
            else
            {
                pathes.Add(new Path() { CollectionSelector = collectionSelector, FromType = typeof(T), ToType = typeof(TChild) });
                Hoge(cSpaceTables, pathes[0]);
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
                    var lambdaExpression = GetRootEntityToChildCollectionSelector(pathes[0].FromType, pathes[0].ToType, pathes[0].CSpaceType);

                    var q = ApplyChildCollectionModifiers<TChild>(children, pathes[0].CollectionModifiers);

                    var rootPK = pathes[0].PkGetter.DynamicInvoke(parent);
                    var param = Expression.Parameter(typeof(TChild), "x");
                    var fk = GetFKProperty(pathes[0].FromType, pathes[0].ToType, cSpaceTables);
                    var body = Expression.Equal(Expression.Property(param, fk), Expression.Constant(rootPK));
                    var where = Expression.Lambda<Func<TChild, bool>>(body, param);

                    q = q.AsNoTracking().Where(where);

                    pathes[0].Setter.DynamicInvoke(parent, q.ToList());
                },
                Loader = (rootFilters, parents) =>
                {
                    var baseType = pathes[0].FromType.BaseType != typeof(object) ? pathes[0].FromType.BaseType : pathes[0].FromType;

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
                    {
                        var lambdaExpression = GetRootEntityToChildCollectionSelector(pathes[0].FromType, pathes[0].ToType, pathes[0].CSpaceType);
                        var selectMany = typeof(Queryable).GetMethods().First(m => m.Name == "SelectMany").MakeGenericMethod(new[] { pathes[0].FromType, pathes[0].ToType });
                        var childQ = (IEnumerable<object>)selectMany.Invoke(null, new object[] { q, lambdaExpression });
                        //childQ = ApplyChildCollectionModifiers<TChild>(childQ, childCollectionModifiers);

                        var toLookup = typeof(Enumerable).GetMethods().First(m => m.Name == "ToLookup").MakeGenericMethod(new[] { pathes[0].ToType, typeof(object) });
                        var dict = toLookup.Invoke(null, new object[] { childQ, pathes[0].FkGetter });
                        var list = parents.Cast<T>().ToList();

                        foreach (var parent in list)
                        {
                            var prop = pathes[0].PkGetter.DynamicInvoke(parent);
                            var contains = typeof(ILookup<,>).MakeGenericType(typeof(object), pathes[0].ToType).GetMethod("Contains");
                            var at = typeof(ILookup<,>).MakeGenericType(typeof(object), pathes[0].ToType).GetProperty("Item").GetGetMethod();
                            var toList = typeof(Enumerable).GetMethod("ToList").MakeGenericMethod(pathes[0].ToType);
                            var childs = ((bool)contains.Invoke(dict, new []{ prop})) ? toList.Invoke(null, new []{ (object) (at.Invoke(dict, new [] { prop }))}) : (object)new List<object>();
                            pathes[0].Setter.DynamicInvoke(parent, childs);
                        }
                    }
                    if (1 < pathes.Count)
                    {
                        var lambdaExpression = GetRootEntityToChildCollectionSelector(pathes[0].FromType, pathes[0].ToType, pathes[0].CSpaceType);
                        var selectMany = typeof(Queryable).GetMethods().First(m => m.Name == "SelectMany").MakeGenericMethod(new[] { pathes[0].FromType, pathes[0].ToType });
                        var childQ = (IEnumerable<object>)selectMany.Invoke(null, new object[] { q, lambdaExpression });
                        //childQ = ApplyChildCollectionModifiers<TChild>(childQ, childCollectionModifiers);
                        var lambdaExpression2 = GetRootEntityToChildCollectionSelector(pathes[1].FromType, pathes[1].ToType, pathes[1].CSpaceType);

                        var selectMany2 = typeof(Queryable).GetMethods().First(m => m.Name == "SelectMany").MakeGenericMethod(new[] { pathes[1].FromType, pathes[1].ToType });
                        var childQ2 = (IEnumerable<object>)selectMany2.Invoke(null, new object[] { childQ, lambdaExpression2 });
                        var toLookup = typeof(Enumerable).GetMethods().First(m => m.Name == "ToLookup").MakeGenericMethod(new[] { pathes[1].ToType, typeof(object) });
                        var dict = (ILookup<object, TChild>)toLookup.Invoke(null, new object[] { childQ2, pathes[1].FkGetter });
                        foreach (var parent in childQ)
                        {
                            var prop = (object)pathes[1].PkGetter.DynamicInvoke(parent);
                            var childs = dict.Contains(prop) ? dict[prop].ToList() : new List<TChild>();
                            pathes[1].Setter.DynamicInvoke(parent, childs);
                        }
                    }

                }
            };

            return new EFUQueryable<T>(query.AsNoTracking()).Include(e);
        }

        private static void Hoge(System.Collections.ObjectModel.ReadOnlyCollection<EntityType> cSpaceTables, Path path)
        {
            path.FkGetter = GetForeignKeyGetter(path.FromType, path.ToType, cSpaceTables);
            path.CSpaceType = cSpaceTables.Single(t => t.Name == path.FromType.Name); //Use single to avoid any problems with multiple tables using the same type
            var keys = path.CSpaceType.KeyProperties;
            if (keys.Count > 1)
            {
                throw new InvalidOperationException("The include method only works on single key entities");
            }
            var pkInfo = path.FromType.GetProperty(keys.First().Name);
            path.PkGetter = MakeGetterDelegate(path.FromType, pkInfo);
            var childProp = SetCollectionModifiersAndGetChildProperty(path.FromType, path.ToType, path.CollectionSelector, path.CollectionModifiers);
            path.Setter = MakeSetterDelegate(path.FromType, childProp);
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
