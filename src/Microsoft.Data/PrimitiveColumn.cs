﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace Microsoft.Data
{
    /// <summary>
    /// A column to hold primitive types such as int, float etc.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public partial class PrimitiveColumn<T> : BaseColumn, IEnumerable<T>
        where T : unmanaged
    {
        private PrimitiveColumnContainer<T> _columnContainer;

        internal PrimitiveColumn(string name, PrimitiveColumnContainer<T> column) : base(name, column.Length, typeof(T))
        {
            _columnContainer = column;
        }

        public PrimitiveColumn(string name, IEnumerable<T> values) : base(name, 0, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>(values);
            Length = _columnContainer.Length;
        }

        public PrimitiveColumn(string name, long length = 0) : base(name, length, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>(length);
        }

        public new IList<T?> this[long startIndex, int length]
        {
            get
            {
                if (startIndex > Length)
                {
                    throw new ArgumentOutOfRangeException(nameof(startIndex));
                }
                return _columnContainer[startIndex, length];
            }
        }

        protected override object GetValue(long startIndex, int length)
        {
            if (startIndex > Length)
            {
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            }
            return _columnContainer[startIndex, length];
        }

        protected override object GetValue(long rowIndex)
        {
            return _columnContainer[rowIndex];
        }

        protected override void SetValue(long rowIndex, object value)
        {
            if (value == null || value.GetType() == typeof(T))
            {
                _columnContainer[rowIndex] = (T?)value;
            }
            else
            {
                throw new ArgumentException(Strings.MismatchedValueType + $" {DataType.ToString()}", nameof(value));
            }
        }

        public new T? this[long rowIndex]
        {
            get
            {
                return _columnContainer[rowIndex];
            }
            set
            {
                if (value == null || value.GetType() == typeof(T))
                {
                    _columnContainer[rowIndex] = value;
                }
                else
                {
                    throw new ArgumentException(Strings.MismatchedValueType + $" {DataType.ToString()}", nameof(value));
                }
            }
        }

        public override void Resize(long length)
        {
            _columnContainer.Resize(length);
            Length = _columnContainer.Length;
        }

        public void Append(T? value)
        {
            _columnContainer.Append(value);
            Length++;
        }

        public void AppendMany(T? value, long count)
        {
            _columnContainer.AppendMany(value, count);
            Length += count;
        }

        public override long NullCount
        {
            get
            {
                Debug.Assert(_columnContainer.NullCount >= 0);
                return _columnContainer.NullCount;
            }
        }

        public bool IsValid(long index) => _columnContainer.IsValid(index);

        public override string ToString()
        {
            return $"{Name}: {_columnContainer.ToString()}";
        }

         public override Dictionary<string, float> Description() 
        { 
            Dictionary<string, float> ret = new Dictionary<string, float>(); 
            float max = (float)Convert.ChangeType(this.Max(), typeof(float));
            float min = (float)Convert.ChangeType(this.Min(), typeof(float));
            float mean = (float)((float)Convert.ChangeType(Sum(), typeof(float)) / Length); 
            ret["Max"] = max; 
            ret["Min"] = min; 
            ret["Mean"] = mean; 
            return ret; 
        }

        public override BaseColumn Clone(BaseColumn mapIndices = null, bool invertMapIndices = false, long numberOfNullsToAppend = 0)
        {
            PrimitiveColumn<T> clone;
            if (!(mapIndices is null))
            {
                if (mapIndices.DataType != typeof(long))
                    throw new ArgumentException(Strings.MismatchedValueType + " PrimitiveColumn<long>", nameof(mapIndices));
                if (mapIndices.Length > Length)
                    throw new ArgumentException(Strings.MapIndicesExceedsColumnLenth, nameof(mapIndices));
                clone = Clone(mapIndices as PrimitiveColumn<long>, invertMapIndices);
            }
            else
            {
                clone = Clone();
            }
            Debug.Assert(!ReferenceEquals(clone, null));
            clone.AppendMany(null, numberOfNullsToAppend);
            return clone;
        }

        public PrimitiveColumn<T> Clone(PrimitiveColumn<long> mapIndices = null, bool invertMapIndices = false)
        {
            if (mapIndices is null)
            {
                PrimitiveColumnContainer<T> newColumnContainer = _columnContainer.Clone();
                return new PrimitiveColumn<T>(Name, newColumnContainer);
            }
            else
            {
                if (mapIndices.Length > Length)
                    throw new ArgumentException(Strings.MapIndicesExceedsColumnLenth, nameof(mapIndices));

                PrimitiveColumn<T> ret = new PrimitiveColumn<T>(Name, mapIndices.Length);
                ret._columnContainer._modifyNullCountWhileIndexing = false;
                if (invertMapIndices == false)
                {
                    for (long i = 0; i < mapIndices.Length; i++)
                    {
                        T? value = _columnContainer[mapIndices._columnContainer[i].Value];
                        ret[i] = value;
                        if (!value.HasValue)
                            ret._columnContainer.NullCount++;
                    }
                }
                else
                {
                    long mapIndicesIndex = mapIndices.Length - 1;
                    for (long i = 0; i < mapIndices.Length; i++)
                    {
                        T? value = _columnContainer[mapIndices._columnContainer[mapIndicesIndex - i].Value];
                        ret[i] = value;
                        if (!value.HasValue)
                            ret._columnContainer.NullCount++;
                    }
                }
                ret._columnContainer._modifyNullCountWhileIndexing = true;
                return ret;
            }
        }

        public PrimitiveColumn<T> Clone(IEnumerable<long> mapIndices)
        {
            IEnumerator<long> rows = mapIndices.GetEnumerator();
            PrimitiveColumn<T> ret = new PrimitiveColumn<T>(Name);
            ret._columnContainer._modifyNullCountWhileIndexing = false;
            long numberOfRows = 0;
            while (rows.MoveNext() && numberOfRows < Length)
            {
                numberOfRows++;
                long curRow = rows.Current;
                T? value = _columnContainer[curRow];
                ret[curRow] = value;
                if (!value.HasValue)
                    ret._columnContainer.NullCount++;
            }
            ret._columnContainer._modifyNullCountWhileIndexing = true;
            return ret;
        }

        internal PrimitiveColumn<bool> CloneAsBoolColumn()
        {
            PrimitiveColumnContainer<bool> newColumnContainer = _columnContainer.CloneAsBoolContainer();
            return new PrimitiveColumn<bool>(Name, newColumnContainer);
        }

        internal PrimitiveColumn<double> CloneAsDoubleColumn()
        {
            PrimitiveColumnContainer<double> newColumnContainer = _columnContainer.CloneAsDoubleContainer();
            return new PrimitiveColumn<double>(Name, newColumnContainer);
        }

        internal PrimitiveColumn<decimal> CloneAsDecimalColumn()
        {
            PrimitiveColumnContainer<decimal> newColumnContainer = _columnContainer.CloneAsDecimalContainer();
            return new PrimitiveColumn<decimal>(Name, newColumnContainer);
        }

        public override GroupBy GroupBy(int columnIndex, DataFrame parent)
        {
            Dictionary<T, ICollection<long>> dictionary = GroupColumnValues<T>();
            return new GroupBy<T>(parent, columnIndex, dictionary);
        }

        public override Dictionary<TKey, ICollection<long>> GroupColumnValues<TKey>()
        {
            if (typeof(TKey) == typeof(T))
            {
                Dictionary<T, ICollection<long>> multimap = new Dictionary<T, ICollection<long>>(EqualityComparer<T>.Default);
                for (long i = 0; i < Length; i++)
                {
                    bool containsKey = multimap.TryGetValue(this[i] ?? default, out ICollection<long> values);
                    if (containsKey)
                    {
                        values.Add(i);
                    }
                    else
                    {
                        multimap.Add(this[i] ?? default, new List<long>() { i });
                    }
                }
                return multimap as Dictionary<TKey, ICollection<long>>;
            }
            else
            {
                throw new NotImplementedException(nameof(TKey));
            }
        }

        public void ApplyElementwise(Func<T, T> func)
        {
            foreach (DataFrameBuffer<T> buffer in _columnContainer.Buffers)
            {
                Span<T> span = buffer.Span;
                for (int i = 0; i < buffer.Length; i++)
                {
                    span[i] = func(span[i]);
                }
            }
        }

        protected internal override void AddDataViewColumn(DataViewSchema.Builder builder)
        {
            builder.AddColumn(Name, GetDataViewType());
        }

        private static DataViewType GetDataViewType()
        {
            if (typeof(T) == typeof(bool))
            {
                return BooleanDataViewType.Instance;
            }
            else if (typeof(T) == typeof(byte))
            {
                return NumberDataViewType.Byte;
            }
            else if (typeof(T) == typeof(double))
            {
                return NumberDataViewType.Double;
            }
            else if (typeof(T) == typeof(float))
            {
                return NumberDataViewType.Single;
            }
            else if (typeof(T) == typeof(int))
            {
                return NumberDataViewType.Int32;
            }
            else if (typeof(T) == typeof(long))
            {
                return NumberDataViewType.Int64;
            }
            else if (typeof(T) == typeof(sbyte))
            {
                return NumberDataViewType.SByte;
            }
            else if (typeof(T) == typeof(short))
            {
                return NumberDataViewType.Int16;
            }
            else if (typeof(T) == typeof(uint))
            {
                return NumberDataViewType.UInt32;
            }
            else if (typeof(T) == typeof(ulong))
            {
                return NumberDataViewType.UInt64;
            }
            else if (typeof(T) == typeof(ushort) )
            {
                return NumberDataViewType.UInt16;
            }
            // The following 2 implementations are not ideal, but IDataView doesn't support
            // these types
            else if (typeof(T) == typeof(char))
            {
                return NumberDataViewType.UInt16;
            }
            else if (typeof(T) == typeof(decimal))
            {
                return NumberDataViewType.Double;
            }

            throw new NotSupportedException();
        }

        protected internal override Delegate GetDataViewGetter(DataViewRowCursor cursor)
        {
            // special cases for types not supported
            if (typeof(T) == typeof(char))
            {
                return CreateCharValueGetterDelegate(cursor);
            }
            else if (typeof(T) == typeof(decimal))
            {
                return CreateDecimalValueGetterDelegate(cursor);
            }
            return CreateValueGetterDelegate(cursor);
        }

        private ValueGetter<T> CreateValueGetterDelegate(DataViewRowCursor cursor) =>
            (ref T value) => value = this[cursor.Position].Value;

        private ValueGetter<ushort> CreateCharValueGetterDelegate(DataViewRowCursor cursor) =>
            (ref ushort value) => value = (ushort)Convert.ChangeType(this[cursor.Position].Value, typeof(ushort));

        private ValueGetter<double> CreateDecimalValueGetterDelegate(DataViewRowCursor cursor) =>
            (ref double value) => value = (double)Convert.ChangeType(this[cursor.Position].Value, typeof(double));

        public IEnumerator<T> GetEnumerator() => _columnContainer.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}