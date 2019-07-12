﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Microsoft.Data
{
    /// <summary>
    /// A column to hold primitive types such as int, float etc.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public partial class PrimitiveColumn<T> : BaseColumn
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

        public PrimitiveColumn(string name, ReadOnlyMemory<byte> buffer, ReadOnlyMemory<byte> nullBitMap, int length = 0, int nullCount = 0) : base(name, length, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>(buffer, nullBitMap, length, nullCount);
        }

        private IArrowType GetArrowType()
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    return BooleanType.Default;
                case Type doubleType when doubleType == typeof(double):
                    return DoubleType.Default;
                case Type floatType when floatType == typeof(float):
                    return FloatType.Default;
                case Type intType when intType == typeof(int):
                    return Int32Type.Default;
                case Type longType when longType == typeof(long):
                    return Int64Type.Default;
                case Type shortType when shortType == typeof(short):
                    return Int16Type.Default;
                case Type uintType when uintType == typeof(uint):
                    return UInt32Type.Default;
                case Type ulongType when ulongType == typeof(ulong):
                    return UInt64Type.Default;
                case Type ushortType when ushortType == typeof(ushort):
                    return UInt16Type.Default;
                case Type decimalType when decimalType == typeof(decimal):
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type sbyteType when sbyteType == typeof(sbyte):
                default:
                    throw new NotImplementedException(nameof(T));
            }
        }

        public override Field Field
        {
            get
            {
                return new Field(Name, GetArrowType(), NullCount != 0);
            }
        }

        public override int MaxRecordBatchLength(long startIndex) => _columnContainer.MaxRecordBatchLength(startIndex);

        public override Apache.Arrow.Array AsArrowArray(long startIndex, int numberOfRows)
        {
            ArrowBuffer valueBuffer = new ArrowBuffer(_columnContainer.GetValueBuffer(startIndex));
            ArrowBuffer nullBuffer = new ArrowBuffer(_columnContainer.GetNullBuffer(startIndex));
            int arrayIndex = _columnContainer.GetArrayContainingRowIndex(startIndex);
            int offset = (int)(startIndex - arrayIndex * DataFrameBuffer<T>.MaxBufferCapacity);
            switch (this)
            {
                case PrimitiveColumn<bool> boolColumn:
                    return new BooleanArray(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<double> doubleColumn:
                    return new DoubleArray(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<float> floatColumn:
                    return new FloatArray(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<int> intColumn:
                    return new Int32Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<long> longColumn:
                    return new Int64Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<short> shortColumn:
                    return new Int16Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<uint> uintColumn:
                    return new UInt32Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<ulong> ulongColumn:
                    return new UInt64Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<ushort> ushortColumn:
                    return new UInt16Array(valueBuffer, nullBuffer, numberOfRows, -1, offset);
                case PrimitiveColumn<byte> byteColumn:
                case PrimitiveColumn<char> charColumn:
                case PrimitiveColumn<decimal> decimalColumn:
                case PrimitiveColumn<sbyte> sbyteColumn:
                default:
                    throw new NotImplementedException(nameof(byteColumn.DataType));
            }
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

        public override BaseColumn Clone(IEnumerable<long> mapIndices, long numberOfNullsToAppend = 0)
        {
            PrimitiveColumn<T> clone = Clone(mapIndices);
            Debug.Assert(!ReferenceEquals(clone, null));
            clone.AppendMany(null, numberOfNullsToAppend);
            return clone;
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

        public override Dictionary<TKey, ICollection<long>> HashColumnValues<TKey>()
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
                MutableDataFrameBuffer<T> mutableBuffer = MutableDataFrameBuffer<T>.GetMutableBuffer(buffer);
                Span<T> span = mutableBuffer.Span;
                for (int i = 0; i < buffer.Length; i++)
                {
                    span[i] = func(span[i]);
                }
            }
        }
    }
}
