// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    /// <summary>
    /// A column to hold primitive values such as int, float etc. Other value types are not really supported
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public partial class PrimitiveColumn<T> : BaseColumn
        where T : struct
    {
        internal PrimitiveColumnContainer<T> _columnContainer;

        internal PrimitiveColumn(string name, PrimitiveColumnContainer<T> column) : base(name, column.Length, typeof(T))
        {
            _columnContainer = column;
        }

        public PrimitiveColumn(string name, IEnumerable<T> values) : base(name, 0, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>(values);
            Length = _columnContainer.Length;
        }

        public PrimitiveColumn(string name, bool isNullable = true) : base(name, 0, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>();
        }

        public override object this[long startIndex, int length] {
            get
            {
                if (startIndex > Length )
                {
                    throw new ArgumentException($"Indexer arguments exceed Length {Length} of the Column");
                }
                return _columnContainer[startIndex, length];
            }
        }

        // This method involves boxing
        public override object this[long rowIndex]
        {
            get
            {
                return _columnContainer[rowIndex];
            }
            set
            {
                if (value.GetType() == typeof(T))
                {
                    _columnContainer[rowIndex] = (T)value;
                }
                else
                {
                    throw new ArgumentException(nameof(value));
                }

            }
        }

        public void Append(T value) => _columnContainer.Append(value);

        public override string ToString()
        {
            return $"{Name}: {_columnContainer.ToString()}";
        }

        public PrimitiveColumn<T> Clone()
        {
            PrimitiveColumnContainer<T> newColumnContainer = _columnContainer.Clone();
            return new PrimitiveColumn<T>(Name, newColumnContainer);
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

        public void ApplyElementwise(Func<T, T> func)
        {
            foreach (var buffer in _columnContainer.Buffers)
            {
                Span<T> span = buffer.Span;
                for (int i = 0; i < buffer.Length; i++)
                {
                    span[i] = func(span[i]);
                }
            }
        }
    }
}
