// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Text;

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

        public PrimitiveColumn(string name, long length = 0, bool isNullable = true) : base(name, length, typeof(T))
        {
            _columnContainer = new PrimitiveColumnContainer<T>(length);
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

        public void Append(T value)
        {
            _columnContainer.Append(value);
            Length++;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            for (long i = 0; i < Math.Min(Length, 25); i++)
            {
                sb.Append(string.Format("{0,-14}", this[i].ToString()));
                sb.AppendLine();
            }
            return sb.ToString();

            return $"{Name}: {_columnContainer.ToString()}";
        }

        public override BaseColumn Clone(BaseColumn mapIndices = null, bool invertMapIndices = false)
        {
            if (!(mapIndices is null))
            {
                if (mapIndices.DataType != typeof(long)) throw new ArgumentException($"Expected sortIndices to be a PrimitiveColumn<long>");
                if (mapIndices.Length != Length) throw new ArgumentException($"{nameof(mapIndices)} must be of length {Length}");
                return _Clone(mapIndices as PrimitiveColumn<long>, invertMapIndices);
            }
            return _Clone();
        }

        public PrimitiveColumn<T> _Clone(PrimitiveColumn<long> mapIndices = null, bool invertMapIndices = false)
        {
            if (mapIndices is null)
            {
                PrimitiveColumnContainer<T> newColumnContainer = _columnContainer.Clone();
                return new PrimitiveColumn<T>(Name, newColumnContainer);
            }
            else
            {
                PrimitiveColumn<T> ret = new PrimitiveColumn<T>(Name);
                if (invertMapIndices == false)
                {
                    for (long i = 0; i < mapIndices.Length; i++)
                    {
                        ret.Append(_columnContainer[mapIndices._columnContainer[i]]);
                    }
                }
                else
                {
                    for (long i = Length - 1; i >= 0; i--)
                    {
                        ret.Append(_columnContainer[mapIndices._columnContainer[i]]);
                    }
                }
                return ret;
            }
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

        public override BaseColumn Clip<U>(U lower, U upper)
        {
            if (typeof(U) == typeof(T))
            {
                return _Clip((T)(object)lower, (T)(object)upper);
            }
            throw new ArgumentException($"Argument types must match Column type {DataType}");
        }

        public override Dictionary<string, float> Description()
        {
            Dictionary<string, float> ret = new Dictionary<string, float>();
            float max = (float)this.Max();
            float min = (float)this.Min();
            float mean = (float)((float)Sum() / Length);
            ret["Max"] = max;
            ret["Min"] = min;
            ret["Mean"] = mean;
            return ret;
        }

        public override BaseColumn ApplyFilter(PrimitiveColumn<bool> filter)
        {
            if (filter.Length != Length)
            {
                throw new ArgumentOutOfRangeException(nameof(filter));
            }
            var ret = new PrimitiveColumn<T>(Name);
            for (int i = 0; i < Length; i++)
            {
                if ((bool)filter[i] == true)
                {
                    ret.Append((T)this[i]);
                }
            }
            return ret;
        }

        public override BaseColumn Filter<U>(U lower, U upper)
        {
            if (typeof(U) == typeof(T))
            {
                return _Filter((T)(object)lower, (T)(object)upper);
            }
            throw new ArgumentException($"Argument types must match Column type {DataType}");
        }

        /// <summary>
        /// Filters values by setting a false flag for values that are outside the input parameters
        /// </summary>
        /// <param name="lo"></param>
        /// <param name="hi"></param>
        /// <returns></returns>
        private PrimitiveColumn<bool> _Filter(T lo, T hi)
        {
            PrimitiveColumn<bool> ret = new PrimitiveColumn<bool>("Filter", Length);
            var comparer = Comparer<T>.Default;
            for (long i = 0; i < Length; i++)
            {
                T value = (T)this[i];

                if (comparer.Compare(value, lo) < 0)
                {
                    ret[i] = false;
                }
                else if (comparer.Compare(value, hi) > 0)
                {
                    ret[i] = false;
                }
                else
                {
                    ret[i] = true;
                }
            }
            return ret;
        }

        private BaseColumn _Clip(T lo, T hi)
        {
            var ret = _Clone();
            var comparer = Comparer<T>.Default;
            for (long i = 0; i < Length; i++)
            {
                T value = (T)ret[i];
                
                if (comparer.Compare(value, lo) < 0)
                {
                    ret[i] = lo;
                }
                if (comparer.Compare(value, hi) > 0)
                {
                    ret[i] = hi;
                }
            }
            return ret;
        }
    }
}
