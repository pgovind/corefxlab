using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    public class DataFrameColumn<T> : BaseDataFrameColumn
        where T : struct
    {
        private DataFrameColumnContainer<T> _columnContainer;
        public Type DataType = typeof(T);
        public DataFrameColumn(string name, DataFrameColumnContainer<T> column) : base(name)
        {
            _columnContainer = column;
        }
        public DataFrameColumn(string name, T[] columnData) : base(name)
        {
            throw new NotImplementedException();
        }
        public DataFrameColumn(string name, IEnumerable<T> values) : base(name)
        {
            _columnContainer = new DataFrameColumnContainer<T>(values);
            Length = _columnContainer.Length;
        }
        public DataFrameColumn(string name, bool isNullable = true) : base(name)
        {
            _columnContainer = new DataFrameColumnContainer<T>();
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
                _columnContainer[rowIndex] = (T)value;
            }
        }
    }
}
