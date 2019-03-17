using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    public abstract class BaseDataFrameColumn
    {
        public BaseDataFrameColumn(string name, long length = 0)
        {
            Length = length;
            Name = name;
        }
        public long Length { get; protected set; }
        public long NullCount { get; protected set; }
        public string Name;

        public virtual object this[long rowIndex] { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }
        public virtual object this[long startIndex, int length] { get { throw new NotImplementedException(); } }

    }
    public class DataFrameColumnContainer<T>
        where T : struct
    {
        public IList<DataFrameBuffer<T>> Buffers = new List<DataFrameBuffer<T>>();
        public DataFrameColumnContainer(T[] values)
        {
            values = values ?? throw new ArgumentNullException(nameof(values));
            long ii = 0;
            long length = values.LongLength;
            DataFrameBuffer<T> curBuffer;
            if (Buffers.Count == 0)
            {
                curBuffer = new DataFrameBuffer<T>();
                Buffers.Add(curBuffer);
            }
            else
            {
                curBuffer = Buffers[Buffers.Count - 1];
            }
            for (; ii < length; ii++)
            {
                if (curBuffer.Length == curBuffer.MaxCapacity)
                {
                    curBuffer = new DataFrameBuffer<T>();
                    Buffers.Add(curBuffer);
                }
                curBuffer.Append(values[ii]);
                Length++;
            }
        }
        public DataFrameColumnContainer(IEnumerable<T> values) {
            values = values ?? throw new ArgumentNullException(nameof(values));
            if (Buffers.Count == 0)
            {
                Buffers.Add(new DataFrameBuffer<T>());
            }
            var curBuffer = Buffers[Buffers.Count - 1];
            foreach (var value in values)
            {
                if (curBuffer.Length == curBuffer.MaxCapacity)
                {
                    curBuffer = new DataFrameBuffer<T>();
                    Buffers.Add(curBuffer);
                }
                curBuffer.Append(value);
                Length++;
            }
        }
        public DataFrameColumnContainer() { }
        public long Length; 
        //TODO:
        public long NullCount => throw new NotImplementedException();
        private int GetArrayContainingRowIndex(ref long rowIndex)
        {
            if (rowIndex > Length)
            {
                throw new ArgumentException($"Index {rowIndex} cannot be greater than the Column's Length {Length}");
            }
            int curArrayIndex = 0;
            int numBuffers = Buffers.Count;
            while (curArrayIndex < numBuffers && rowIndex > Buffers[curArrayIndex].Length)
            {
                rowIndex -= Buffers[curArrayIndex].Length;
                curArrayIndex++;
            }
            return curArrayIndex;
        }

        public IList<T> this[long startIndex, int length]
        {
            get
            {
                var ret = new List<T>();
                long endIndex = startIndex + length;
                int arrayIndex = GetArrayContainingRowIndex(ref startIndex);
                bool temp = Buffers[arrayIndex][(int)startIndex, length, ret];
                while (ret.Count < length)
                {
                    long nextRowIndex = startIndex + ret.Count + 1;
                    arrayIndex++;
                    temp = Buffers[arrayIndex][(int)nextRowIndex, length - ret.Count, ret];
                }
                return ret;
            }
        }
        public T this[long rowIndex]
        {
            get
            {
                int arrayIndex = GetArrayContainingRowIndex(ref rowIndex);
                return Buffers[arrayIndex][(int)rowIndex];
            }
            set
            {
                int arrayIndex = GetArrayContainingRowIndex(ref rowIndex);
                Buffers[arrayIndex][(int)rowIndex] = value;
            }
        }
    }
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
