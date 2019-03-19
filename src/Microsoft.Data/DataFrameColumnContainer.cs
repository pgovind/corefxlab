using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data
{
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
}
