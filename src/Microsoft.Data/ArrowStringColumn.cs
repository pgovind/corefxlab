﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Microsoft.Data
{
    /// <summary>
    /// An immutable to hold Arrow style strings
    /// </summary>
    public partial class ArrowStringColumn : BaseColumn
    {
        private IList<DataFrameBuffer<byte>> _dataBuffers;
        private IList<DataFrameBuffer<int>> _offsetsBuffers;
        private IList<DataFrameBuffer<byte>> _nullBitMapBuffers;

        public ArrowStringColumn(string name) : base(name, 0, typeof(string))
        {
            _dataBuffers = new List<DataFrameBuffer<byte>>();
            _offsetsBuffers = new List<DataFrameBuffer<int>>();
            _nullBitMapBuffers = new List<DataFrameBuffer<byte>>();
        }

        public ArrowStringColumn(string name, DataFrameBuffer<byte> dataBuffer, DataFrameBuffer<int> offsetBuffer, DataFrameBuffer<byte> nullBitMapBuffer, int length, int nullCount) : base(name, length, typeof(string))
        {
            if (dataBuffer == null)
                throw new ArgumentNullException(nameof(dataBuffer));
            if (offsetBuffer == null)
                throw new ArgumentNullException(nameof(offsetBuffer));
            if (nullBitMapBuffer == null)
                throw new ArgumentNullException(nameof(nullBitMapBuffer));
            if (length + 1 != offsetBuffer.Length)
                throw new ArgumentException(nameof(offsetBuffer));

            _dataBuffers = new List<DataFrameBuffer<byte>>();
            _offsetsBuffers = new List<DataFrameBuffer<int>>();
            _nullBitMapBuffers = new List<DataFrameBuffer<byte>>();

            _dataBuffers.Add(dataBuffer);
            _offsetsBuffers.Add(offsetBuffer);
            _nullBitMapBuffers.Add(nullBitMapBuffer);

            _nullCount = nullCount;
        }

        private long _nullCount;
        public override long NullCount => _nullCount;

        public bool IsValid(long index) => NullCount == 0 || GetValidityBit(index);

        public bool GetValidityBit(long index)
        {
            if ((ulong)index > (ulong)Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            // First find the right bitMapBuffer
            int bitMapIndex = GetBufferIndexContainingRowIndex(index, out int indexInBuffer);
            Debug.Assert(_nullBitMapBuffers.Count > bitMapIndex);
            DataFrameBuffer<byte> bitMapBuffer = _nullBitMapBuffers[bitMapIndex];
            int bitMapBufferIndex = (int)((uint)index / 8);
            Debug.Assert(bitMapBuffer.Length > bitMapBufferIndex);
            byte curBitMap = bitMapBuffer[bitMapBufferIndex];
            return ((curBitMap >> (indexInBuffer & 7)) & 1) != 0;
        }

        private void SetValidityBit(long index, bool value)
        {
            if ((ulong)index > (ulong)Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            // First find the right bitMapBuffer
            int bitMapIndex = GetBufferIndexContainingRowIndex(index, out int indexInBuffer);
            Debug.Assert(_nullBitMapBuffers.Count > bitMapIndex);
            MutableDataFrameBuffer<byte> bitMapBuffer = (MutableDataFrameBuffer<byte>)_nullBitMapBuffers[bitMapIndex];

            // Set the bit
            int bitMapBufferIndex = (int)((uint)indexInBuffer / 8);
            Debug.Assert(bitMapBuffer.Length >= bitMapBufferIndex);
            if (bitMapBuffer.Length == bitMapBufferIndex)
                bitMapBuffer.Append(0);
            byte curBitMap = bitMapBuffer[bitMapBufferIndex];
            byte newBitMap;
            if (value)
            {
                newBitMap = (byte)(curBitMap | (byte)(1 << (indexInBuffer & 7))); //bit hack for index % 8
                if ((curBitMap >> (indexInBuffer & 7) & 1) == 0 && indexInBuffer < Length && NullCount > 0)
                {
                    // Old value was null.
                    _nullCount--;
                }
            }
            else
            {
                if ((curBitMap >> (indexInBuffer & 7) & 1) == 1 && indexInBuffer < Length)
                {
                    // old value was NOT null and new value is null
                    _nullCount++;
                }
                else if (indexInBuffer == Length)
                {
                    // New entry from an append
                    _nullCount++;
                }
                newBitMap = (byte)(curBitMap & (byte)~(1 << (int)((uint)indexInBuffer & 7)));
            }
            bitMapBuffer[bitMapBufferIndex] = newBitMap;
        }

        // This is an immutable column, however this method exists to support Clone(). Keep this method private
        private void Append(string value)
        {
            if (_dataBuffers.Count == 0)
            {
                _dataBuffers.Add(new MutableDataFrameBuffer<byte>());
                _nullBitMapBuffers.Add(new MutableDataFrameBuffer<byte>());
                _offsetsBuffers.Add(new MutableDataFrameBuffer<int>());
            }
            MutableDataFrameBuffer<int> mutableOffsetsBuffer = _offsetsBuffers[_offsetsBuffers.Count - 1] as MutableDataFrameBuffer<int>;
            if (mutableOffsetsBuffer.Length == 0)
            {
                mutableOffsetsBuffer.Append(0);
            }
            Length++;
            if (value == null)
            {
                mutableOffsetsBuffer.Append(mutableOffsetsBuffer[mutableOffsetsBuffer.Length - 1]);
            }
            else
            {
                byte[] bytes = Encoding.UTF8.GetBytes(value);
                MutableDataFrameBuffer<byte> mutableDataBuffer = _dataBuffers[_dataBuffers.Count - 1] as MutableDataFrameBuffer<byte>;
                if (mutableDataBuffer.Length == DataFrameBuffer<byte>.MaxCapacity)
                {
                    mutableDataBuffer = new MutableDataFrameBuffer<byte>();
                    _dataBuffers.Add(mutableDataBuffer);
                    _nullBitMapBuffers.Add(new MutableDataFrameBuffer<byte>());
                    var offsetBuffer = new MutableDataFrameBuffer<int>();
                    _offsetsBuffers.Add(offsetBuffer);
                    offsetBuffer.Append(0);
                }
                mutableDataBuffer.EnsureCapacity(bytes.Length);
                bytes.AsMemory().CopyTo(mutableDataBuffer.Memory.Slice(mutableDataBuffer.Length));
                mutableDataBuffer.Length += bytes.Length;
                mutableOffsetsBuffer.Append(mutableOffsetsBuffer[mutableOffsetsBuffer.Length - 1] + bytes.Length);
            }
            SetValidityBit(Length - 1, value == null ? true : false);
        }

        private int GetBufferIndexContainingRowIndex(long rowIndex, out int indexInBuffer)
        {
            if (rowIndex >= Length)
            {
                throw new ArgumentOutOfRangeException(Strings.ColumnIndexOutOfRange, nameof(rowIndex));
            }

            // Since the strings here could be of variable length, scan linearly
            int curArrayIndex = 0;
            int numBuffers = _offsetsBuffers.Count;
            while (curArrayIndex < numBuffers && rowIndex > _offsetsBuffers[curArrayIndex].Length - 1)
            {
                rowIndex -= _offsetsBuffers[curArrayIndex].Length - 1;
                curArrayIndex++;
            }
            indexInBuffer = (int)rowIndex;
            return curArrayIndex;
        }

        private ReadOnlySpan<byte> GetBytes(long index)
        {
            int offsetsBufferIndex = GetBufferIndexContainingRowIndex(index, out int indexInBuffer);
            ReadOnlySpan<int> offsetBufferSpan = _offsetsBuffers[offsetsBufferIndex].ReadOnlySpan;
            int currentOffset = offsetBufferSpan[indexInBuffer];
            int nextOffset = offsetBufferSpan[indexInBuffer + 1];
            int numberOfBytes = nextOffset - currentOffset;
            return _dataBuffers[offsetsBufferIndex].ReadOnlySpan.Slice(currentOffset, numberOfBytes);
        }

        protected override object GetValue(long rowIndex)
        {
            if (!IsValid(rowIndex))
            {
                return null;
            }
            var bytes = GetBytes(rowIndex);
            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return Encoding.UTF8.GetString(data, bytes.Length);
            }
        }

        protected override object GetValue(long startIndex, int length)
        {
            var ret = new List<string>();
            while (ret.Count < length)
            {
                ret.Add((string)GetValue(startIndex++));
            }
            return ret;
        }

        protected override void SetValue(long rowIndex, object value)
        {
            throw new NotSupportedException(Strings.ImmutableStringColumn);
        }

        public new string this[long rowIndex]
        {
            get
            {
                return (string)GetValue(rowIndex);
            }
            set => throw new NotSupportedException(Strings.ImmutableStringColumn);
        }

        public new List<string> this[long startIndex, int length]
        {
            get
            {
                var ret = new List<string>();
                while (ret.Count < length)
                {
                    ret.Add((string)GetValue(startIndex++));
                }
                return ret;
            }
        }

        public override Field Field => new Field(Name, StringType.Default, NullCount != 0);

        public override int MaxRecordBatchLength(long startIndex)
        {
            if (Length == 0)
                return 0;
            int offsetsBufferIndex = GetBufferIndexContainingRowIndex(startIndex, out int indexInBuffer);
            Debug.Assert(indexInBuffer <= Int32.MaxValue);
            return _offsetsBuffers[offsetsBufferIndex].Length - indexInBuffer;
        }

        private int GetNullCount(long startIndex, int numberOfRows)
        {
            int nullCount = 0;
            for (long i = startIndex; i < numberOfRows; i++)
            {
                if (!IsValid(i))
                    nullCount++;
            }
            return nullCount;
        }


        public override Apache.Arrow.Array AsArrowArray(long startIndex, int numberOfRows)
        {
            int offsetsBufferIndex = GetBufferIndexContainingRowIndex(startIndex, out int indexInBuffer);
            ArrowBuffer dataBuffer = numberOfRows == 0 ? ArrowBuffer.Empty : new ArrowBuffer(_dataBuffers[offsetsBufferIndex].ReadOnlyMemory);
            ArrowBuffer offsetsBuffer = numberOfRows == 0 ? ArrowBuffer.Empty : new ArrowBuffer(_offsetsBuffers[offsetsBufferIndex].ReadOnlyMemory);
            ArrowBuffer nullBuffer = numberOfRows == 0 ? ArrowBuffer.Empty : new ArrowBuffer(_nullBitMapBuffers[offsetsBufferIndex].ReadOnlyMemory);
            int nullCount = GetNullCount(indexInBuffer, numberOfRows);
            return new StringArray(numberOfRows, offsetsBuffer, dataBuffer, nullBuffer, nullCount, indexInBuffer);
        }

        public override BaseColumn Sort(bool ascending = true) => throw new NotSupportedException();

        public override BaseColumn Clone(BaseColumn mapIndices = null, bool invertMapIndices = false, long numberOfNullsToAppend = 0)
        {
            ArrowStringColumn clone;
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
            for (long i = 0; i < numberOfNullsToAppend; i++)
            {
                clone.Append(null);
            }
            return clone;
        }

        private ArrowStringColumn Clone(PrimitiveColumn<long> mapIndices = null, bool invertMapIndex = false)
        {
            ArrowStringColumn ret = new ArrowStringColumn(Name);
            if (mapIndices is null)
            {
                for (long i = 0; i < Length; i++)
                {
                    ret.Append(this[i]);
                }
            }
            else
            {
                if (mapIndices.Length > Length)
                    throw new ArgumentException(Strings.MapIndicesExceedsColumnLenth, nameof(mapIndices));
                if (invertMapIndex == false)
                {
                    for (long i = 0; i < mapIndices.Length; i++)
                    {
                        ret.Append(this[(long)mapIndices[i]]);
                    }
                }
                else
                {
                    long mapIndicesLengthIndex = mapIndices.Length - 1;
                    for (long i = 0; i < mapIndices.Length; i++)
                    {
                        ret.Append(this[(long)mapIndices[mapIndicesLengthIndex - i]]);
                    }
                }
            }
            return ret;
        }

        public override GroupBy GroupBy(int columnIndex, DataFrame parent)
        {
            Dictionary<string, ICollection<long>> dictionary = GroupColumnValues<string>();
            return new GroupBy<string>(parent, columnIndex, dictionary);
        }

        public override Dictionary<TKey, ICollection<long>> GroupColumnValues<TKey>()
        {
            if (typeof(TKey) == typeof(string))
            {
                Dictionary<string, ICollection<long>> multimap = new Dictionary<string, ICollection<long>>(EqualityComparer<string>.Default);
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
                throw new NotSupportedException(nameof(TKey));
            }
        }

    }
}
