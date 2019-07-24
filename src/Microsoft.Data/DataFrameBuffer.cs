﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Microsoft.Data
{
    /// <summary>
    /// A basic immutable store to hold values in a DataFrame column. Supports wrapping with an ArrowBuffer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataFrameBuffer<T>
        where T : struct
    {
        // TODO: Change this to Memory<T>

        private ReadOnlyMemory<byte> _readOnlyMemory;

        public virtual ReadOnlyMemory<byte> ReadOnlyMemory => _readOnlyMemory;

        protected static int Size = Unsafe.SizeOf<T>();

        protected int Capacity => ReadOnlyMemory.Length / Size;

        public static int MaxBufferCapacity
        {
            get
            {
                return Int32.MaxValue / Size;
            }
        }

        public int MaxCapacity => Int32.MaxValue / Size;

        public ReadOnlySpan<T> ReadOnlySpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => MemoryMarshal.Cast<byte, T>(ReadOnlyMemory.Span);
        }

        public int Length { get; internal set; }

        public DataFrameBuffer(int numberOfValues = 8)
        {
            Size = Unsafe.SizeOf<T>();
            if ((long)numberOfValues * Size > MaxCapacity)
            {
                throw new ArgumentException($"{numberOfValues} exceeds buffer capacity", nameof(numberOfValues));
            }
            _readOnlyMemory = new byte[numberOfValues * Size];
        }

        public DataFrameBuffer(ReadOnlyMemory<byte> buffer, int length)
        {
            Size = Unsafe.SizeOf<T>();
            _readOnlyMemory = buffer;
            Length = length;
        }

        internal virtual T this[int index]
        {
            get
            {
                if (index > Length)
                    throw new ArgumentOutOfRangeException(nameof(index));
                return ReadOnlySpan[index];
            }
            set => throw new NotSupportedException();
        }

        internal bool this[int startIndex, int length, IList<T> returnList]
        {
            get
            {
                if (startIndex > Length)
                    throw new ArgumentOutOfRangeException(nameof(startIndex));
                long endIndex = Math.Min(Length, startIndex + length);
                for (int i = startIndex; i < endIndex; i++)
                {
                    returnList.Add(ReadOnlySpan[i]);
                }
                return true;
            }
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            ReadOnlySpan<T> span = ReadOnlySpan;
            for (int i = 0; i < Length; i++)
            {
                sb.Append(span[i]).Append(" ");
            }
            return sb.ToString();
        }

    }
}
