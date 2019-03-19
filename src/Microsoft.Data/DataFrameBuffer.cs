using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Microsoft.Data
{
    public class DataFrameBuffer<T>
        where T : struct
    {
        public Memory<byte> Memory { get; private set; }
        private readonly int _size;
        private int Capacity => Memory.Length / _size;
        public int MaxCapacity => Int32.MaxValue / _size;
        public Span<T> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => MemoryMarshal.Cast<byte, T>(Memory.Span);
        }
        public int Length { get; private set; }

        public DataFrameBuffer(int numberOfValues = 8)
        {
            _size = Unsafe.SizeOf<T>();
            Memory = new byte[numberOfValues * _size];
            Length = 0;
        }
        public void Append(T value)
        {
            if (Length == MaxCapacity)
            {
                throw new ArgumentException("Current buffer is full. Please create a new one", nameof(value));
            }
            EnsureCapacity(1);
            Span[Length] = value;
            if (Length < MaxCapacity) ++Length;
        }
        public void EnsureCapacity(int numberOfValues)
        {
            long newLength = Length + numberOfValues;
            if (newLength > MaxCapacity)
            {
                throw new ArgumentException("Current buffer is full. Please create a new one", nameof(numberOfValues));
            }

            if (newLength > Capacity)
            {
                var newCapacity = Math.Max(newLength * _size, Memory.Length * 2);
                var memory = new Memory<byte>(new byte[newCapacity]);
                Memory.CopyTo(memory);
                Memory = memory;
            }
        }
        internal T this[int index]
        {
            get
            {
                if (index > Length) throw new ArgumentOutOfRangeException(nameof(index));
                return Span[index];
            }
            set
            {
                if (index > Length) throw new ArgumentOutOfRangeException(nameof(index));
                Span[index] = value;
            }
        }
        internal bool this[int startIndex, int length, IList<T> returnList]
        {
            get
            {
                if (startIndex > Length) throw new ArgumentOutOfRangeException(nameof(startIndex));
                long endIndex = Math.Min(Length, startIndex + length);
                for (int ii = startIndex; ii < endIndex; ii++)
                {
                    returnList.Add(Span[ii]);
                }
                return true;
            }
        }
    }
}
