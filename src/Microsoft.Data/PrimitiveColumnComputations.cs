

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// Generated from PrimitiveColumnComputations.tt. Do not modify directly

using System;

namespace Microsoft.Data
{
    internal interface IPrimitiveColumnComputation<T>
        where T : struct
    {
        void Abs(PrimitiveColumnContainer<T> column);
        void All(PrimitiveColumnContainer<T> column, out bool ret);
        void Any(PrimitiveColumnContainer<T> column, out bool ret);
        void Cummax(PrimitiveColumnContainer<T> column);
        void Cummin(PrimitiveColumnContainer<T> column);
        void Cumprod(PrimitiveColumnContainer<T> column);
        void Cumsum(PrimitiveColumnContainer<T> column);
        void Max(PrimitiveColumnContainer<T> column, out T ret);
        void Min(PrimitiveColumnContainer<T> column, out T ret);
        void Prod(PrimitiveColumnContainer<T> column, out T ret);
        void Sum(PrimitiveColumnContainer<T> column, out T ret);
        void Round(PrimitiveColumnContainer<T> column);
    }

    internal static class PrimitiveColumnComputation<T>
        where T : struct
    {
        public static IPrimitiveColumnComputation<T> Instance => PrimitiveColumnComputation.GetComputation<T>();
    }

    internal static class PrimitiveColumnComputation
    {
        public static IPrimitiveColumnComputation<T> GetComputation<T>()
            where T : struct
        {
            if (typeof(T) == typeof(bool))
            {
                return (IPrimitiveColumnComputation<T>)new BoolComputation();
            }
            else if (typeof(T) == typeof(byte))
            {
                return (IPrimitiveColumnComputation<T>)new ByteComputation();
            }
            else if (typeof(T) == typeof(char))
            {
                return (IPrimitiveColumnComputation<T>)new CharComputation();
            }
            else if (typeof(T) == typeof(decimal))
            {
                return (IPrimitiveColumnComputation<T>)new DecimalComputation();
            }
            else if (typeof(T) == typeof(double))
            {
                return (IPrimitiveColumnComputation<T>)new DoubleComputation();
            }
            else if (typeof(T) == typeof(float))
            {
                return (IPrimitiveColumnComputation<T>)new FloatComputation();
            }
            else if (typeof(T) == typeof(int))
            {
                return (IPrimitiveColumnComputation<T>)new IntComputation();
            }
            else if (typeof(T) == typeof(long))
            {
                return (IPrimitiveColumnComputation<T>)new LongComputation();
            }
            else if (typeof(T) == typeof(sbyte))
            {
                return (IPrimitiveColumnComputation<T>)new SByteComputation();
            }
            else if (typeof(T) == typeof(short))
            {
                return (IPrimitiveColumnComputation<T>)new ShortComputation();
            }
            else if (typeof(T) == typeof(uint))
            {
                return (IPrimitiveColumnComputation<T>)new UIntComputation();
            }
            else if (typeof(T) == typeof(ulong))
            {
                return (IPrimitiveColumnComputation<T>)new ULongComputation();
            }
            else if (typeof(T) == typeof(ushort))
            {
                return (IPrimitiveColumnComputation<T>)new UShortComputation();
            }
            throw new NotSupportedException();
        }
    }

    internal class BoolComputation : IPrimitiveColumnComputation<bool>
    {
        public void Abs(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

        public void All(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            ret = true;
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer.Span[i] == false)
                    {
                        ret = false;
                        break;
                    }
                }
            }
        }

        public void Any(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            ret = false;
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer.Span[i] == true)
                    {
                        ret = true;
                        break;
                    }
                }
            }
        }

        public void Cummax(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

        public void Cummin(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

        public void Cumprod(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

        public void Cumsum(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

        public void Max(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Min(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Prod(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Sum(PrimitiveColumnContainer<bool> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Round(PrimitiveColumnContainer<bool> column)
        {
            throw new NotSupportedException();
        }

    }
    internal class ByteComputation : IPrimitiveColumnComputation<byte>
    {
        public void Abs(PrimitiveColumnContainer<byte> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (byte)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<byte> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<byte> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<byte> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<byte> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<byte> column)
        {
            var ret = (byte)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<byte> column)
        {
            var ret = (byte)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<byte> column, out byte ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<byte> column, out byte ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<byte> column, out byte ret)
        {
            ret = (byte)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<byte> column, out byte ret)
        {
            ret = (byte)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (byte)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<byte> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (byte)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class CharComputation : IPrimitiveColumnComputation<char>
    {
        public void Abs(PrimitiveColumnContainer<char> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (char)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<char> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<char> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<char> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<char> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<char> column)
        {
            var ret = (char)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<char> column)
        {
            var ret = (char)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<char> column, out char ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<char> column, out char ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<char> column, out char ret)
        {
            ret = (char)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<char> column, out char ret)
        {
            ret = (char)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (char)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<char> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (char)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class DecimalComputation : IPrimitiveColumnComputation<decimal>
    {
        public void Abs(PrimitiveColumnContainer<decimal> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (decimal)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<decimal> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<decimal> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<decimal> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<decimal> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<decimal> column)
        {
            var ret = (decimal)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<decimal> column)
        {
            var ret = (decimal)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<decimal> column, out decimal ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<decimal> column, out decimal ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<decimal> column, out decimal ret)
        {
            ret = (decimal)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<decimal> column, out decimal ret)
        {
            ret = (decimal)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (decimal)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<decimal> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (decimal)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class DoubleComputation : IPrimitiveColumnComputation<double>
    {
        public void Abs(PrimitiveColumnContainer<double> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (double)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<double> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<double> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<double> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<double> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<double> column)
        {
            var ret = (double)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<double> column)
        {
            var ret = (double)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<double> column, out double ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<double> column, out double ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<double> column, out double ret)
        {
            ret = (double)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<double> column, out double ret)
        {
            ret = (double)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (double)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<double> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (double)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class FloatComputation : IPrimitiveColumnComputation<float>
    {
        public void Abs(PrimitiveColumnContainer<float> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (float)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<float> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<float> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<float> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<float> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<float> column)
        {
            var ret = (float)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<float> column)
        {
            var ret = (float)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<float> column, out float ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<float> column, out float ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<float> column, out float ret)
        {
            ret = (float)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<float> column, out float ret)
        {
            ret = (float)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (float)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<float> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (float)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class IntComputation : IPrimitiveColumnComputation<int>
    {
        public void Abs(PrimitiveColumnContainer<int> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (int)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<int> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<int> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<int> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<int> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<int> column)
        {
            var ret = (int)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<int> column)
        {
            var ret = (int)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<int> column, out int ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<int> column, out int ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<int> column, out int ret)
        {
            ret = (int)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<int> column, out int ret)
        {
            ret = (int)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (int)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<int> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (int)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class LongComputation : IPrimitiveColumnComputation<long>
    {
        public void Abs(PrimitiveColumnContainer<long> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (long)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<long> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<long> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<long> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<long> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<long> column)
        {
            var ret = (long)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<long> column)
        {
            var ret = (long)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<long> column, out long ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<long> column, out long ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<long> column, out long ret)
        {
            ret = (long)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<long> column, out long ret)
        {
            ret = (long)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (long)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<long> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (long)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class SByteComputation : IPrimitiveColumnComputation<sbyte>
    {
        public void Abs(PrimitiveColumnContainer<sbyte> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (sbyte)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<sbyte> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<sbyte> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<sbyte> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<sbyte> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<sbyte> column)
        {
            var ret = (sbyte)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<sbyte> column)
        {
            var ret = (sbyte)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<sbyte> column, out sbyte ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<sbyte> column, out sbyte ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<sbyte> column, out sbyte ret)
        {
            ret = (sbyte)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<sbyte> column, out sbyte ret)
        {
            ret = (sbyte)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (sbyte)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<sbyte> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (sbyte)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class ShortComputation : IPrimitiveColumnComputation<short>
    {
        public void Abs(PrimitiveColumnContainer<short> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (short)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<short> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<short> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<short> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<short> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<short> column)
        {
            var ret = (short)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<short> column)
        {
            var ret = (short)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<short> column, out short ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<short> column, out short ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<short> column, out short ret)
        {
            ret = (short)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<short> column, out short ret)
        {
            ret = (short)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (short)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<short> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (short)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class UIntComputation : IPrimitiveColumnComputation<uint>
    {
        public void Abs(PrimitiveColumnContainer<uint> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (uint)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<uint> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<uint> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<uint> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<uint> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<uint> column)
        {
            var ret = (uint)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<uint> column)
        {
            var ret = (uint)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<uint> column, out uint ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<uint> column, out uint ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<uint> column, out uint ret)
        {
            ret = (uint)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<uint> column, out uint ret)
        {
            ret = (uint)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (uint)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<uint> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (uint)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class ULongComputation : IPrimitiveColumnComputation<ulong>
    {
        public void Abs(PrimitiveColumnContainer<ulong> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (ulong)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<ulong> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<ulong> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<ulong> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<ulong> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<ulong> column)
        {
            var ret = (ulong)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<ulong> column)
        {
            var ret = (ulong)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<ulong> column, out ulong ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<ulong> column, out ulong ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<ulong> column, out ulong ret)
        {
            ret = (ulong)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<ulong> column, out ulong ret)
        {
            ret = (ulong)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ulong)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<ulong> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (ulong)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
    internal class UShortComputation : IPrimitiveColumnComputation<ushort>
    {
        public void Abs(PrimitiveColumnContainer<ushort> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (ushort)(Math.Abs((decimal)buffer.Span[i]));
                }
            }
        }

        public void All(PrimitiveColumnContainer<ushort> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Any(PrimitiveColumnContainer<ushort> column, out bool ret)
        {
            throw new NotSupportedException();
        }

        public void Cummax(PrimitiveColumnContainer<ushort> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(Math.Max(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cummin(PrimitiveColumnContainer<ushort> column)
        {
            var ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(Math.Min(buffer.Span[i], ret));
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumprod(PrimitiveColumnContainer<ushort> column)
        {
            var ret = (ushort)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(buffer.Span[i] * ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Cumsum(PrimitiveColumnContainer<ushort> column)
        {
            var ret = (ushort)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(buffer.Span[i] + ret);
                    buffer.Span[i] = ret;
                }
            }
        }

        public void Max(PrimitiveColumnContainer<ushort> column, out ushort ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(Math.Max(buffer.Span[i], ret));
                }
            }
        }

        public void Min(PrimitiveColumnContainer<ushort> column, out ushort ret)
        {
            ret = column.Buffers[0].Span[0];
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(Math.Min(buffer.Span[i], ret));
                }
            }
        }

        public void Prod(PrimitiveColumnContainer<ushort> column, out ushort ret)
        {
            ret = (ushort)1;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(buffer.Span[i] * ret);
                }
            }
        }

        public void Sum(PrimitiveColumnContainer<ushort> column, out ushort ret)
        {
            ret = (ushort)0;
            for (int bb = 0 ; bb < column.Buffers.Count; bb++)
            {
                var buffer = column.Buffers[bb];
                for (int i = 0; i < buffer.Length; i++)
                {
                    ret = (ushort)(buffer.Span[i] + ret);
                }
            }
        }

        public void Round(PrimitiveColumnContainer<ushort> column)
        {
            for (int b = 0; b < column.Buffers.Count; b++)
            {
                var buffer = column.Buffers[b];
                for (int i = 0; i < buffer.Length; i++)
                {
                    buffer.Span[i] = (ushort)(Math.Round((decimal)buffer.Span[i]));
                }
            }
        }

    }
}
