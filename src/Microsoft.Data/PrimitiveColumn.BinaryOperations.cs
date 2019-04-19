
// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// Generated from PrimitiveColumn.BinaryOperations.tt. Do not modify directly

using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    public partial class PrimitiveColumn<T> : BaseColumn
        where T : struct
    {
        #region Binary Operations
        public override BaseColumn Add(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Add(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Add(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Add(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Add(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Add(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Add(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Add(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Add(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Add(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Add(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Add(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Add(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Add(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Add<U>(U value)
        {
            return _Add(value);
        }
        public override BaseColumn Subtract(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Subtract(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Subtract(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Subtract(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Subtract(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Subtract(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Subtract(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Subtract(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Subtract(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Subtract(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Subtract(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Subtract(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Subtract(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Subtract(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Subtract<U>(U value)
        {
            return _Subtract(value);
        }
        public override BaseColumn Multiply(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Multiply(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Multiply(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Multiply(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Multiply(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Multiply(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Multiply(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Multiply(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Multiply(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Multiply(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Multiply(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Multiply(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Multiply(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Multiply(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Multiply<U>(U value)
        {
            return _Multiply(value);
        }
        public override BaseColumn Divide(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Divide(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Divide(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Divide(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Divide(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Divide(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Divide(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Divide(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Divide(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Divide(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Divide(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Divide(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Divide(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Divide(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Divide<U>(U value)
        {
            return _Divide(value);
        }
        public override BaseColumn Modulo(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Modulo(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Modulo(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Modulo(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Modulo(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Modulo(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Modulo(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Modulo(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Modulo(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Modulo(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Modulo(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Modulo(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Modulo(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Modulo(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Modulo<U>(U value)
        {
            return _Modulo(value);
        }
        public override BaseColumn And(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _And(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _And(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _And(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _And(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _And(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _And(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _And(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _And(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _And(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _And(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _And(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _And(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _And(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn And<U>(U value)
        {
            return _And(value);
        }
        public override BaseColumn Or(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Or(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Or(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Or(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Or(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Or(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Or(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Or(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Or(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Or(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Or(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Or(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Or(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Or(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Or<U>(U value)
        {
            return _Or(value);
        }
        public override BaseColumn Xor(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Xor(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Xor(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Xor(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Xor(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Xor(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Xor(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Xor(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Xor(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Xor(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Xor(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Xor(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Xor(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Xor(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Xor<U>(U value)
        {
            return _Xor(value);
        }
        public override BaseColumn LeftShift(int value)
        {
            return _LeftShift(value);
        }
        public override BaseColumn RightShift(int value)
        {
            return _RightShift(value);
        }
        public override BaseColumn Equals(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _Equals(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _Equals(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _Equals(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _Equals(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _Equals(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _Equals(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _Equals(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _Equals(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _Equals(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _Equals(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _Equals(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _Equals(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _Equals(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn Equals<U>(U value)
        {
            return _Equals(value);
        }
        public override BaseColumn NotEquals(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _NotEquals(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _NotEquals(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _NotEquals(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _NotEquals(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _NotEquals(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _NotEquals(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _NotEquals(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _NotEquals(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _NotEquals(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _NotEquals(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _NotEquals(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _NotEquals(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _NotEquals(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn NotEquals<U>(U value)
        {
            return _NotEquals(value);
        }
        public override BaseColumn GreaterThanOrEqual(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _GreaterThanOrEqual(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn GreaterThanOrEqual<U>(U value)
        {
            return _GreaterThanOrEqual(value);
        }
        public override BaseColumn LessThanOrEqual(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _LessThanOrEqual(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _LessThanOrEqual(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _LessThanOrEqual(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _LessThanOrEqual(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _LessThanOrEqual(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _LessThanOrEqual(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _LessThanOrEqual(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _LessThanOrEqual(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _LessThanOrEqual(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _LessThanOrEqual(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _LessThanOrEqual(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _LessThanOrEqual(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _LessThanOrEqual(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn LessThanOrEqual<U>(U value)
        {
            return _LessThanOrEqual(value);
        }
        public override BaseColumn GreaterThan(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _GreaterThan(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _GreaterThan(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _GreaterThan(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _GreaterThan(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _GreaterThan(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _GreaterThan(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _GreaterThan(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _GreaterThan(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _GreaterThan(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _GreaterThan(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _GreaterThan(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _GreaterThan(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _GreaterThan(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn GreaterThan<U>(U value)
        {
            return _GreaterThan(value);
        }
        public override BaseColumn LessThan(BaseColumn column)
        {
            switch (column.DataType)
            {
                case Type boolType when boolType == typeof(bool):
                    return _LessThan(column as PrimitiveColumn<bool>);
                case Type byteType when byteType == typeof(byte):
                    return _LessThan(column as PrimitiveColumn<byte>);
                case Type charType when charType == typeof(char):
                    return _LessThan(column as PrimitiveColumn<char>);
                case Type decimalType when decimalType == typeof(decimal):
                    return _LessThan(column as PrimitiveColumn<decimal>);
                case Type doubleType when doubleType == typeof(double):
                    return _LessThan(column as PrimitiveColumn<double>);
                case Type floatType when floatType == typeof(float):
                    return _LessThan(column as PrimitiveColumn<float>);
                case Type intType when intType == typeof(int):
                    return _LessThan(column as PrimitiveColumn<int>);
                case Type longType when longType == typeof(long):
                    return _LessThan(column as PrimitiveColumn<long>);
                case Type sbyteType when sbyteType == typeof(sbyte):
                    return _LessThan(column as PrimitiveColumn<sbyte>);
                case Type shortType when shortType == typeof(short):
                    return _LessThan(column as PrimitiveColumn<short>);
                case Type uintType when uintType == typeof(uint):
                    return _LessThan(column as PrimitiveColumn<uint>);
                case Type ulongType when ulongType == typeof(ulong):
                    return _LessThan(column as PrimitiveColumn<ulong>);
                case Type ushortType when ushortType == typeof(ushort):
                    return _LessThan(column as PrimitiveColumn<ushort>);
                default:
                    throw new NotSupportedException();
            }
        }
        public override BaseColumn LessThan<U>(U value)
        {
            return _LessThan(value);
        }

        internal BaseColumn _Add<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Add(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Add(column.CloneAsDecimalColumn()._columnContainer);
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Add(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Add((column as PrimitiveColumn<decimal>)._columnContainer);
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Add(column.CloneAsDoubleColumn()._columnContainer);
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Add<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Add(value);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Add(DecimalConverter<U>.Instance.GetDecimal(value));
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Add(value);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Add(DecimalConverter<U>.Instance.GetDecimal(value));
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Add(DoubleConverter<U>.Instance.GetDouble(value));
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Subtract<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Subtract(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Subtract(column.CloneAsDecimalColumn()._columnContainer);
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Subtract(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Subtract((column as PrimitiveColumn<decimal>)._columnContainer);
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Subtract(column.CloneAsDoubleColumn()._columnContainer);
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Subtract<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Subtract(value);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Subtract(DecimalConverter<U>.Instance.GetDecimal(value));
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Subtract(value);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Subtract(DecimalConverter<U>.Instance.GetDecimal(value));
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Subtract(DoubleConverter<U>.Instance.GetDouble(value));
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Multiply<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Multiply(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Multiply(column.CloneAsDecimalColumn()._columnContainer);
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Multiply(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Multiply((column as PrimitiveColumn<decimal>)._columnContainer);
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Multiply(column.CloneAsDoubleColumn()._columnContainer);
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Multiply<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Multiply(value);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Multiply(DecimalConverter<U>.Instance.GetDecimal(value));
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Multiply(value);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Multiply(DecimalConverter<U>.Instance.GetDecimal(value));
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Multiply(DoubleConverter<U>.Instance.GetDouble(value));
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Divide<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Divide(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Divide(column.CloneAsDecimalColumn()._columnContainer);
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Divide(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Divide((column as PrimitiveColumn<decimal>)._columnContainer);
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Divide(column.CloneAsDoubleColumn()._columnContainer);
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Divide<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Divide(value);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Divide(DecimalConverter<U>.Instance.GetDecimal(value));
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Divide(value);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Divide(DecimalConverter<U>.Instance.GetDecimal(value));
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Divide(DoubleConverter<U>.Instance.GetDouble(value));
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Modulo<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Modulo(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Modulo(column.CloneAsDecimalColumn()._columnContainer);
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Modulo(column._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Modulo((column as PrimitiveColumn<decimal>)._columnContainer);
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Modulo(column.CloneAsDoubleColumn()._columnContainer);
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Modulo<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Modulo(value);
                        return newColumn;
                    }
                    else 
                    {
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Modulo(DecimalConverter<U>.Instance.GetDecimal(value));
                        return decimalColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<U> newColumn = primitiveColumn.Clone();
                        newColumn._columnContainer.Modulo(value);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Modulo(DecimalConverter<U>.Instance.GetDecimal(value));
                            return decimalColumn;
                        }
                        else
                        {
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Modulo(DoubleConverter<U>.Instance.GetDouble(value));
                            return doubleColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _And<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.And(column._columnContainer);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _And<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.And(value);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Or<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.Or(column._columnContainer);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Or<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.Or(value);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Xor<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.Xor(column._columnContainer);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Xor<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<U> retColumn = (this as PrimitiveColumn<U>).Clone();
                    retColumn._columnContainer.Xor(value);
                    return retColumn;
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type decimalType when decimalType == typeof(decimal):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _LeftShift(int value)
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type byteType when byteType == typeof(byte):
                    PrimitiveColumn<byte> byteColumn = this as PrimitiveColumn<byte>;
                    var newbyteColumn = byteColumn.Clone();
                    newbyteColumn._columnContainer.LeftShift(value);
                    return newbyteColumn;
                case Type charType when charType == typeof(char):
                    PrimitiveColumn<char> charColumn = this as PrimitiveColumn<char>;
                    var newcharColumn = charColumn.Clone();
                    newcharColumn._columnContainer.LeftShift(value);
                    return newcharColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    throw new NotSupportedException();
                case Type doubleType when doubleType == typeof(double):
                    throw new NotSupportedException();
                case Type floatType when floatType == typeof(float):
                    throw new NotSupportedException();
                case Type intType when intType == typeof(int):
                    PrimitiveColumn<int> intColumn = this as PrimitiveColumn<int>;
                    var newintColumn = intColumn.Clone();
                    newintColumn._columnContainer.LeftShift(value);
                    return newintColumn;
                case Type longType when longType == typeof(long):
                    PrimitiveColumn<long> longColumn = this as PrimitiveColumn<long>;
                    var newlongColumn = longColumn.Clone();
                    newlongColumn._columnContainer.LeftShift(value);
                    return newlongColumn;
                case Type sbyteType when sbyteType == typeof(sbyte):
                    PrimitiveColumn<sbyte> sbyteColumn = this as PrimitiveColumn<sbyte>;
                    var newsbyteColumn = sbyteColumn.Clone();
                    newsbyteColumn._columnContainer.LeftShift(value);
                    return newsbyteColumn;
                case Type shortType when shortType == typeof(short):
                    PrimitiveColumn<short> shortColumn = this as PrimitiveColumn<short>;
                    var newshortColumn = shortColumn.Clone();
                    newshortColumn._columnContainer.LeftShift(value);
                    return newshortColumn;
                case Type uintType when uintType == typeof(uint):
                    PrimitiveColumn<uint> uintColumn = this as PrimitiveColumn<uint>;
                    var newuintColumn = uintColumn.Clone();
                    newuintColumn._columnContainer.LeftShift(value);
                    return newuintColumn;
                case Type ulongType when ulongType == typeof(ulong):
                    PrimitiveColumn<ulong> ulongColumn = this as PrimitiveColumn<ulong>;
                    var newulongColumn = ulongColumn.Clone();
                    newulongColumn._columnContainer.LeftShift(value);
                    return newulongColumn;
                case Type ushortType when ushortType == typeof(ushort):
                    PrimitiveColumn<ushort> ushortColumn = this as PrimitiveColumn<ushort>;
                    var newushortColumn = ushortColumn.Clone();
                    newushortColumn._columnContainer.LeftShift(value);
                    return newushortColumn;
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _RightShift(int value)
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type byteType when byteType == typeof(byte):
                    PrimitiveColumn<byte> byteColumn = this as PrimitiveColumn<byte>;
                    var newbyteColumn = byteColumn.Clone();
                    newbyteColumn._columnContainer.RightShift(value);
                    return newbyteColumn;
                case Type charType when charType == typeof(char):
                    PrimitiveColumn<char> charColumn = this as PrimitiveColumn<char>;
                    var newcharColumn = charColumn.Clone();
                    newcharColumn._columnContainer.RightShift(value);
                    return newcharColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    throw new NotSupportedException();
                case Type doubleType when doubleType == typeof(double):
                    throw new NotSupportedException();
                case Type floatType when floatType == typeof(float):
                    throw new NotSupportedException();
                case Type intType when intType == typeof(int):
                    PrimitiveColumn<int> intColumn = this as PrimitiveColumn<int>;
                    var newintColumn = intColumn.Clone();
                    newintColumn._columnContainer.RightShift(value);
                    return newintColumn;
                case Type longType when longType == typeof(long):
                    PrimitiveColumn<long> longColumn = this as PrimitiveColumn<long>;
                    var newlongColumn = longColumn.Clone();
                    newlongColumn._columnContainer.RightShift(value);
                    return newlongColumn;
                case Type sbyteType when sbyteType == typeof(sbyte):
                    PrimitiveColumn<sbyte> sbyteColumn = this as PrimitiveColumn<sbyte>;
                    var newsbyteColumn = sbyteColumn.Clone();
                    newsbyteColumn._columnContainer.RightShift(value);
                    return newsbyteColumn;
                case Type shortType when shortType == typeof(short):
                    PrimitiveColumn<short> shortColumn = this as PrimitiveColumn<short>;
                    var newshortColumn = shortColumn.Clone();
                    newshortColumn._columnContainer.RightShift(value);
                    return newshortColumn;
                case Type uintType when uintType == typeof(uint):
                    PrimitiveColumn<uint> uintColumn = this as PrimitiveColumn<uint>;
                    var newuintColumn = uintColumn.Clone();
                    newuintColumn._columnContainer.RightShift(value);
                    return newuintColumn;
                case Type ulongType when ulongType == typeof(ulong):
                    PrimitiveColumn<ulong> ulongColumn = this as PrimitiveColumn<ulong>;
                    var newulongColumn = ulongColumn.Clone();
                    newulongColumn._columnContainer.RightShift(value);
                    return newulongColumn;
                case Type ushortType when ushortType == typeof(ushort):
                    PrimitiveColumn<ushort> ushortColumn = this as PrimitiveColumn<ushort>;
                    var newushortColumn = ushortColumn.Clone();
                    newushortColumn._columnContainer.RightShift(value);
                    return newushortColumn;
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Equals<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<bool> retColumn = CloneAsBoolColumn();
                    (this as PrimitiveColumn<U>)._columnContainer.Equals(column._columnContainer, retColumn._columnContainer);
                    return retColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.Equals(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Equals(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.Equals(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Equals((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Equals(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _Equals<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<bool> retColumn = CloneAsBoolColumn();
                    (this as PrimitiveColumn<U>)._columnContainer.Equals(value, retColumn._columnContainer);
                    return retColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.Equals(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.Equals(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.Equals(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.Equals(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.Equals(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _NotEquals<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<bool> retColumn = CloneAsBoolColumn();
                    (this as PrimitiveColumn<U>)._columnContainer.NotEquals(column._columnContainer, retColumn._columnContainer);
                    return retColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.NotEquals(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.NotEquals(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.NotEquals(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.NotEquals((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.NotEquals(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _NotEquals<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    if (typeof(U) != typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    PrimitiveColumn<bool> retColumn = CloneAsBoolColumn();
                    (this as PrimitiveColumn<U>)._columnContainer.NotEquals(value, retColumn._columnContainer);
                    return retColumn;
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.NotEquals(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.NotEquals(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.NotEquals(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.NotEquals(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.NotEquals(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _GreaterThanOrEqual<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThanOrEqual(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.GreaterThanOrEqual(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThanOrEqual(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.GreaterThanOrEqual((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.GreaterThanOrEqual(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _GreaterThanOrEqual<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThanOrEqual(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.GreaterThanOrEqual(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThanOrEqual(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.GreaterThanOrEqual(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.GreaterThanOrEqual(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _LessThanOrEqual<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThanOrEqual(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.LessThanOrEqual(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThanOrEqual(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.LessThanOrEqual((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.LessThanOrEqual(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _LessThanOrEqual<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThanOrEqual(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.LessThanOrEqual(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThanOrEqual(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.LessThanOrEqual(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.LessThanOrEqual(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _GreaterThan<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThan(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.GreaterThan(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThan(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.GreaterThan((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.GreaterThan(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _GreaterThan<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThan(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.GreaterThan(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.GreaterThan(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.GreaterThan(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.GreaterThan(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _LessThan<U>(PrimitiveColumn<U> column)
            where U : struct
        {
            if (column.Length != Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThan(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.LessThan(column.CloneAsDecimalColumn()._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThan(column._columnContainer, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.LessThan((column as PrimitiveColumn<decimal>)._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.LessThan(column.CloneAsDoubleColumn()._columnContainer, newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        internal BaseColumn _LessThan<U>(U value)
            where U : struct
        {
            switch (typeof(T))
            {
                case Type boolType when boolType == typeof(bool):
                    throw new NotSupportedException();
                case Type decimalType when decimalType == typeof(decimal):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThan(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        var decimalColumn = CloneAsDecimalColumn();
                        decimalColumn._columnContainer.LessThan(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                        return newColumn;
                    }
                case Type byteType when byteType == typeof(byte):
                case Type charType when charType == typeof(char):
                case Type doubleType when doubleType == typeof(double):
                case Type floatType when floatType == typeof(float):
                case Type intType when intType == typeof(int):
                case Type longType when longType == typeof(long):
                case Type sbyteType when sbyteType == typeof(sbyte):
                case Type shortType when shortType == typeof(short):
                case Type uintType when uintType == typeof(uint):
                case Type ulongType when ulongType == typeof(ulong):
                case Type ushortType when ushortType == typeof(ushort):
                    if (typeof(U) == typeof(bool))
                    {
                        throw new NotSupportedException();
                    }
                    if (typeof(U) == typeof(T))
                    {
                        // No conversions
                        PrimitiveColumn<U> primitiveColumn = this as PrimitiveColumn<U>;
                        PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                        primitiveColumn._columnContainer.LessThan(value, newColumn._columnContainer);
                        return newColumn;
                    }
                    else 
                    {
                        if (typeof(U) == typeof(decimal))
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var decimalColumn = CloneAsDecimalColumn();
                            decimalColumn._columnContainer.LessThan(DecimalConverter<U>.Instance.GetDecimal(value), newColumn._columnContainer);
                            return newColumn;
                        }
                        else
                        {
                            PrimitiveColumn<bool> newColumn = CloneAsBoolColumn();
                            var doubleColumn = CloneAsDoubleColumn();
                            doubleColumn._columnContainer.LessThan(DoubleConverter<U>.Instance.GetDouble(value), newColumn._columnContainer);
                            return newColumn;
                        }
                    }
                default:
                    throw new NotSupportedException();
            }
        }
        #endregion
    }
}
