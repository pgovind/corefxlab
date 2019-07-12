﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Microsoft.Data
{
    public enum JoinAlgorithm
    {
        Left,
        Right,
        FullOuter,
        Inner
    }

    /// <summary>
    /// A DataFrame to support indexing, binary operations, sorting, selection and other APIs. This will eventually also expose an IDataView for ML.NET
    /// </summary>
    public partial class DataFrame
    {
        private readonly DataFrameTable _table;
        public DataFrame()
        {
            _table = new DataFrameTable();
        }

        public DataFrame(IList<BaseColumn> columns)
        {
            _table = new DataFrameTable(columns);
        }

        public DataFrame(RecordBatch recordBatch)
        {
            _table = new DataFrameTable();
            Apache.Arrow.Schema arrowSchema = recordBatch.Schema;
            int fieldIndex = 0;
            IEnumerable<IArrowArray> arrowArrays = recordBatch.Arrays;
            foreach (IArrowArray arrowArray in arrowArrays)
            {
                Field field = arrowSchema.GetFieldByIndex(fieldIndex);
                IArrowType fieldType = field.DataType;
                BaseColumn dataFrameColumn = null;
                switch (fieldType.TypeId)
                {
                    case ArrowTypeId.Boolean:
                        ReadOnlyMemory<byte> valueBuffer = (arrowArray as BooleanArray).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> nullBitMapBuffer = (arrowArray as BooleanArray).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<bool>(field.Name, valueBuffer, nullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Decimal:
                        ReadOnlyMemory<byte> decimalValueBuffer = (arrowArray as PrimitiveArray<decimal>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> decimalNullBitMapBuffer = (arrowArray as PrimitiveArray<decimal>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<decimal>(field.Name, decimalValueBuffer, decimalNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Double:
                        ReadOnlyMemory<byte> doubleValueBuffer = (arrowArray as PrimitiveArray<double>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> doubleNullBitMapBuffer = (arrowArray as PrimitiveArray<double>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<double>(field.Name, doubleValueBuffer, doubleNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Float:
                        ReadOnlyMemory<byte> floatValueBuffer = (arrowArray as PrimitiveArray<float>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> floatNullBitMapBuffer = (arrowArray as PrimitiveArray<float>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<float>(field.Name, floatValueBuffer, floatNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Int8:
                    case ArrowTypeId.Int16:
                    case ArrowTypeId.Int32:
                        ReadOnlyMemory<byte> intValueBuffer = (arrowArray as PrimitiveArray<int>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> intNullBitMapBuffer = (arrowArray as PrimitiveArray<int>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<int>(field.Name, intValueBuffer, intNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Int64:
                        ReadOnlyMemory<byte> longValueBuffer = (arrowArray as PrimitiveArray<long>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> longNullBitMapBuffer = (arrowArray as PrimitiveArray<long>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<long>(field.Name, longValueBuffer, longNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.UInt8:
                    case ArrowTypeId.UInt16:
                    case ArrowTypeId.UInt32:
                        ReadOnlyMemory<byte> uintValueBuffer = (arrowArray as PrimitiveArray<uint>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> uintNullBitMapBuffer = (arrowArray as PrimitiveArray<uint>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<uint>(field.Name, uintValueBuffer, uintNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.UInt64:
                        ReadOnlyMemory<byte> ulongValueBuffer = (arrowArray as PrimitiveArray<ulong>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> ulongNullBitMapBuffer = (arrowArray as PrimitiveArray<ulong>).NullBitmapBuffer.Memory;
                        dataFrameColumn = new PrimitiveColumn<ulong>(field.Name, ulongValueBuffer, ulongNullBitMapBuffer, arrowArray.Length, arrowArray.NullCount);
                        break;
                    case ArrowTypeId.Binary:
                    case ArrowTypeId.Date32:
                    case ArrowTypeId.Date64:
                    case ArrowTypeId.Dictionary:
                    case ArrowTypeId.FixedSizedBinary:
                    case ArrowTypeId.HalfFloat:
                    case ArrowTypeId.Interval:
                    case ArrowTypeId.List:
                    case ArrowTypeId.Map:
                    case ArrowTypeId.Null:
                    case ArrowTypeId.Struct:
                    case ArrowTypeId.Time32:
                    case ArrowTypeId.Time64:
                        throw new NotImplementedException(nameof(fieldType.Name));
                }
                _table.InsertColumn(ColumnCount, dataFrameColumn);
                fieldIndex++;
            }
        }

        public IEnumerable<RecordBatch> AsArrowRecordBatches()
        {
            Apache.Arrow.Schema.Builder schemaBuilder = new Apache.Arrow.Schema.Builder();

            int recordBatchLength = Int32.MaxValue;
            int columnCount = ColumnCount;
            for (int i = 0; i < columnCount; i++)
            {
                BaseColumn column = Column(i);
                Field field = column.Field;
                schemaBuilder.Field(field);
            }

            Schema schema = schemaBuilder.Build();
            List<Apache.Arrow.Array> arrays = new List<Apache.Arrow.Array>();

            long numberOfRowsProcessed = 0;
            int numberOfRowsInThisRecordBatch = (int)Math.Min(recordBatchLength, RowCount);
            while (numberOfRowsProcessed < RowCount)
            {
                for (int i = 0; i < columnCount; i++)
                {
                    BaseColumn column = Column(i);
                    numberOfRowsInThisRecordBatch = (int)Math.Min(numberOfRowsInThisRecordBatch, column.MaxRecordBatchLength(numberOfRowsProcessed));
                }
                for (int i = 0; i < columnCount; i++)
                {
                    BaseColumn column = Column(i);
                    arrays.Add(column.AsArrowArray(numberOfRowsProcessed, numberOfRowsInThisRecordBatch));
                }
                numberOfRowsProcessed += numberOfRowsInThisRecordBatch;
                yield return new RecordBatch(schema, arrays, numberOfRowsInThisRecordBatch);
            }
        }

        public long RowCount => _table.RowCount;

        public int ColumnCount => _table.ColumnCount;

        public IList<string> Columns
        {
            get
            {
                var ret = new List<string>(ColumnCount);
                for (int i = 0; i < ColumnCount; i++)
                {
                    ret.Add(_table.Column(i).Name);
                }
                return ret;
            }
        }

        public BaseColumn Column(int index) => _table.Column(index);

        public void InsertColumn(int columnIndex, BaseColumn column) => _table.InsertColumn(columnIndex, column);

        public void SetColumn(int columnIndex, BaseColumn column) => _table.SetColumn(columnIndex, column);

        public void RemoveColumn(int columnIndex) => _table.RemoveColumn(columnIndex);

        public void RemoveColumn(string columnName) => _table.RemoveColumn(columnName);

        public object this[long rowIndex, int columnIndex]
        {
            get => _table.Column(columnIndex)[rowIndex];
            set => _table.Column(columnIndex)[rowIndex] = value;
        }

        #region Operators
        public IList<object> this[long rowIndex]
        {
            get
            {
                return _table.GetRow(rowIndex);
            }
            //TODO?: set?
        }

        public BaseColumn this[string columnName]
        {
            get
            {
                int columnIndex = _table.GetColumnIndex(columnName);
                if (columnIndex == -1)
                    throw new ArgumentException($"{columnName} does not exist");
                return _table.Column(columnIndex);
            }
            set
            {
                int columnIndex = _table.GetColumnIndex(columnName);
                BaseColumn newColumn = value;
                newColumn.Name = columnName;
                if (columnIndex == -1)
                {
                    _table.InsertColumn(ColumnCount, newColumn);
                }
                else
                {
                    _table.SetColumn(columnIndex, newColumn);
                }
            }
        }

        public IList<IList<object>> Head(int numberOfRows)
        {
            var ret = new List<IList<object>>();
            for (int i = 0; i < numberOfRows; i++)
            {
                ret.Add(this[i]);
            }
            return ret;
        }

        public IList<IList<object>> Tail(int numberOfRows)
        {
            var ret = new List<IList<object>>();
            for (long i = RowCount - numberOfRows; i < RowCount; i++)
            {
                ret.Add(this[i]);
            }
            return ret;
        }
        // TODO: Add strongly typed versions of these APIs
        #endregion

        private DataFrame Clone(BaseColumn mapIndices = null, bool invertMapIndices = false)
        {
            List<BaseColumn> newColumns = new List<BaseColumn>(ColumnCount);
            for (int i = 0; i < ColumnCount; i++)
            {
                newColumns.Add(Column(i).Clone(mapIndices, invertMapIndices));
            }
            return new DataFrame(newColumns);
        }

        public DataFrame Sort(string columnName, bool ascending = true)
        {
            BaseColumn column = this[columnName];
            BaseColumn sortIndices = column.GetAscendingSortIndices();
            List<BaseColumn> newColumns = new List<BaseColumn>(ColumnCount);
            for (int i = 0; i < ColumnCount; i++)
            {
                BaseColumn oldColumn = Column(i);
                BaseColumn newColumn = oldColumn.Clone(sortIndices, !ascending, oldColumn.NullCount);
                Debug.Assert(newColumn.NullCount == oldColumn.NullCount);
                newColumns.Add(newColumn);
            }
            return new DataFrame(newColumns);
        }

        private void SetSuffixForDuplicatedColumnNames(DataFrame dataFrame, BaseColumn column, string leftSuffix, string rightSuffix)
        {
            int index = dataFrame._table.GetColumnIndex(column.Name);
            while (index != -1)
            {
                // Pre-existing column. Change name
                BaseColumn existingColumn = dataFrame.Column(index);
                dataFrame._table.SetColumnName(existingColumn, existingColumn.Name + leftSuffix);
                column.Name += rightSuffix;
                index = dataFrame._table.GetColumnIndex(column.Name);
            }
        }

        public DataFrame Join(DataFrame other, string leftSuffix = "_left", string rightSuffix = "_right", JoinAlgorithm joinAlgorithm = JoinAlgorithm.Left)
        {
            DataFrame ret = new DataFrame();
            if (joinAlgorithm == JoinAlgorithm.Left)
            {
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn = Column(i).Clone();
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                long minLength = Math.Min(RowCount, other.RowCount);
                PrimitiveColumn<long> mapIndices = new PrimitiveColumn<long>("mapIndices", minLength);
                for (long i = 0; i < minLength; i++)
                {
                    mapIndices[i] = i;
                }
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn;
                    if (other.RowCount < RowCount)
                    {
                        newColumn = other.Column(i).Clone(numberOfNullsToAppend: RowCount - other.RowCount);
                    }
                    else
                    {
                        newColumn = other.Column(i).Clone(mapIndices);
                    }
                    SetSuffixForDuplicatedColumnNames(ret, newColumn, leftSuffix, rightSuffix);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.Right)
            {
                long minLength = Math.Min(RowCount, other.RowCount);
                PrimitiveColumn<long> mapIndices = new PrimitiveColumn<long>("mapIndices", minLength);
                for (long i = 0; i < minLength; i++)
                {
                    mapIndices[i] = i;
                }
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn;
                    if (RowCount < other.RowCount)
                    {
                        newColumn = Column(i).Clone(numberOfNullsToAppend: other.RowCount - RowCount);
                    }
                    else
                    {
                        newColumn = Column(i).Clone(mapIndices);
                    }
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).Clone();
                    SetSuffixForDuplicatedColumnNames(ret, newColumn, leftSuffix, rightSuffix);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.FullOuter)
            {
                long newRowCount = Math.Max(RowCount, other.RowCount);
                long numberOfNulls = newRowCount - RowCount;
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn = Column(i).Clone(numberOfNullsToAppend: numberOfNulls);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                numberOfNulls = newRowCount - other.RowCount;
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).Clone(numberOfNullsToAppend: numberOfNulls);
                    SetSuffixForDuplicatedColumnNames(ret, newColumn, leftSuffix, rightSuffix);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.Inner)
            {
                long newRowCount = Math.Min(RowCount, other.RowCount);
                PrimitiveColumn<long> mapIndices = new PrimitiveColumn<long>("mapIndices", newRowCount);
                for (long i = 0; i < newRowCount; i++)
                {
                    mapIndices[i] = i;
                }
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn = Column(i).Clone(mapIndices);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).Clone(mapIndices);
                    SetSuffixForDuplicatedColumnNames(ret, newColumn, leftSuffix, rightSuffix);
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            return ret;
        }

        public GroupBy GroupBy(string columnName)
        {
            int columnIndex = _table.GetColumnIndex(columnName);
            if (columnIndex == -1)
                throw new ArgumentException($"{columnName} does not exist");

            BaseColumn column = _table.Column(columnIndex);

            switch (column)
            {
                case PrimitiveColumn<bool> boolColumn:
                    Dictionary<bool, ICollection<long>> boolDictionary = boolColumn.HashColumnValues<bool>();
                    return new GroupBy<bool>(this, columnIndex, boolDictionary);
                case PrimitiveColumn<byte> byteColumn:
                    Dictionary<byte, ICollection<long>> byteDictionary = byteColumn.HashColumnValues<byte>();
                    return new GroupBy<byte>(this, columnIndex, byteDictionary);
                case PrimitiveColumn<char> charColumn:
                    Dictionary<char, ICollection<long>> charDictionary = charColumn.HashColumnValues<char>();
                    return new GroupBy<char>(this, columnIndex, charDictionary);
                case PrimitiveColumn<decimal> decimalColumn:
                    Dictionary<decimal, ICollection<long>> decimalDictionary = decimalColumn.HashColumnValues<decimal>();
                    return new GroupBy<decimal>(this, columnIndex, decimalDictionary);
                case PrimitiveColumn<double> doubleColumn:
                    Dictionary<double, ICollection<long>> doubleDictionary = doubleColumn.HashColumnValues<double>();
                    return new GroupBy<double>(this, columnIndex, doubleDictionary);
                case PrimitiveColumn<float> floatColumn:
                    Dictionary<float, ICollection<long>> floatDictionary = floatColumn.HashColumnValues<float>();
                    return new GroupBy<float>(this, columnIndex, floatDictionary);
                case PrimitiveColumn<int> intColumn:
                    Dictionary<int, ICollection<long>> intDictionary = intColumn.HashColumnValues<int>();
                    return new GroupBy<int>(this, columnIndex, intDictionary);
                case PrimitiveColumn<long> longColumn:
                    Dictionary<long, ICollection<long>> longDictionary = longColumn.HashColumnValues<long>();
                    return new GroupBy<long>(this, columnIndex, longDictionary);
                case PrimitiveColumn<sbyte> sbyteColumn:
                    Dictionary<sbyte, ICollection<long>> sbyteDictionary = sbyteColumn.HashColumnValues<sbyte>();
                    return new GroupBy<sbyte>(this, columnIndex, sbyteDictionary);
                case PrimitiveColumn<short> shortColumn:
                    Dictionary<short, ICollection<long>> shortDictionary = shortColumn.HashColumnValues<short>();
                    return new GroupBy<short>(this, columnIndex, shortDictionary);
                case PrimitiveColumn<uint> uintColumn:
                    Dictionary<uint, ICollection<long>> uintDictionary = uintColumn.HashColumnValues<uint>();
                    return new GroupBy<uint>(this, columnIndex, uintDictionary);
                case PrimitiveColumn<ulong> ulongColumn:
                    Dictionary<ulong, ICollection<long>> ulongDictionary = ulongColumn.HashColumnValues<ulong>();
                    return new GroupBy<ulong>(this, columnIndex, ulongDictionary);
                case PrimitiveColumn<ushort> ushortColumn:
                    Dictionary<ushort, ICollection<long>> ushortDictionary = ushortColumn.HashColumnValues<ushort>();
                    return new GroupBy<ushort>(this, columnIndex, ushortDictionary);
                case StringColumn stringColumn:
                    Dictionary<string, ICollection<long>> stringDictionary = stringColumn.HashColumnValues<string>();
                    return new GroupBy<string>(this, columnIndex, stringDictionary);
                default:
                    Dictionary<object, ICollection<long>> dictionary = column.HashColumnValues<object>();
                    return new GroupBy<object>(this, columnIndex, dictionary);
            }
        }

        // In a GroupBy call, columns get resized. We need to set the RowCount to reflect the true Length of the DataFrame. Internal only. Should not be exposed
        internal void SetTableRowCount(long rowCount)
        {
            // Even if current RowCount == rowCount, do the validation
            int numberOfColumns = ColumnCount;
            for (int i = 0; i < numberOfColumns; i++)
            {
                if (Column(i).Length != rowCount)
                    throw new ArgumentException(String.Format("{0} {1}", Strings.MismatchedRowCount, Column(i).Name));
            }
            _table.RowCount = rowCount;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            int longestColumnName = 0;
            for (int i = 0; i < ColumnCount; i++)
            {
                longestColumnName = Math.Max(longestColumnName, Column(i).Name.Length);
            }
            for (int i = 0; i < ColumnCount; i++)
            {
                // Left align by 10
                sb.Append(string.Format(Column(i).Name.PadRight(longestColumnName)));
            }
            sb.AppendLine();
            long numberOfRows = Math.Min(RowCount, 25);
            for (int i = 0; i < numberOfRows; i++)
            {
                IList<object> row = this[i];
                foreach (object obj in row)
                {
                    sb.Append(obj.ToString().PadRight(longestColumnName));
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }
    }
}
