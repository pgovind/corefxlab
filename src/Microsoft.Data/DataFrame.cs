// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Collections.Extensions;

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
                Memory<byte> dataFrameMemory, nullBitMapMemory;
                bool copied;
                switch (fieldType.TypeId)
                {
                    case ArrowTypeId.Boolean:
                        ReadOnlyMemory<byte> valueBuffer = (arrowArray as BooleanArray).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> nullBitMapBuffer = (arrowArray as BooleanArray).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[valueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[nullBitMapBuffer.Length]);
                        copied = valueBuffer.TryCopyTo(dataFrameMemory) && nullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<bool>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Decimal:
                        ReadOnlyMemory<byte> decimalValueBuffer = (arrowArray as PrimitiveArray<decimal>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> decimalNullBitMapBuffer = (arrowArray as PrimitiveArray<decimal>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[decimalValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[decimalNullBitMapBuffer.Length]);
                        copied = decimalValueBuffer.TryCopyTo(dataFrameMemory) && decimalNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<decimal>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Double:
                        ReadOnlyMemory<byte> doubleValueBuffer = (arrowArray as PrimitiveArray<double>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> doubleNullBitMapBuffer = (arrowArray as PrimitiveArray<double>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[doubleValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[doubleNullBitMapBuffer.Length]);
                        copied = doubleValueBuffer.TryCopyTo(dataFrameMemory) && doubleNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<double>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Float:
                        ReadOnlyMemory<byte> floatValueBuffer = (arrowArray as PrimitiveArray<float>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> floatNullBitMapBuffer = (arrowArray as PrimitiveArray<float>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[floatValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[floatNullBitMapBuffer.Length]);
                        copied = floatValueBuffer.TryCopyTo(dataFrameMemory) && floatNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<float>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Int8:
                    case ArrowTypeId.Int16:
                    case ArrowTypeId.Int32:
                        ReadOnlyMemory<byte> intValueBuffer = (arrowArray as PrimitiveArray<int>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> intNullBitMapBuffer = (arrowArray as PrimitiveArray<int>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[intValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[intNullBitMapBuffer.Length]);
                        copied = intValueBuffer.TryCopyTo(dataFrameMemory) && intNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<int>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Int64:
                        ReadOnlyMemory<byte> longValueBuffer = (arrowArray as PrimitiveArray<long>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> longNullBitMapBuffer = (arrowArray as PrimitiveArray<long>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[longValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[longNullBitMapBuffer.Length]);
                        copied = longValueBuffer.TryCopyTo(dataFrameMemory) && longNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<long>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.UInt8:
                    case ArrowTypeId.UInt16:
                    case ArrowTypeId.UInt32:
                        ReadOnlyMemory<byte> uintValueBuffer = (arrowArray as PrimitiveArray<uint>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> uintNullBitMapBuffer = (arrowArray as PrimitiveArray<uint>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[uintValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[uintNullBitMapBuffer.Length]);
                        copied = uintValueBuffer.TryCopyTo(dataFrameMemory) && uintNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<uint>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.UInt64:
                        ReadOnlyMemory<byte> ulongValueBuffer = (arrowArray as PrimitiveArray<ulong>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> ulongNullBitMapBuffer = (arrowArray as PrimitiveArray<ulong>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[ulongValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[ulongNullBitMapBuffer.Length]);
                        copied = ulongValueBuffer.TryCopyTo(dataFrameMemory) && ulongNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<ulong>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
                    case ArrowTypeId.Binary:
                        ReadOnlyMemory<byte> byteValueBuffer = (arrowArray as PrimitiveArray<byte>).ValueBuffer.Memory;
                        ReadOnlyMemory<byte> byteNullBitMapBuffer = (arrowArray as PrimitiveArray<byte>).NullBitmapBuffer.Memory;
                        dataFrameMemory = new Memory<byte>(new byte[byteValueBuffer.Length]);
                        nullBitMapMemory = new Memory<byte>(new byte[byteNullBitMapBuffer.Length]);
                        copied = byteValueBuffer.TryCopyTo(dataFrameMemory) && byteNullBitMapBuffer.TryCopyTo(nullBitMapMemory);
                        if (copied)
                        {
                            dataFrameColumn = new PrimitiveColumn<byte>(field.Name, dataFrameMemory, nullBitMapMemory, arrowArray.Length);
                        }
                        break;
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
                    case ArrowTypeId.Timestamp:
                    case ArrowTypeId.Union:
                    default:
                        throw new NotImplementedException(nameof(fieldType.Name));
                }
                _table.InsertColumn(ColumnCount, dataFrameColumn);
                fieldIndex++;
            }
        }

        public IEnumerable<RecordBatch> AsArrowRecordBatch()
        {
            Apache.Arrow.Schema.Builder schemaBuilder = new Apache.Arrow.Schema.Builder();
            Field.Builder fieldBuilder = new Field.Builder();
            // TODO: Sanity check. Some column types are not Arrow Compatible
            // Derived column types will NOT be supported

            // Find the number of DataFrameBuffers in each column. All columns have the same number of buffers
            int numberOfColumns = ColumnCount;
            // Assuming that all columns are Arrow compatible 
            int numberOfRecordBatches = 0;
            for (int i = 0; i < numberOfColumns; i++)
            {
                BaseColumn column = Column(i);
                switch (column)
                {
                    case PrimitiveColumn<bool> boolColumn:
                        numberOfRecordBatches = boolColumn.NumberOfBuffers;
                        Field boolField = fieldBuilder.Name(boolColumn.Name).Nullable(true).DataType(BooleanType.Default).Build();
                        schemaBuilder.Field(boolField);
                        break;
                    case PrimitiveColumn<byte> byteColumn:
                    case PrimitiveColumn<char> charColumn:
                    case PrimitiveColumn<decimal> decimalColumn:
                    case PrimitiveColumn<sbyte> sbyteColumn:
                        throw new NotImplementedException(nameof(byteColumn.DataType));
                    case PrimitiveColumn<double> doubleColumn:
                        numberOfRecordBatches = doubleColumn.NumberOfBuffers;
                        Field doubleField = fieldBuilder.Name(doubleColumn.Name).Nullable(true).DataType(DoubleType.Default).Build();
                        schemaBuilder.Field(doubleField);
                        break;
                    case PrimitiveColumn<float> floatColumn:
                        numberOfRecordBatches = floatColumn.NumberOfBuffers;
                        Field floatField = fieldBuilder.Name(floatColumn.Name).Nullable(true).DataType(FloatType.Default).Build();
                        schemaBuilder.Field(floatField);
                        break;
                    case PrimitiveColumn<int> intColumn:
                        numberOfRecordBatches = intColumn.NumberOfBuffers;
                        Field intField = fieldBuilder.Name(intColumn.Name).Nullable(true).DataType(Int32Type.Default).Build();
                        schemaBuilder.Field(intField);
                        break;
                    case PrimitiveColumn<long> longColumn:
                        numberOfRecordBatches = longColumn.NumberOfBuffers;
                        Field longField = fieldBuilder.Name(longColumn.Name).Nullable(true).DataType(Int64Type.Default).Build();
                        schemaBuilder.Field(longField);
                        break;
                    case PrimitiveColumn<short> shortColumn:
                        numberOfRecordBatches = shortColumn.NumberOfBuffers;
                        Field shortField = fieldBuilder.Name(shortColumn.Name).Nullable(true).DataType(Int16Type.Default).Build();
                        schemaBuilder.Field(shortField);
                        break;
                    case PrimitiveColumn<uint> uintColumn:
                        numberOfRecordBatches = uintColumn.NumberOfBuffers;
                        Field uintField = fieldBuilder.Name(uintColumn.Name).Nullable(true).DataType(UInt32Type.Default).Build();
                        schemaBuilder.Field(uintField);
                        break;
                    case PrimitiveColumn<ulong> ulongColumn:
                        numberOfRecordBatches = ulongColumn.NumberOfBuffers;
                        Field ulongField = fieldBuilder.Name(ulongColumn.Name).Nullable(true).DataType(UInt64Type.Default).Build();
                        schemaBuilder.Field(ulongField);
                        break;
                    case PrimitiveColumn<ushort> ushortColumn:
                        numberOfRecordBatches = ushortColumn.NumberOfBuffers;
                        Field ushortField = fieldBuilder.Name(ushortColumn.Name).Nullable(true).DataType(UInt16Type.Default).Build();
                        schemaBuilder.Field(ushortField);
                        break;
                }
            }
            Schema schema = schemaBuilder.Build();
            List<Apache.Arrow.Array> arrays = new List<Apache.Arrow.Array>();

            for (int n = 0; n < numberOfRecordBatches; n++)
            {
                for (int i = 0; i < numberOfColumns; i++)
                {
                    BaseColumn column = Column(i);
                    //Memory<byte> values, nulls;
                    //ArrowBuffer valueBuffer, nullBuffer;
                    switch (column)
                    {
                        case PrimitiveColumn<bool> boolColumn:
                            arrays.Add(boolColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<byte> byteColumn:
                            arrays.Add(byteColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<char> charColumn:
                            arrays.Add(charColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<decimal> decimalColumn:
                            arrays.Add(decimalColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<double> doubleColumn:
                            arrays.Add(doubleColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<float> floatColumn:
                            arrays.Add(floatColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<int> intColumn:
                            arrays.Add(intColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<long> longColumn:
                            arrays.Add(longColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<sbyte> sbyteColumn:
                            arrays.Add(sbyteColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<short> shortColumn:
                            arrays.Add(shortColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<uint> uintColumn:
                            arrays.Add(uintColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<ulong> ulongColumn:
                            arrays.Add(ulongColumn.AsArrowArray(n));
                            break;
                        case PrimitiveColumn<ushort> ushortColumn:
                            arrays.Add(ushortColumn.AsArrowArray(n));
                            break;
                        default:
                            throw new NotImplementedException(nameof(column.DataType));
                    }
                }
                yield return new RecordBatch(schema, arrays, 0);
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

        private void AppendForMerge(DataFrame dataFrame, long dataFrameRow, DataFrame left, DataFrame right, long leftRow, long rightRow)
        {
            for (int i = 0; i < left.ColumnCount; i++)
            {
                BaseColumn leftColumn = left.Column(i);
                BaseColumn column = dataFrame.Column(i);
                if (leftRow == -1)
                {
                    column[dataFrameRow] = null;
                }
                else
                {
                    column[dataFrameRow] = leftColumn[leftRow];
                }
            }
            for (int i = 0; i < right.ColumnCount; i++)
            {
                BaseColumn rightColumn = right.Column(i);
                BaseColumn column = dataFrame.Column(i + left.ColumnCount);
                if (rightRow == -1)
                {
                    column[dataFrameRow] = null;
                }
                else
                {
                    column[dataFrameRow] = rightColumn[rightRow];
                }
            }
        }

        // TODO: Merge API with an "On" parameter that merges on a column common to 2 dataframes

        /// <summary>
        /// Merge DataFrames with a database style join
        /// </summary>
        /// <param name="other"></param>
        /// <param name="leftJoinColumn"></param>
        /// <param name="rightJoinColumn"></param>
        /// <param name="leftSuffix"></param>
        /// <param name="rightSuffix"></param>
        /// <param name="joinAlgorithm"></param>
        /// <returns></returns>
        public DataFrame Merge<TKey>(DataFrame other, string leftJoinColumn, string rightJoinColumn, string leftSuffix = "_left", string rightSuffix = "_right", JoinAlgorithm joinAlgorithm = JoinAlgorithm.Left)
        {
            // A simple hash join
            DataFrame ret = new DataFrame();
            PrimitiveColumn<long> emptyMap = new PrimitiveColumn<long>("Empty");
            for (int i = 0; i < ColumnCount; i++)
            {
                // Create empty columns
                BaseColumn column = Column(i).Clone(emptyMap);
                ret.InsertColumn(ret.ColumnCount, column);
            }

            for (int i = 0; i < other.ColumnCount; i++)
            {
                // Create empty columns
                BaseColumn column = other.Column(i).Clone(emptyMap);
                SetSuffixForDuplicatedColumnNames(ret, column, leftSuffix, rightSuffix);
                ret.InsertColumn(ret.ColumnCount, column);
            }

            // The final table size is not known until runtime
            long rowNumber = 0;
            if (joinAlgorithm == JoinAlgorithm.Left)
            {
                // First hash other dataframe on the rightJoinColumn
                BaseColumn otherColumn = other[rightJoinColumn];
                MultiValueDictionary<TKey, long> multimap = otherColumn.HashColumnValues<TKey>();

                // Go over the records in this dataframe and match with the hashtable
                BaseColumn thisColumn = this[leftJoinColumn];
                for (int c = 0; c < ret.ColumnCount; c++)
                {
                    ret.Column(c).Resize(thisColumn.Length);
                }

                for (long i = 0; i < thisColumn.Length; i++)
                {
                    if (rowNumber >= thisColumn.Length)
                    {
                        for (int c = 0; c < ret.ColumnCount; c++)
                        {
                            ret.Column(c).Resize(rowNumber + 1);
                        }
                    }
                    TKey value = (TKey)(thisColumn[i] ?? default(TKey));
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            if (thisColumn[i] == null)
                            {
                                // Match only with nulls in otherColumn
                                if (otherColumn[row] == null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                }
                            }
                            else
                            {
                                // Cannot match nulls in otherColumn
                                if (otherColumn[row] != null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                }
                            }
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, rowNumber++, this, other, i, -1);
                    }
                }
                ret._table.RowCount = rowNumber;
            }
            else if (joinAlgorithm == JoinAlgorithm.Right)
            {
                BaseColumn thisColumn = this[leftJoinColumn];
                MultiValueDictionary<TKey, long> multimap = thisColumn.HashColumnValues<TKey>();

                BaseColumn otherColumn = other[rightJoinColumn];
                for (int c = 0; c < ret.ColumnCount; c++)
                {
                    ret.Column(c).Resize(otherColumn.Length);
                }

                for (long i = 0; i < otherColumn.Length; i++)
                {
                    if (rowNumber >= otherColumn.Length)
                    {
                        for (int c = 0; c < ret.ColumnCount; c++)
                        {
                            ret.Column(c).Resize(rowNumber + 1);
                        }
                    }
                    TKey value = (TKey)(otherColumn[i] ?? default(TKey));
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            if (otherColumn[i] == null)
                            {
                                if (thisColumn[row] == null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, row, i);
                                }
                            }
                            else
                            {
                                if (thisColumn[row] != null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, row, i);
                                }
                            }
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, rowNumber++, this, other, -1, i);
                    }
                }
                ret._table.RowCount = rowNumber;
            }
            else if (joinAlgorithm == JoinAlgorithm.Inner)
            {
                // Hash the column with the smaller RowCount
                long leftRowCount = RowCount;
                long rightRowCount = other.RowCount;
                DataFrame longerDataFrame = leftRowCount < rightRowCount ? other : this;
                DataFrame shorterDataFrame = ReferenceEquals(longerDataFrame, this) ? other : this;
                BaseColumn hashColumn = (leftRowCount < rightRowCount) ? this[leftJoinColumn] : other[rightJoinColumn];
                BaseColumn otherColumn = ReferenceEquals(hashColumn, this[leftJoinColumn]) ? other[rightJoinColumn] : this[leftJoinColumn];
                MultiValueDictionary<TKey, long> multimap = hashColumn.HashColumnValues<TKey>();

                for (int c = 0; c < ret.ColumnCount; c++)
                {
                    ret.Column(c).Resize(1);
                }

                for (long i = 0; i < otherColumn.Length; i++)
                {
                    if (rowNumber >= ret.Column(0).Length)
                    {
                        for (int c = 0; c < ret.ColumnCount; c++)
                        {
                            ret.Column(c).Resize(rowNumber + 1);
                        }
                    }
                    TKey value = (TKey)(otherColumn[i] ?? default(TKey));
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            if (otherColumn[i] == null)
                            {
                                if (hashColumn[row] == null)
                                {
                                    AppendForMerge(ret, rowNumber++, shorterDataFrame, longerDataFrame, row, i);
                                }
                            }
                            else
                            {
                                if (hashColumn[row] != null)
                                {
                                    AppendForMerge(ret, rowNumber++, shorterDataFrame, longerDataFrame, row, i);
                                }
                            }
                        }
                    }
                }
                ret._table.RowCount = rowNumber;
            }
            else if (joinAlgorithm == JoinAlgorithm.FullOuter)
            {
                BaseColumn otherColumn = other[rightJoinColumn];
                MultiValueDictionary<TKey, long> multimap = otherColumn.HashColumnValues<TKey>();
                MultiValueDictionary<TKey, long> intersection = new MultiValueDictionary<TKey, long>(EqualityComparer<TKey>.Default);

                // Go over the records in this dataframe and match with the hashtable
                BaseColumn thisColumn = this[rightJoinColumn];
                for (int c = 0; c < ret.ColumnCount; c++)
                {
                    ret.Column(c).Resize(thisColumn.Length + 1);
                }

                for (long i = 0; i < thisColumn.Length; i++)
                {
                    if (rowNumber >= thisColumn.Length)
                    {
                        for (int c = 0; c < ret.ColumnCount; c++)
                        {
                            ret.Column(c).Resize(rowNumber + 1);
                        }
                    }
                    TKey value = (TKey)(thisColumn[i] ?? default(TKey));
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            if (thisColumn[i] == null)
                            {
                                // Has to match only with nulls in otherColumn
                                if (otherColumn[row] == null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                    intersection.Add(value, rowNumber);
                                }
                            }
                            else
                            {
                                // Cannot match to nulls in otherColumn
                                if (otherColumn[row] != null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                    intersection.Add(value, rowNumber);
                                }
                            }
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, rowNumber++, this, other, i, -1);
                    }
                }
                for (long i = 0; i < otherColumn.Length; i++)
                {
                    if (rowNumber >= ret.Column(0).Length)
                    {
                        for (int c = 0; c < ret.ColumnCount; c++)
                        {
                            ret.Column(c).Resize(rowNumber + 1);
                        }
                    }
                    TKey value = (TKey)(otherColumn[i] ?? default(TKey));
                    if (!intersection.ContainsKey(value))
                    {
                        if (rowNumber >= otherColumn.Length)
                        {
                            for (int c = 0; c < ret.ColumnCount; c++)
                            {
                                ret.Column(c).Resize(rowNumber + 1);
                            }
                        }
                        AppendForMerge(ret, rowNumber++, this, other, -1, i);
                    }
                }
                ret._table.RowCount = rowNumber;
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
                    MultiValueDictionary<bool, long> boolDictionary = boolColumn.HashColumnValues<bool>();
                    return new GroupBy<bool>(this, columnIndex, boolDictionary);
                case PrimitiveColumn<byte> byteColumn:
                    MultiValueDictionary<byte, long> byteDictionary = byteColumn.HashColumnValues<byte>();
                    return new GroupBy<byte>(this, columnIndex, byteDictionary);
                case PrimitiveColumn<char> charColumn:
                    MultiValueDictionary<char, long> charDictionary = charColumn.HashColumnValues<char>();
                    return new GroupBy<char>(this, columnIndex, charDictionary);
                case PrimitiveColumn<decimal> decimalColumn:
                    MultiValueDictionary<decimal, long> decimalDictionary = decimalColumn.HashColumnValues<decimal>();
                    return new GroupBy<decimal>(this, columnIndex, decimalDictionary);
                case PrimitiveColumn<double> doubleColumn:
                    MultiValueDictionary<double, long> doubleDictionary = doubleColumn.HashColumnValues<double>();
                    return new GroupBy<double>(this, columnIndex, doubleDictionary);
                case PrimitiveColumn<float> floatColumn:
                    MultiValueDictionary<float, long> floatDictionary = floatColumn.HashColumnValues<float>();
                    return new GroupBy<float>(this, columnIndex, floatDictionary);
                case PrimitiveColumn<int> intColumn:
                    MultiValueDictionary<int, long> intDictionary = intColumn.HashColumnValues<int>();
                    return new GroupBy<int>(this, columnIndex, intDictionary);
                case PrimitiveColumn<long> longColumn:
                    MultiValueDictionary<long, long> longDictionary = longColumn.HashColumnValues<long>();
                    return new GroupBy<long>(this, columnIndex, longDictionary);
                case PrimitiveColumn<sbyte> sbyteColumn:
                    MultiValueDictionary<sbyte, long> sbyteDictionary = sbyteColumn.HashColumnValues<sbyte>();
                    return new GroupBy<sbyte>(this, columnIndex, sbyteDictionary);
                case PrimitiveColumn<short> shortColumn:
                    MultiValueDictionary<short, long> shortDictionary = shortColumn.HashColumnValues<short>();
                    return new GroupBy<short>(this, columnIndex, shortDictionary);
                case PrimitiveColumn<uint> uintColumn:
                    MultiValueDictionary<uint, long> uintDictionary = uintColumn.HashColumnValues<uint>();
                    return new GroupBy<uint>(this, columnIndex, uintDictionary);
                case PrimitiveColumn<ulong> ulongColumn:
                    MultiValueDictionary<ulong, long> ulongDictionary = ulongColumn.HashColumnValues<ulong>();
                    return new GroupBy<ulong>(this, columnIndex, ulongDictionary);
                case PrimitiveColumn<ushort> ushortColumn:
                    MultiValueDictionary<ushort, long> ushortDictionary = ushortColumn.HashColumnValues<ushort>();
                    return new GroupBy<ushort>(this, columnIndex, ushortDictionary);
                case StringColumn stringColumn:
                    MultiValueDictionary<string, long> stringDictionary = stringColumn.HashColumnValues<string>();
                    return new GroupBy<string>(this, columnIndex, stringDictionary);
                default:
                    MultiValueDictionary<object, long> dictionary = column.HashColumnValues<object>();
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
