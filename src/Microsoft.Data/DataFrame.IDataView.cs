// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.ML;
using Microsoft.ML.Data;
using System.Dynamic;

namespace Microsoft.Data
{
    public partial class DataFrame : IDataView
    {
        public class RowCursor : DataViewRowCursor
        {
            private long _position = -1;
            private static long _batch = -1;
            private long _rowBatch = ++_batch;
            Func<int, bool> _isColumnActive;
            private DataFrame _dataFrame;
            private IList<object> _row;

            public RowCursor(DataFrame dataFrame, long startRowIndex, Func<int, bool> isColumnActive)
            {
                _dataFrame = dataFrame;
                _row = dataFrame[startRowIndex];
                _isColumnActive = isColumnActive;
            }

            public override long Position => _position;

            public override long Batch => _rowBatch;

            public override DataViewSchema Schema => _dataFrame.Schema;

            public override ValueGetter<TValue> GetGetter<TValue>(DataViewSchema.Column column)
            {
                if (!IsColumnActive(column)) throw new ArgumentOutOfRangeException(nameof(column));
                void valueGetterImplementation(ref TValue value)
                {
                    //switch (typeof(TValue))
                    //{
                    //    case ReadOnlyMemory<char> chars1:
                    //        ReadOnlyMemory<char> chars = _row[column.Index].ToString().AsMemory();
                    //        value = chars1;
                    //        break;

                    //}
                    //if (typeof(TValue) == typeof(ReadOnlyMemory<char>))
                    //{
                    //    ReadOnlyMemory<char> chars = _row[column.Index].ToString().AsMemory();
                    //    value = (TValue)chars;
                    //}
                    //else if (TValue is ReadOnlyMemory<char>)
                    //{

                    //}
                    //else
                    //{
                    //    value = (TValue)_row[column.Index];
                    //}
                    if (value is ReadOnlyMemory<char> chars)
                    {
                        value = (TValue)(object)_row[column.Index].ToString().AsMemory();
                    }
                    else
                    {
                        value = (TValue)_row[column.Index];
                    }
                }
                return valueGetterImplementation;
            }

            public override ValueGetter<DataViewRowId> GetIdGetter()
            {
                // TODO: Not totally sure about this
                void IdGetterImplementation(ref DataViewRowId id)
                {
                    id = new DataViewRowId((ulong)_position, 0);
                }
                return IdGetterImplementation;
            }

            public override bool IsColumnActive(DataViewSchema.Column column)
            {
                return _isColumnActive(column.Index);
            }

            public override bool MoveNext()
            {
                _position++;
                bool canMove = _position < _dataFrame.RowCount;
                if (canMove)
                {
                    _row = _dataFrame[_position];
                }
                return canMove;
            }
        }
        public bool CanShuffle => true;

        private DataViewSchema _schema;
        public DataViewSchema Schema
        {
            get
            {
                if (_schema != null && _schema.Count == ColumnCount)
                {
                    return _schema;
                }
                var schemaBuilder = new DataViewSchema.Builder();
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn baseColumn = Column(i);
                    switch (baseColumn.DataType)
                    {
                        case Type boolType when boolType == typeof(bool):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.BooleanDataViewType.Instance);
                            break;
                        case Type byteType when byteType == typeof(byte):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Byte);
                            break;
                        case Type doubleType when doubleType == typeof(double):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Double);
                            break;
                        case Type floatType when floatType == typeof(float):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Single);
                            break;
                        case Type intType when intType == typeof(int):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Int32);
                            break;
                        case Type longType when longType == typeof(long):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Int64);
                            break;
                        case Type sbyteType when sbyteType == typeof(sbyte):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.SByte);
                            break;
                        case Type shortType when shortType == typeof(short):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.Int16);
                            break;
                        case Type uintType when uintType == typeof(uint):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.UInt32);
                            break;
                        case Type ulongType when ulongType == typeof(ulong):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.UInt64);
                            break;
                        case Type ushortType when ushortType == typeof(ushort):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.NumberDataViewType.UInt16);
                            break;
                        case Type stringType when stringType == typeof(string):
                            schemaBuilder.AddColumn(baseColumn.Name, ML.Data.TextDataViewType.Instance);
                            break;
                        case Type charType when charType == typeof(char):
                        case Type decimalType when decimalType == typeof(decimal):
                        default:
                            int bh = -1;
                            throw new NotSupportedException();

                    }
                }
                _schema = schemaBuilder.ToSchema();
                return _schema;
            }
            set
            {
                _schema = value;
            }
        }

        public long? GetRowCount() => RowCount;

        public DataViewRowCursor GetRowCursor(IEnumerable<DataViewSchema.Column> columnsNeeded, Random rand = null)
        {
            HashSet<int> neededColumns = new HashSet<int>();
            foreach (var col in columnsNeeded)
            {
                neededColumns.Add(col.Index);
            }
            bool needCol(int colIndex) => neededColumns.Contains(colIndex);
            return new RowCursor(this, 0, needCol);
        }

        public DataViewRowCursor[] GetRowCursorSet(IEnumerable<DataViewSchema.Column> columnsNeeded, int n, Random rand = null)
        {
            // TODO: Simple change to support parallel cursors
            return new DataViewRowCursor[] { GetRowCursor(columnsNeeded, rand) };
            throw new NotImplementedException();
        }

        public class Row : DynamicObject
        {
            private DataFrame _dataFrame;
            private long _rowIndex;
            private IList<object> _row;
            public Row(DataFrame dataFrame, long rowIndex)
            {
                _dataFrame = dataFrame;
                _rowIndex = rowIndex;
                _row = dataFrame[rowIndex];
            }
            public Row()
            { }
            public object GetPropertyValue(string propertyName)
            {
                int columnIndex = _dataFrame._table.GetColumnIndex(propertyName);
                return _row[columnIndex];
            }

            public override bool TryGetMember(GetMemberBinder binder, out object result)
            {
                result = GetPropertyValue(binder.Name);
                return result == null ? false : true;
            }

        }
        public static DataFrame FromIDataView(IDataView dataView)
        {
            // HACK
            var ret = new DataFrame();
            int cc = 0;
            int numberOfRows = (int)Math.Min((long)dataView.GetRowCount(), (long)int.MaxValue);
            var preview = dataView.Preview(numberOfRows);
            var temp = preview.ColumnView;
            foreach (var columnView in temp)
            {
                var values = columnView.Values;
                var dvColumn = columnView.Column;
                if (dvColumn.IsHidden)
                    continue;//columnEnumerator.MoveNext();
                else
                {
                    //neededColumns.Add(dvColumn);
                    string columnName = dvColumn.Name;
                    Type columnType = default;
                    long rowCount = numberOfRows;

                    switch (dvColumn.Type)
                    {
                        case NumberDataViewType number:
                            columnType = typeof(float);
                            break;
                        case TextDataViewType text:
                            columnType = typeof(string);
                            break;
                        case BooleanDataViewType boolType:
                            columnType = typeof(bool);
                            break;
                        case VectorDataViewType vector:
                            continue;
                            switch (vector.ItemType)
                            {
                                case NumberDataViewType number:
                                    columnType = typeof(float);
                                    break;
                                case TextDataViewType text:
                                    columnType = typeof(string);
                                    break;
                                case BooleanDataViewType boolType:
                                    columnType = typeof(bool);
                                    break;
                            }
                            break;
                        default:
                            throw new NotSupportedException(nameof(dvColumn.Type));
                    }
                    BaseColumn column;
                    switch (columnType)
                    {
                        case Type floatType when floatType == typeof(float):
                            column = new PrimitiveColumn<float>(columnName, rowCount);
                            break;
                        case Type boolType when boolType == typeof(bool):
                            column = new PrimitiveColumn<bool>(columnName, rowCount);
                            break;
                        case Type stringType when stringType == typeof(string):
                            column = new StringColumn(columnName, rowCount);
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                    for (int j = 0; j < values.Length; j++)
                    {
                        column[j] = values[j];
                    }
                    ret._table.InsertColumn(cc++, column);
                }
            
                int bh = -1;
            }

            return ret;
            DataViewSchema dvSchema = dataView.Schema;
            var columnEnumerator = dvSchema.GetEnumerator();
            List<DataViewSchema.Column> neededColumns = new List<DataViewSchema.Column>();
            while (columnEnumerator.MoveNext())
            {
                var dvColumn = columnEnumerator.Current;
                if (dvColumn.IsHidden)
                    columnEnumerator.MoveNext();
                else
                {
                    neededColumns.Add(dvColumn);
                    string columnName = dvColumn.Name;
                    Type columnType = default;
                    long rowCount = dataView.GetRowCount() ?? 0;
                    
                    switch (dvColumn.Type)
                    {
                        case NumberDataViewType number:
                            columnType = typeof(float);
                            break;
                        case TextDataViewType text:
                            columnType = typeof(string);
                            break;
                        case BooleanDataViewType boolType:
                            columnType = typeof(bool);
                            break;
                        case VectorDataViewType vector:
                            switch (vector.ItemType)
                            {
                                case NumberDataViewType number:
                                    columnType = typeof(float);
                                    break;
                                case TextDataViewType text:
                                    columnType = typeof(string);
                                    break;
                                case BooleanDataViewType boolType:
                                    columnType = typeof(bool);
                                    break;
                            }
                            break;
                        default:
                            throw new NotSupportedException(nameof(dvColumn.Type));
                    }
                    BaseColumn column;
                    switch (columnType)
                    {
                        case Type floatType when floatType == typeof(float):
                            column = new PrimitiveColumn<float>(columnName, rowCount);
                            break;
                        case Type boolType when boolType == typeof(bool):
                            column = new PrimitiveColumn<bool>(columnName, rowCount);
                            break;
                        case Type stringType when stringType == typeof(string):
                            column = new StringColumn(columnName, rowCount);
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                    ret._table.InsertColumn(cc++, column);
                }
            }

            long rowIndex = 0;
            columnEnumerator.Reset();
            var rowCursor = dataView.GetRowCursor(neededColumns);
            while (rowCursor.MoveNext())
            {
                while (columnEnumerator.MoveNext())
                {
                    var dvColumn = columnEnumerator.Current;
                    if (dvColumn.IsHidden)
                        columnEnumerator.MoveNext();
                    else
                    {
                        try
                        {
                            switch (columnEnumerator.Current.Type)
                            {
                                //case Type floatType when floatType == typeof(float):
                                case NumberDataViewType number:
                                    float value = 0;
                                    rowCursor.GetGetter<float>(columnEnumerator.Current)(ref value);
                                    ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = value;
                                    break;
                                //case Type stringType when stringType == typeof(string):
                                case TextDataViewType text:
                                    ReadOnlyMemory<char> textValue = new ReadOnlyMemory<char>();
                                    rowCursor.GetGetter<ReadOnlyMemory<char>>(columnEnumerator.Current)(ref textValue);
                                    ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = textValue;
                                    break;
                                //case Type boolType when boolType == typeof(bool):
                                case BooleanDataViewType boolType:
                                    bool boolValue = false;
                                    rowCursor.GetGetter<bool>(columnEnumerator.Current)(ref boolValue);
                                    ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = boolValue;
                                    break;
                                case VectorDataViewType vector:
                                    switch (vector.ItemType)
                                    {
                                        case NumberDataViewType number:
                                            float fvalue = 0;
                                            rowCursor.GetGetter<float>(columnEnumerator.Current)(ref fvalue);
                                            ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = fvalue;
                                            break;
                                        case TextDataViewType text:
                                            ReadOnlyMemory<char> ttextValue = new ReadOnlyMemory<char>();
                                            rowCursor.GetGetter<ReadOnlyMemory<char>>(columnEnumerator.Current)(ref ttextValue);
                                            ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = ttextValue;
                                            break;
                                        case BooleanDataViewType boolType:
                                            bool bboolValue = false;
                                            rowCursor.GetGetter<bool>(columnEnumerator.Current)(ref bboolValue);
                                            ret._table.Column(ret._table.GetColumnIndex(columnEnumerator.Current.Name))[rowIndex++] = bboolValue;
                                            break;
                                    }
                                    break;
                                default:
                                    throw new NotSupportedException(nameof(dvColumn.Type));
                            }

                        }
                        catch (Exception)
                        {

                            int bh = -1;
                        }
                    }
                }
                columnEnumerator.Reset();
            }
            return ret;
        }

        public DataFrame Predict(MLContext mlContext, ITransformer model, string columnToPredict)
        {
            var testIDV = model.Transform(this);
            return DataFrame.FromIDataView(testIDV);
        }
    }
}
