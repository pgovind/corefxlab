// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.ML;
using Microsoft.ML.Data;

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
                    value = (TValue)_row[column.Index];
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
                        case Type charType when charType == typeof(char):
                        case Type decimalType when decimalType == typeof(decimal):
                        default:
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
    }
}
