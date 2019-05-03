// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.ML;

namespace Microsoft.Data
{
    /// <summary>
    /// A DataFrame to support indexing, binary operations, sorting, selection and other APIs. This will eventually also expose an IDataView for ML.NET
    /// </summary>
    public partial class DataFrame : IDataView, IDataFrame
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
                if (columnIndex == -1) throw new ArgumentException($"{columnName} does not exist");
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

        //public string Head(int numberOfRows)
        public IList<IList<object>> Head(int numberOfRows)
        {
            //return PrettyPrint(0, numberOfRows);
            var ret = new List<IList<object>>();
            for (int i = 0; i < numberOfRows; i++)
            {
                ret.Add(this[i]);
            }
            return ret;
        }

        //public string Tail(int numberOfRows)
        public IList<IList<object>> Tail(int numberOfRows)
        {
            //return PrettyPrint(RowCount - numberOfRows, numberOfRows);
            var ret = new List<IList<object>>();
            for (long i = RowCount - numberOfRows; i < RowCount; i++)
            {
                ret.Add(this[i]);
            }
            return ret;
        }
        // TODO: Add strongly typed versions of these APIs
        #endregion

        private DataFrame Clone()
        {
            List<BaseColumn> newColumns = new List<BaseColumn>(ColumnCount);
            for (int i = 0; i < ColumnCount; i++)
            {
                newColumns.Add(Column(i).Clone());
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
                var newColumn = Column(i).Clone(sortIndices, !ascending);
                newColumns.Add(newColumn);
            }
            return new DataFrame(newColumns);
        }

        public DataFrame Description()
        {
            var ret = new DataFrame();
            for (int i = 0; i < ColumnCount; i++)
            {
                var column = Column(i);
                if (column.DataType == typeof(string) || column.DataType == typeof(bool))
                {
                    continue;
                }
                Dictionary<string, float> columnDescription = column.Description();
                PrimitiveColumn<float> newFloatColumn = new PrimitiveColumn<float>(column.Name, columnDescription.Count + 1);
                ret.InsertColumn(ret.ColumnCount, newFloatColumn);
                newFloatColumn[0] = (float)column.Length;
                columnDescription.TryGetValue("Max", out float max);
                columnDescription.TryGetValue("Min", out float min);
                columnDescription.TryGetValue("Mean", out float mean);
                newFloatColumn[1] = max;
                newFloatColumn[2] = min;
                newFloatColumn[3] = mean;
            }
            return ret;
        }

        public DataFrame FilterRows<T>(string columnName, T lowerBound, T upperBound)
            where T : unmanaged
        {
            var ret = new DataFrame();
            var column = this[columnName];
            PrimitiveColumn<bool> filter = column.Filter(lowerBound, upperBound) as PrimitiveColumn<bool>;
            for (int i = 0; i < ColumnCount; i++)
            {

                column = Column(i);
                //if (column.DataType == typeof(string) || column.DataType == typeof(bool))
                //{
                //    continue;
                //}
                BaseColumn newColumn;
                switch (column.DataType)
                {
                    case Type boolType when boolType == typeof(bool):
                        PrimitiveColumn<bool> boolColumn = column as PrimitiveColumn<bool>;
                        newColumn = boolColumn.ApplyFilter(filter);
                        break;
                    case Type byteType when byteType == typeof(byte):
                        PrimitiveColumn<byte> byteColumn = column as PrimitiveColumn<byte>;
                        newColumn = byteColumn.ApplyFilter(filter);
                        break;
                    case Type charType when charType == typeof(char):
                        PrimitiveColumn<char> charColumn = column as PrimitiveColumn<char>;
                        newColumn = charColumn.ApplyFilter(filter);
                        break;
                    case Type decimalType when decimalType == typeof(decimal):
                        PrimitiveColumn<decimal> decimalColumn = column as PrimitiveColumn<decimal>;
                        newColumn = decimalColumn.ApplyFilter(filter);
                        break;
                    case Type doubleType when doubleType == typeof(double):
                        PrimitiveColumn<double> doubleColumn = column as PrimitiveColumn<double>;
                        newColumn = doubleColumn.ApplyFilter(filter);
                        break;
                    case Type floatType when floatType == typeof(float):
                        PrimitiveColumn<float> floatColumn = column as PrimitiveColumn<float>;
                        newColumn = floatColumn.ApplyFilter(filter);
                        break;
                    case Type intType when intType == typeof(int):
                        PrimitiveColumn<int> intColumn = column as PrimitiveColumn<int>;
                        newColumn = intColumn.ApplyFilter(filter);
                        break;
                    case Type longType when longType == typeof(long):
                        PrimitiveColumn<long> longColumn = column as PrimitiveColumn<long>;
                        newColumn = longColumn.ApplyFilter(filter);
                        break;
                    case Type sbyteType when sbyteType == typeof(sbyte):
                        PrimitiveColumn<sbyte> sbyteColumn = column as PrimitiveColumn<sbyte>;
                        newColumn = sbyteColumn.ApplyFilter(filter);
                        break;
                    case Type shortType when shortType == typeof(short):
                        PrimitiveColumn<short> shortColumn = column as PrimitiveColumn<short>;
                        newColumn = shortColumn.ApplyFilter(filter);
                        break;
                    case Type uintType when uintType == typeof(uint):
                        PrimitiveColumn<uint> uintColumn = column as PrimitiveColumn<uint>;
                        newColumn = uintColumn.ApplyFilter(filter);
                        break;
                    case Type ulongType when ulongType == typeof(ulong):
                        PrimitiveColumn<ulong> ulongColumn = column as PrimitiveColumn<ulong>;
                        newColumn = ulongColumn.ApplyFilter(filter);
                        break;
                    case Type ushortType when ushortType == typeof(ushort):
                        PrimitiveColumn<ushort> ushortColumn = column as PrimitiveColumn<ushort>;
                        newColumn = ushortColumn.ApplyFilter(filter);
                        break;
                    case Type stringType when stringType == typeof(string):
                        StringColumn stringColumn = column as StringColumn;
                        newColumn = stringColumn.ApplyFilter(filter);
                        break;
                    default:
                        throw new NotSupportedException();
                }
                ret.InsertColumn(i, newColumn);
            }
            return ret;
        }

        private string PrettyPrint(long start = 0, long numberOfRows = 25)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ColumnCount; i++)
            {
                // Left align by 10
                // TODO: Bug here if Name.Length > 10. The alignment will go out of whack
                sb.Append(string.Format("{0,-14}", Column(i).Name));
            }
            sb.AppendLine();
            numberOfRows = (int)Math.Min(RowCount, start + numberOfRows);
            for (long i = start; i < numberOfRows; i++)
            {
                IList<object> row = this[i];
                foreach (object obj in row)
                {
                    sb.Append(string.Format("{0,-14}", obj.ToString()));
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }

        public override string ToString()
        {
            return PrettyPrint();
        }
    }
}
