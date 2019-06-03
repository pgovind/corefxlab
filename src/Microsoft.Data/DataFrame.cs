// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Microsoft.Collections.Extensions;

namespace Microsoft.Data
{
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
                BaseColumn newColumn = oldColumn.CloneAndAppendNulls(oldColumn.NullCount, sortIndices, !ascending);
                Debug.Assert(newColumn.NullCount == oldColumn.NullCount);
                newColumns.Add(newColumn);
            }
            return new DataFrame(newColumns);
        }

        public enum JoinAlgorithm
        {
            LEFT,
            RIGHT,
            OUTER,
            INNER
        }

        public DataFrame Join(DataFrame other, string leftSuffix = "_left", string rightSuffix = "_right", JoinAlgorithm joinAlgorithm = JoinAlgorithm.LEFT)
        {
            DataFrame ret = new DataFrame();
            if (joinAlgorithm == JoinAlgorithm.LEFT)
            {
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn = Column(i).Clone();
                    newColumn.Name += leftSuffix;
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
                        newColumn = other.Column(i).CloneAndAppendNulls(RowCount - other.RowCount);
                    }
                    else
                    {
                        newColumn = other.Column(i).Clone(mapIndices);
                    }
                    newColumn.Name += rightSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.RIGHT)
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
                        newColumn = Column(i).CloneAndAppendNulls(other.RowCount - RowCount);
                    }
                    else
                    {
                        newColumn = Column(i).Clone(mapIndices);
                    }
                    newColumn.Name += leftSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).Clone();
                    newColumn.Name += rightSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.OUTER)
            {
                long newRowCount = Math.Max(RowCount, other.RowCount);
                long numberOfNulls = newRowCount - RowCount;
                for (int i = 0; i < ColumnCount; i++)
                {
                    BaseColumn newColumn = Column(i).CloneAndAppendNulls(numberOfNulls);
                    newColumn.Name += leftSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                numberOfNulls = newRowCount - other.RowCount;
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).CloneAndAppendNulls(numberOfNulls);
                    newColumn.Name += rightSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.INNER)
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
                    newColumn.Name += leftSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
                for (int i = 0; i < other.ColumnCount; i++)
                {
                    BaseColumn newColumn = other.Column(i).Clone(mapIndices);
                    newColumn.Name += rightSuffix;
                    ret.InsertColumn(ret.ColumnCount, newColumn);
                }
            }
            return ret;
        }

        private void AppendForMerge(DataFrame dataFrameToAppendOn, DataFrame left, DataFrame right, long leftRow, long rightRow)
        {
            for (int i = 0; i < left.ColumnCount; i++)
            {
                BaseColumn leftColumn = left.Column(i);
                BaseColumn column = dataFrameToAppendOn.Column(i);
                if (leftRow == -1)
                {
                    column[]
                }
                appendColumn.Resize(1);
                appendColumn
                appendColumn.Append(leftRow == -1 ? null : leftColumn[leftRow]);
            }
            for (int i = 0; i < right.ColumnCount; i++)
            {
                BaseColumn rightColumn = right.Column(i);
                BaseColumn appendColumn = dataFrameToAppendOn.Column(i + left.ColumnCount);
                appendColumn.Resize(1);
                appendColumn.Append(rightRow == -1 ? null : rightColumn[rightRow]);
            }
        }

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
        public DataFrame Merge<TKey>(DataFrame other, string leftJoinColumn, string rightJoinColumn, string leftSuffix = "_left", string rightSuffix = "_right", JoinAlgorithm joinAlgorithm = JoinAlgorithm.LEFT)
        {
            // A simple hash join
            DataFrame ret = new DataFrame();
            PrimitiveColumn<long> emptyMap = new PrimitiveColumn<long>("Empty");
            for (int i = 0; i < ColumnCount; i++)
            {
                // Create empty columns
                BaseColumn column = Column(i).Clone(emptyMap);
                column.Name += leftSuffix;
                ret.InsertColumn(ret.ColumnCount, column);
            }
            for (int i = 0; i < other.ColumnCount; i++)
            {
                // Create empty columns
                BaseColumn column = other.Column(i).Clone(emptyMap);
                column.Name += rightSuffix;
                ret.InsertColumn(ret.ColumnCount, column);
            }
            if (joinAlgorithm == JoinAlgorithm.LEFT)
            {
                // First hash other dataframe on the rightJoinColumn
                BaseColumn otherColumn = other[rightJoinColumn];
                MultiValueDictionary<TKey, long> multimap = otherColumn.HashColumnValues<TKey>();

                // Go over the records in this dataframe and match with the hashtable
                BaseColumn thisColumn = this[leftJoinColumn];
                ret[thisColumn.Name + leftSuffix].Resize(thisColumn.Length);
                for (long i = 0; i < thisColumn.Length; i++)
                {
                    TKey value = (TKey)(thisColumn[i] ?? default(TKey));
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            AppendForMerge(ret, this, other, i, row);
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, this, other, i, -1);
                    }
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.RIGHT)
            {
                BaseColumn thisColumn = this[leftJoinColumn];
                MultiValueDictionary<TKey, long> multimap = thisColumn.HashColumnValues<TKey>();

                BaseColumn otherColumn = other[rightJoinColumn];
                for (long i = 0; i < otherColumn.Length; i++)
                {
                    TKey value = (TKey)otherColumn[i];
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            AppendForMerge(ret, this, other, row, i);
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, this, other, -1, i);
                    }
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.INNER)
            {
                // Hash the column with the smaller RowCount
                long leftRowCount = RowCount;
                long rightRowCount = other.RowCount;
                BaseColumn hashColumn = (leftRowCount < rightRowCount) ? this[leftJoinColumn] : other[rightJoinColumn];
                BaseColumn otherColumn = ReferenceEquals(hashColumn, this[leftJoinColumn]) ? other[rightJoinColumn] : this[leftJoinColumn];
                MultiValueDictionary<TKey, long> multimap = hashColumn.HashColumnValues<TKey>();

                for (long i = 0; i < otherColumn.Length; i++)
                {
                    TKey value = (TKey)otherColumn[i];
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            AppendForMerge(ret, this, other, row, i);
                        }
                    }
                }
            }
            else if (joinAlgorithm == JoinAlgorithm.OUTER)
            {
                BaseColumn otherColumn = other[rightJoinColumn];
                MultiValueDictionary<TKey, long> multimap = otherColumn.HashColumnValues<TKey>();

                // Go over the records in this dataframe and match with the hashtable
                BaseColumn thisColumn = this[rightJoinColumn];
                for (long i = 0; i < thisColumn.Length; i++)
                {
                    TKey value = (TKey)thisColumn[i];
                    if (multimap.TryGetValue(value, out IReadOnlyCollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            AppendForMerge(ret, this, other, i, row);
                        }
                    }
                    else
                    {
                        AppendForMerge(ret, this, other, i, -1);
                    }
                }
                for (long i = 0; i < otherColumn.Length; i++)
                {
                    TKey value = (TKey)otherColumn[i];
                    if (!multimap.ContainsKey(value))
                    {
                        AppendForMerge(ret, this, other, -1, i);
                    }
                }

            }
            return ret;
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
                sb.Append(string.Format($"0,{-longestColumnName}", Column(i).Name));
            }
            sb.AppendLine();
            long numberOfRows = Math.Min(RowCount, 25);
            for (int i = 0; i < numberOfRows; i++)
            {
                IList<object> row = this[i];
                foreach (object obj in row)
                {
                    sb.Append(string.Format($"0,{-longestColumnName}", obj.ToString()));
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }
    }
}
