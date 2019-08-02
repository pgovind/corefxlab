// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

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

        public void InsertColumn(int columnIndex, BaseColumn column)
        {
            _table.InsertColumn(columnIndex, column, this);
            OnColumnsChanged();
        }

        public void SetColumn(int columnIndex, BaseColumn column)
        {
            _table.SetColumn(columnIndex, column);
            OnColumnsChanged();
        }

        public void RemoveColumn(int columnIndex)
        {
            _table.RemoveColumn(columnIndex);
            OnColumnsChanged();
        }

        public void RemoveColumn(string columnName)
        {
            _table.RemoveColumn(columnName);
            OnColumnsChanged();
        }

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
                    throw new ArgumentException(Strings.InvalidColumnName, nameof(columnName));
                return _table.Column(columnIndex);
            }
            set
            {
                int columnIndex = _table.GetColumnIndex(columnName);
                BaseColumn newColumn = value;
                newColumn.Name = columnName;
                if (columnIndex == -1)
                {
                    _table.InsertColumn(ColumnCount, newColumn, this);
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

        public DataFrame Description()
        {
            var ret = new DataFrame();
            StringColumn descColumn = new StringColumn("Description", 0);
            descColumn.Append("Length");
            descColumn.Append("Max");
            descColumn.Append("Min");
            descColumn.Append("Mean");
            ret.InsertColumn(ret.ColumnCount, descColumn);
            for (int i = 0; i < ColumnCount; i++)
            {
                var column = Column(i);
                if (column.DataType == typeof(string) || column.DataType == typeof(bool) || column.DataType == typeof(char))
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

        /// <summary>
        /// Return a new DataFrame with rows filtered by true values in boolColumn 
        /// </summary>
        /// <param name="boolColumn">A column of bools where true implies a selection</param>
        public DataFrame this[BaseColumn boolColumn] => Clone(boolColumn);

        private DataFrame Clone(BaseColumn mapIndices = null, bool invertMapIndices = false)
        {
            List<BaseColumn> newColumns = new List<BaseColumn>(ColumnCount);
            for (int i = 0; i < ColumnCount; i++)
            {
                newColumns.Add(Column(i).Clone(mapIndices, invertMapIndices));
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
                Dictionary<TKey, ICollection<long>> multimap = otherColumn.GroupColumnValues<TKey>();

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
                    if (multimap.TryGetValue(value, out ICollection<long> rowNumbers))
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
                Dictionary<TKey, ICollection<long>> multimap = thisColumn.GroupColumnValues<TKey>();

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
                    if (multimap.TryGetValue(value, out ICollection<long> rowNumbers))
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
                Dictionary<TKey, ICollection<long>> multimap = hashColumn.GroupColumnValues<TKey>();

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
                    if (multimap.TryGetValue(value, out ICollection<long> rowNumbers))
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
                Dictionary<TKey, ICollection<long>> multimap = otherColumn.GroupColumnValues<TKey>();
                Dictionary<TKey, ICollection<long>> intersection = new Dictionary<TKey, ICollection<long>>(EqualityComparer<TKey>.Default);

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
                    if (multimap.TryGetValue(value, out ICollection<long> rowNumbers))
                    {
                        foreach (long row in rowNumbers)
                        {
                            if (thisColumn[i] == null)
                            {
                                // Has to match only with nulls in otherColumn
                                if (otherColumn[row] == null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                    if (intersection.TryGetValue(value, out ICollection<long> currentRows))
                                    {
                                        currentRows.Add(rowNumber);
                                    }
                                    else
                                    {
                                        var newRows = new List<long>();
                                        newRows.Add(rowNumber);
                                        intersection.Add(value, newRows);
                                    }
                                }
                            }
                            else
                            {
                                // Cannot match to nulls in otherColumn
                                if (otherColumn[row] != null)
                                {
                                    AppendForMerge(ret, rowNumber++, this, other, i, row);
                                    if (intersection.TryGetValue(value, out ICollection<long> currentRows))
                                    {
                                        currentRows.Add(rowNumber);
                                    }
                                    else
                                    {
                                        var newRows = new List<long>();
                                        newRows.Add(rowNumber);
                                        intersection.Add(value, newRows);
                                    }
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
                throw new ArgumentException(Strings.InvalidColumnName, nameof(columnName));

            BaseColumn column = _table.Column(columnIndex);
            return column.GroupBy(columnIndex, this);
        }

        // In a GroupBy call, columns get resized. We need to set the RowCount to reflect the true Length of the DataFrame. Internal only. Should not be exposed
        internal void SetTableRowCount(long rowCount)
        {
            // Even if current RowCount == rowCount, do the validation
            for (int i = 0; i < ColumnCount; i++)
            {
                if (Column(i).Length != rowCount)
                    throw new ArgumentException(String.Format("{0} {1}", Strings.MismatchedRowCount, Column(i).Name));
            }
            _table.RowCount = rowCount;
        }

        /// <summary>
        /// Invalidates any cached data after a column has changed.
        /// </summary>
        private void OnColumnsChanged()
        {
            _schema = null;
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
                    sb.Append((obj ?? "null").ToString().PadRight(longestColumnName));
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }
    }
}
