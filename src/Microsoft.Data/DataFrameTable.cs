using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    public class DataFrameTable
    {
        private IList<BaseDataFrameColumn> _columns;

        private List<string> _columnNames = new List<string>();
        private Dictionary<string, int> _columnNameToIndexDictionary = new Dictionary<string, int>();

        public long NumRows { get; private set; } = 0;
        public int NumColumns { get; private set; } = 0;
        public DataFrameTable()
        {
            _columns = new List<BaseDataFrameColumn>();
        }

        public DataFrameTable(IList<BaseDataFrameColumn> columns)
        {
            columns = columns ?? throw new ArgumentNullException(nameof(columns));
            _columns = columns;
            NumColumns = columns.Count;
            if (columns.Count > 0)
            {
                NumRows = columns[0].Length;
                int cc = 0;
                foreach (var column in columns)
                {
                    _columnNames.Add(column.Name);
                    _columnNameToIndexDictionary.Add(column.Name, cc++);
                }
            }
        }

        public DataFrameTable(BaseDataFrameColumn column) : this(new List<BaseDataFrameColumn> { column }) { }
        public BaseDataFrameColumn Column(int columnIndex) => _columns[columnIndex];
        public IList<object> GetRow(long rowIndex)
        {
            var ret = new List<object>();
            for (int ii = 0; ii < NumColumns; ii++)
            {
                ret.Add(Column(ii)[rowIndex]);
            }
            return ret;
        }

        public void InsertColumn<T>(int columnIndex, IEnumerable<T> column, string columnName)
            where T : struct
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex > _columns.Count)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed into Table.AddColumn");
            }
            BaseDataFrameColumn newColumn = new DataFrameColumn<T>(columnName, column);
            InsertColumn(columnIndex, newColumn);
        }
        public void InsertColumn(int columnIndex, BaseDataFrameColumn column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex > _columns.Count)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed into Table.AddColumn");
            }
            if (NumRows > 0 && column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match Table's length {NumRows}");
            }
            if (_columnNameToIndexDictionary.ContainsKey(column.Name))
            {
                throw new ArgumentException($"Table already contains a column called {column.Name}");
            }
            NumRows = column.Length;
            _columnNames.Insert(columnIndex, column.Name);
            _columnNameToIndexDictionary[column.Name] = columnIndex;
            _columns.Insert(columnIndex, column);
            NumColumns++;
        }
        public void SetColumn(int columnIndex, BaseDataFrameColumn column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex >= NumColumns)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed in to Table.SetColumn");
            }
            if (NumRows > 0 && column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match table's length {NumRows}");
            }
            if (_columnNameToIndexDictionary.ContainsKey(column.Name))
            {
                throw new ArgumentException($"Table already contains a column called {column.Name}");
            }
            _columnNameToIndexDictionary.Remove(_columnNames[columnIndex]);
            _columnNames[columnIndex] = column.Name;
            _columnNameToIndexDictionary[column.Name] = columnIndex;
            _columns[columnIndex] = column;
        }
        public void RemoveColumn(int columnIndex)
        {
            _columnNameToIndexDictionary.Remove(_columnNames[columnIndex]);
            _columnNames.RemoveAt(columnIndex);
            _columns.RemoveAt(columnIndex);
            NumColumns--;
        }

        public int GetColumnIndex(string columnName)
        {
            if (_columnNameToIndexDictionary.TryGetValue(columnName, out int columnIndex) )
            {
                return columnIndex;
            }
            return -1;
        }
    }
}
