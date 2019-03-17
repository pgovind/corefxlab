using System;
using System.Collections.Generic;

namespace Microsoft.Data
{
    public class DataFrame
    {
        private DataFrameTable _table;
        public DataFrame()
        {
            _table = new DataFrameTable();
        }
        public DataFrame(DataFrameTable table)
        {
            _table = table;
        }

        public long NumRows => _table.NumRows;
        public int NumColumns => _table.NumColumns;
        public IList<string> Columns()
        {
            var ret = new List<string>();
            for (int ii = 0; ii < NumColumns; ii++)
            {
                ret.Add(_table.Column(ii).Name);
            }
            return ret;
        }
        public void InsertColumn(int columnIndex, BaseDataFrameColumn column) => _table.InsertColumn(columnIndex, column);
        public void SetColumn(int columnIndex, BaseDataFrameColumn column) => _table.SetColumn(columnIndex, column);
        public object this[long rowIndex, int columnIndex]
        {
            get
            {
                return _table.Column(columnIndex)[rowIndex];
            }
            set
            {
                _table.Column(columnIndex)[rowIndex] = value;
            }
        }

        public IList<object> this[long rowIndex]
        {
            get
            {
                return _table.GetRow(rowIndex);
            }
            //TODO?: set?
        }

        public object this[string columnName]
        {
            get
            {
                int columnIndex = _table.GetColumnIndex(columnName);
                if (columnIndex == -1) throw new ArgumentException($"{columnName} does not exist");
                return _table.Column(columnIndex)[0, (int)Math.Min(_table.NumRows, Int32.MaxValue)];
            }
        }

        public IList<IList<object>> Head(int numberOfRows)
        {
            var ret = new List<IList<object>>();
            for (int ii = 0; ii < numberOfRows; ii++)
            {
                ret.Add(this[ii]);
            }
            return ret;
        }

        public IList<IList<object>> Tail(int numberOfRows)
        {
            var ret = new List<IList<object>>();
            for (long ii = NumRows - numberOfRows; ii < NumRows; ii++)
            {
                ret.Add(this[ii]);
            }
            return ret;
        }
    }
}
