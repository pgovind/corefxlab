using System;
using Apache.Arrow;
using System.Collections.Generic;

namespace Microsoft.Data.DataFrame
{
    public class DataFrame // IDataView eventually
    {
        private Table table;

        #region constructors
        public DataFrame()
        {
            table = new Table();
        }

        // Constructor with a dictionary
        public DataFrame(IEnumerable<int> columnData, IList<string> columnNames) 
        {
            // Is there a way to pass IList<T> as the parameter? Nope. Might need a builder class for this
        }

        #endregion

        #region DataFrame

        public int NumColumns => table.NumColumns;
        public int NumRows => table.NumColumns;
        public void AddColumn(int columnIndex, Column column)
        {
            table.AddColumn(columnIndex, column);
        }

        public void RemoveColumn(int columnIndex)
        {
            table.RemoveColumn(columnIndex);
        }

        public void SetColumn(int columnIndex, Column column)
        {
            table.SetColumn(columnIndex, column);
        }
        #endregion

        #region Operators

        public Column this[int columnIndex] => table.Column(columnIndex);

        #endregion
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }
    }
}
