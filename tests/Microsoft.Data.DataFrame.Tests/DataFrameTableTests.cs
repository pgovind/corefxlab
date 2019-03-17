using System.Linq;
using System.Collections.Generic;
using Microsoft.Data;
using Xunit;

namespace Microsoft.DataFrame.Tests
{
    public class DataFrameTableTests
    {
        public Data.DataFrame MakeTestTableWithTwoColumns(int length)
        {
            BaseDataFrameColumn dataFrameColumn1 = new DataFrameColumn<int>("Int1", Enumerable.Range(0, length).Select(x => x));
            BaseDataFrameColumn dataFrameColumn2 = new DataFrameColumn<int>("Int2", Enumerable.Range(10, length).Select(x => x));
            Data.DataFrame dataFrame = new Data.DataFrame();
            dataFrame.InsertColumn(0, dataFrameColumn1);
            dataFrame.InsertColumn(1, dataFrameColumn2);
            return dataFrame;
        }
        [Fact]
        public void TestIndexer()
        {
            Data.DataFrame dataFrame = MakeTestTableWithTwoColumns(length: 10);
            var foo = dataFrame[0, 0];
            Assert.Equal(0, dataFrame[0, 0]);
            Assert.Equal(11, dataFrame[1, 1]);
            Assert.Equal(2, dataFrame.Columns().Count);
            Assert.Equal("Int1", dataFrame.Columns()[0]);

            var headList = dataFrame.Head(5);
            Assert.Equal(14, (int)headList[4][1]);

            var tailList = dataFrame.Tail(5);
            Assert.Equal(19, (int)tailList[4][1]);

            dataFrame[2, 1] = 1000;
            Assert.Equal(1000, dataFrame[2, 1]);

            var row = dataFrame[4];
            Assert.Equal(14, (int)row[1]);

            var column = (IList<int>)dataFrame["Int2"];
            Assert.Equal(1000, (int)column[2]);
        }

        [Fact]
        public void ColumnAndTableCreationTest()
        {
            BaseDataFrameColumn intColumn = new DataFrameColumn<int>("IntColumn", Enumerable.Range(0, 10).Select(x => x));
            BaseDataFrameColumn floatColumn = new DataFrameColumn<float>("FloatColumn", Enumerable.Range(0, 10).Select(x => (float)x));
            DataFrameTable table = new DataFrameTable(new List<BaseDataFrameColumn> { intColumn, floatColumn });
            Assert.Equal(10, table.NumRows);
            Assert.Equal(2, table.NumColumns);
            Assert.Equal(10, table.Column(0).Length);
            Assert.Equal("IntColumn", table.Column(0).Name);
            Assert.Equal(10, table.Column(1).Length);
            Assert.Equal("FloatColumn", table.Column(1).Name);
            
            BaseDataFrameColumn bigColumn = new DataFrameColumn<float>("BigColumn", Enumerable.Range(0, 11).Select(x => (float)x));
            BaseDataFrameColumn repeatedName = new DataFrameColumn<float>("FloatColumn", Enumerable.Range(0, 10).Select(x => (float)x));
            Assert.Throws<System.ArgumentException>( () => table.InsertColumn(2, bigColumn));
            Assert.Throws<System.ArgumentException>( () => table.InsertColumn(2, repeatedName));
            Assert.Throws<System.ArgumentException>( () => table.InsertColumn(10, repeatedName));

            Assert.Equal(2, table.NumColumns);
            BaseDataFrameColumn intColumnCopy = new DataFrameColumn<int>("IntColumn", Enumerable.Range(0, 10).Select(x => x));
            Assert.Throws<System.ArgumentException>(() => table.SetColumn(1, intColumnCopy));
            
            BaseDataFrameColumn differentIntColumn = new DataFrameColumn<int>("IntColumn1", Enumerable.Range(0, 10).Select(x => x));
            table.SetColumn(1, differentIntColumn);
            Assert.True(differentIntColumn == table.Column(1));

            table.RemoveColumn(1);
            Assert.Equal(1, table.NumColumns);
            Assert.True(intColumn == table.Column(0));
        }
    }
}
