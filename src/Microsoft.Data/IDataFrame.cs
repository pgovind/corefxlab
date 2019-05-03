using System.Collections.Generic;
using Microsoft.ML;

namespace Microsoft.Data
{
    public interface IDataFrame
    {
        IList<object> this[long rowIndex] { get; }
        BaseColumn this[string columnName] { get; set; }
        object this[long rowIndex, int columnIndex] { get; set; }

        int ColumnCount { get; }
        IList<string> Columns { get; }
        long RowCount { get; }
        DataViewSchema Schema { get; set; }

        DataFrame Add<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Add<T>(T value) where T : unmanaged;
        DataFrame And<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame And<T>(T value) where T : unmanaged;
        BaseColumn Column(int index);
        DataFrame Description();
        DataFrame Divide<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Divide<T>(T value) where T : unmanaged;
        DataFrame Equals<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Equals<T>(T value) where T : unmanaged;
        DataFrame FilterRows<T>(string columnName, T lowerBound, T upperBound) where T : unmanaged;
        long? GetRowCount();
        DataFrame GreaterThan<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame GreaterThan<T>(T value) where T : unmanaged;
        DataFrame GreaterThanOrEqual<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame GreaterThanOrEqual<T>(T value) where T : unmanaged;
        IList<IList<object>> Head(int numberOfRows);
        void InsertColumn(int columnIndex, BaseColumn column);
        DataFrame LeftShift(int value);
        DataFrame LessThan<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame LessThan<T>(T value) where T : unmanaged;
        DataFrame LessThanOrEqual<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame LessThanOrEqual<T>(T value) where T : unmanaged;
        DataFrame Modulo<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Modulo<T>(T value) where T : unmanaged;
        DataFrame Multiply<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Multiply<T>(T value) where T : unmanaged;
        DataFrame NotEquals<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame NotEquals<T>(T value) where T : unmanaged;
        DataFrame Or<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Or<T>(T value) where T : unmanaged;
        DataFrame Predict(MLContext mlContext, ITransformer model, string columnToPredict);
        void RemoveColumn(int columnIndex);
        void RemoveColumn(string columnName);
        DataFrame RightShift(int value);
        void SetColumn(int columnIndex, BaseColumn column);
        DataFrame Sort(string columnName, bool ascending = true);
        DataFrame Subtract<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Subtract<T>(T value) where T : unmanaged;
        IList<IList<object>> Tail(int numberOfRows);
        string ToString();
        DataFrame Xor<T>(IReadOnlyList<T> values) where T : unmanaged;
        DataFrame Xor<T>(T value) where T : unmanaged;
    }
}