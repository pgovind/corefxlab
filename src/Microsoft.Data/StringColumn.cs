using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data
{
    public partial class StringColumn : BaseColumn
    {
        internal IList<IList<string>> stringBuffers = new List<IList<string>>(); // To store more than intMax number of strings

        public StringColumn(string name, long length) : base(name, length, typeof(string))
        {
        }

        public StringColumn(string name, IEnumerable<string> values) : base(name, 0, typeof(string)) {
            values = values ?? throw new ArgumentNullException(nameof(values));
            if (stringBuffers.Count == 0)
            {
                stringBuffers.Add(new List<string>());
            }
            var curList = stringBuffers[stringBuffers.Count - 1];
            foreach (var value in values)
            {
                if (curList.Count == int.MaxValue)
                {
                    curList = new List<string>();
                    stringBuffers.Add(curList);
                }
                curList.Add(value);
                Length++;
            }
        }

        private int GetBufferIndexContainingRowIndex(ref long rowIndex)
        {
            if (rowIndex > Length)
            {
                throw new ArgumentOutOfRangeException($"Index {rowIndex} cannot be greater than the Column's Length {Length}");
            }
            int curArrayIndex = 0;
            int numBuffers = stringBuffers.Count;
            while (curArrayIndex < numBuffers && rowIndex > stringBuffers[curArrayIndex].Count)
            {
                rowIndex -= stringBuffers[curArrayIndex].Count;
                curArrayIndex++;
            }
            return curArrayIndex;
        }

        public override object this[long rowIndex]
        {
            get
            {
                int bufferIndex = GetBufferIndexContainingRowIndex(ref rowIndex);
                return stringBuffers[bufferIndex][(int)rowIndex];
            }
            set
            {
                if (value is string)
                {
                    int bufferIndex = GetBufferIndexContainingRowIndex(ref rowIndex);
                    stringBuffers[bufferIndex][(int)rowIndex] = (string)value;
                }
                else
                {
                    throw new ArgumentException(nameof(value));
                }
            }
        }

        public override object this[long startIndex, int length]
        {
            get
            {
                var ret = new List<string>();
                int bufferIndex = GetBufferIndexContainingRowIndex(ref startIndex);
                while (ret.Count < length && bufferIndex < stringBuffers.Count)
                {
                    for (int i = (int)startIndex; ret.Count < length && i < stringBuffers[bufferIndex].Count; i++)
                    {
                        ret.Add(stringBuffers[bufferIndex][i]);
                    }
                    bufferIndex++;
                    startIndex = 0;
                }
                return ret;
            }
        }

        public StringColumn Clone()
        {
            StringColumn ret = new StringColumn(Name, Length);
            for (int i = 0; i < stringBuffers.Count; i++)
            {
                IList<string> buffer = stringBuffers[i];
                IList<string> newBuffer = new List<string>(buffer.Count);
                ret.stringBuffers.Add(newBuffer);
                int bufferLen = buffer.Count;
                for (int j = 0; j < bufferLen; j++)
                {
                    newBuffer.Add(buffer[j]);
                }
            }
            return ret;
        }
    }
}
