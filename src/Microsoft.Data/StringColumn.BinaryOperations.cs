using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Data
{
    public partial class StringColumn : BaseColumn
    {
        public override BaseColumn Add(BaseColumn column)
        {
            // TODO: Using indexing is VERY inefficient here. Each indexer call will find the "right" buffer and then return the value
            if (Length != column.Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            StringColumn ret = Clone();
            for (long i = 0; i < Length; i++)
            {
                ret[i] += column[i].ToString();
            }
            return ret;
        }

        public override BaseColumn Add<T>(T value)
        {
            StringColumn ret = Clone();
            string valString = value.ToString();
            for (int i = 0; i < ret.stringBuffers.Count; i++)
            {
                IList<string> buffer = ret.stringBuffers[i];
                int bufferLen = buffer.Count;
                for (int j = 0; j < bufferLen; j++)
                {
                    buffer[j] += valString;
                }
            }
            return ret;
        }

        public override BaseColumn Equals(BaseColumn column)
        {
            // TODO: Using indexing is VERY inefficient here. Each indexer call will find the "right" buffer and then return the value
            if (Length != column.Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            PrimitiveColumn<bool> ret = new PrimitiveColumn<bool>(Name);
            for (long i = 0; i < Length; i++)
            {
                ret.Append(this[i] == column[i]);
            }
            return ret;
        }

        public override BaseColumn Equals<T>(T value)
        {
            PrimitiveColumn<bool> ret = new PrimitiveColumn<bool>(Name);
            string valString = value.ToString();
            for (int i = 0; i < stringBuffers.Count; i++)
            {
                IList<string> buffer = stringBuffers[i];
                int bufferLen = buffer.Count;
                for (int j = 0; j < bufferLen; j++)
                {
                    ret.Append(buffer[j] == valString);
                }
            }
            return ret;
        }

        public override BaseColumn NotEquals(BaseColumn column)
        {
            // TODO: Using indexing is VERY inefficient here. Each indexer call will find the "right" buffer and then return the value
            if (Length != column.Length)
            {
                throw new ArgumentException($"Column lengths are mismatched", nameof(column));
            }
            PrimitiveColumn<bool> ret = new PrimitiveColumn<bool>(Name);
            for (long i = 0; i < Length; i++)
            {
                ret.Append(this[i] != column[i]);
            }
            return ret;
        }

        public override BaseColumn NotEquals<T>(T value)
        {
            PrimitiveColumn<bool> ret = new PrimitiveColumn<bool>(Name);
            string valString = value.ToString();
            for (int i = 0; i < stringBuffers.Count; i++)
            {
                IList<string> buffer = stringBuffers[i];
                int bufferLen = buffer.Count;
                for (int j = 0; j < bufferLen; j++)
                {
                    ret.Append(buffer[j] != valString);
                }
            }
            return ret;
        }
    }
}
