// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data
{
    public partial class PrimitiveColumn<T> : BaseColumn
    where T : unmanaged
    {
        public override BaseColumn Clip<U>(U lower, U upper)
        {
            object convertedLower = Convert.ChangeType(lower, typeof(T));
            if (typeof(T) == typeof(U) || convertedLower != null)
            {
                return _Clip((T)convertedLower, (T)Convert.ChangeType(upper, typeof(T)));
            }
            else
                throw new ArgumentException(Strings.MismatchedValueType + typeof(T).ToString(), nameof(U));
        }

        private PrimitiveColumn<T> _Clip(T lower, T upper)
        {
            PrimitiveColumn<T> ret = Clone() as PrimitiveColumn<T>;
            Comparer<T> comparer = Comparer<T>.Default;
            for (long i = 0; i < Length; i++)
            {
                if (!ret.IsValid(i))
                    continue;
                T value = ret[i].Value;

                if (comparer.Compare(value, lower) < 0)
                {
                    ret[i] = lower;
                }
                if (comparer.Compare(value, upper) > 0)
                {
                    ret[i] = upper;
                }
            }
            return ret;
        }
    }
}
