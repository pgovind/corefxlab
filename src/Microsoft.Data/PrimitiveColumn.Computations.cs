

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// Generated from PrimitiveColumn.Computations.tt. Do not modify directly

using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data
{
    public partial class PrimitiveColumn<T> : BaseColumn
        where T : unmanaged
    {
        #region Computations
        public override void Abs()
        {
            PrimitiveColumnComputation<T>.Instance.Abs(_columnContainer);
        }
        public override bool All()
        {
            PrimitiveColumnComputation<T>.Instance.All(_columnContainer, out bool ret);
            return ret;
        }
        public override bool Any()
        {
            PrimitiveColumnComputation<T>.Instance.Any(_columnContainer, out bool ret);
            return ret;
        }
        public override void Cummax()
        {
            PrimitiveColumnComputation<T>.Instance.Cummax(_columnContainer);
        }
        public override void Cummin()
        {
            PrimitiveColumnComputation<T>.Instance.Cummin(_columnContainer);
        }
        public override void Cumprod()
        {
            PrimitiveColumnComputation<T>.Instance.Cumprod(_columnContainer);
        }
        public override void Cumsum()
        {
            PrimitiveColumnComputation<T>.Instance.Cumsum(_columnContainer);
        }
        public override object Max()
        {
            PrimitiveColumnComputation<T>.Instance.Max(_columnContainer, out T ret);
            return ret;
        }
        public override object Min()
        {
            PrimitiveColumnComputation<T>.Instance.Min(_columnContainer, out T ret);
            return ret;
        }
        public override object Prod()
        {
            PrimitiveColumnComputation<T>.Instance.Prod(_columnContainer, out T ret);
            return ret;
        }
        public override object Sum()
        {
            PrimitiveColumnComputation<T>.Instance.Sum(_columnContainer, out T ret);
            return ret;
        }
        public override void Round()
        {
            PrimitiveColumnComputation<T>.Instance.Round(_columnContainer);
        }
        #endregion
    }
}
