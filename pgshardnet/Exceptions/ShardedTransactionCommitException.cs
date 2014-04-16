using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
namespace Pgshardnet.Exceptions
{

    [Serializable]
    public class ShardedTransactionCommitException : Exception
    {
        // Constructors
        public ShardedTransactionCommitException(string message)
            : base(message)
        { }

        // Ensure Exception is Serializable
        protected ShardedTransactionCommitException(SerializationInfo info, StreamingContext ctxt)
            : base(info, ctxt)
        { }
    }

}
