using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
namespace Pgshardnet.Exceptions
{

    [Serializable]
    public class DataNotIntegralException : Exception
    {
        // Constructors
        public DataNotIntegralException(string message)
            : base(message)
        { }

        // Ensure Exception is Serializable
        protected DataNotIntegralException(SerializationInfo info, StreamingContext ctxt)
            : base(info, ctxt)
        { }
    }

}
