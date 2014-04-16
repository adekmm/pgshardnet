using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Pgshardnet
{
    public class LogicalShard
    {
        public int id { get; set; }
        public string name { get; set; }
        public PhysicalShard physicalShard { get; set; }
    }
}
