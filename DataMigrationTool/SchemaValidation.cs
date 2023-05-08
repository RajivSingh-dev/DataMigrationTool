using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMigrationTool
{
    internal class SchemaValidation
    {

        public bool IsSameColumnns { get; set; }  
        public bool IsSameDatatypes { get; set;}
        public bool IsIdentity { get; set;}
    }
}
