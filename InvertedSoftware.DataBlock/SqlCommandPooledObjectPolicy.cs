using Microsoft.Extensions.ObjectPool;
using System.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace InvertedSoftware.DataBlock.Core
{
    public class SqlCommandPooledObjectPolicy : PooledObjectPolicy<SqlCommand>
    {
        public override SqlCommand Create() => new SqlCommand();

        public override bool Return(SqlCommand obj)
        {
            if (obj.Connection != null)
            {
                if (obj.Connection.State == ConnectionState.Open)
                    obj.Connection.Close();
                obj.Connection.Dispose();
				obj.Connection = null;
			}
            
            obj.Parameters.Clear();
            return true;
        }
    }
}
