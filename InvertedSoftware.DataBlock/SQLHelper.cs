// THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED.
//
// Copyright (C) Inverted Software(TM). All rights reserved.
//

using System.Data;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;

namespace InvertedSoftware.DataBlock.Core
{
    /// <summary>
    /// The SqlHelper class is intended to encapsulate high performance, common uses of SqlClient.
    /// </summary>
    public static class SqlHelper
    {
        public static DefaultObjectPool<SqlCommand> CommandPool = new DefaultObjectPool<SqlCommand>(new SqlCommandPooledObjectPolicy());

        /// <summary>
        /// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">The stored procedure name or T-SQL command.</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>An int representing the number of rows affected by the command.</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            int val;
            SqlCommand cmd = CommandPool.Get();
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    PrepareCommand(cmd, conn, cmdType, cmdText, null, commandParameters);
                    val = cmd.ExecuteNonQuery();
                }
            }
            finally
            {
                CommandPool.Return(cmd);
            }

            return val;
        }

        /// <summary>
        /// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">The stored procedure name or T-SQL command.</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>An int representing the number of rows affected by the command.</returns>
        public static async Task<int> ExecuteNonQueryAsync(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            int val;
            SqlCommand cmd = CommandPool.Get();
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    await PrepareCommandAsync(cmd, conn, cmdType, cmdText, null, commandParameters);
                    val = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                CommandPool.Return(cmd);
            }

            return val;
        }

        /// <summary>
        /// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <param name="conn">The connection to use.</param>
        /// <param name="tran">The SqlTransaction to use.</param>
        /// <param name="cmdType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="cmdText">The stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>An int representing the number of rows affected by the command.</returns>
        public static int ExecuteNonQuery(SqlConnection conn, SqlCommand cmd, SqlTransaction tran, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            PrepareCommand(cmd, conn, cmdType, cmdText, tran, commandParameters);
            return cmd.ExecuteNonQuery();
        }

        /// <summary>
        /// Execute a SqlCommand that returns a resultset against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader r = ExecuteReader(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>A SqlDataReader containing the results.</returns>
        public static SqlDataReader ExecuteReader(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            SqlConnection conn = new SqlConnection(connectionString);
            SqlCommand cmd = conn.CreateCommand();

            // we use a try/catch here because if the method throws an exception we want to 
            // close the connection throw code, because no datareader will exist, hence the 
            // commandBehaviour.CloseConnection will not work
            try
            {
                PrepareCommand(cmd, conn, cmdType, cmdText, null, commandParameters);
                return cmd.ExecuteReader(CommandBehavior.CloseConnection);
            }
            catch
            {
                cmd.Dispose();
                if (conn.State == ConnectionState.Open)
                    conn.Close();
                conn.Dispose();
                throw;
            }
        }


        /// <summary>
        /// Execute a SqlCommand that returns a resultset against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  SqlDataReader r = ExecuteReader(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">a valid connection string for a SqlConnection</param>
        /// <param name="commandType">the CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">the stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">an array of SqlParamters used to execute the command</param>
        /// <returns>A SqlDataReader containing the results.</returns>
        public static async Task<SqlDataReader> ExecuteReaderAsync(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            SqlConnection conn = new SqlConnection(connectionString);
            SqlCommand cmd = conn.CreateCommand();

            // we use a try/catch here because if the method throws an exception we want to 
            // close the connection throw code, because no datareader will exist, hence the 
            // commandBehaviour.CloseConnection will not work
            try
            {
                await PrepareCommandAsync(cmd, conn, cmdType, cmdText, null, commandParameters);
                return await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection).ConfigureAwait(false);
            }
            catch
            {
                cmd.Dispose();
                if (conn.State == ConnectionState.Open)
                    conn.Close();
                conn.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Sends the CommandText to the Connection and builds a SqlDataReader withing the current transaction using the provided parameters.
        /// </summary>
        /// <param name="conn">The connection to use.</param>
        /// <param name="tran">The SqlTransaction to use.</param>
        /// <param name="cmdType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="cmdText">The stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">>An array of SqlParamters used to execute the command.</param>
        /// <returns>A SqlDataReader containing the results.</returns>
        public static SqlDataReader ExecuteReader(SqlConnection conn, SqlCommand cmd, SqlTransaction tran, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            PrepareCommand(cmd, conn, cmdType, cmdText, tran, commandParameters);
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// Execute a SqlCommand that returns the first column of the first record against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <remarks>
        /// e.g.:  
        ///  Object obj = ExecuteScalar(connString, CommandType.StoredProcedure, "PublishOrders", new SqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connectionString">A valid connection string for a SqlConnection</param>
        /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="commandText">The stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>The first column of the first row in the result set, or a null reference if the result set is empty. Returns a maximum of 2033 characters. This object should be converted to the expected type using Convert.To{Type}.</returns>
        public static object ExecuteScalar(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            object val;
            SqlCommand cmd = CommandPool.Get();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    PrepareCommand(cmd, connection, cmdType, cmdText, null, commandParameters);
                    val = cmd.ExecuteScalar();
                }
            }
            finally
            {
                CommandPool.Return(cmd);
            }

            return val;
        }

        /// <summary>
        /// Execute a SqlCommand that returns the first column of the first record against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <param name="conn">The connection to use.</param>
        /// <param name="cmdType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="cmdText">The stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>The first column of the first row in the result set, or a null reference if the result set is empty. Returns a maximum of 2033 characters. This object should be converted to the expected type using Convert.To{Type}.</returns>
        public static async Task<object> ExecuteScalarAsync(string connectionString, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            object val;
            SqlCommand cmd = CommandPool.Get();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await PrepareCommandAsync(cmd, connection, cmdType, cmdText, null, commandParameters);
                    val = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                CommandPool.Return(cmd);
            }

            return val;
        }

        /// <summary>
        /// Execute a SqlCommand that returns the first column of the first record against the database specified in the connection string 
        /// using the provided parameters.
        /// </summary>
        /// <param name="conn">The connection to use.</param>
        /// <param name="tran">The SqlTransaction to use.</param>
        /// <param name="cmdType">The CommandType (stored procedure, text, etc.)</param>
        /// <param name="cmdText">The stored procedure name or T-SQL command</param>
        /// <param name="commandParameters">An array of SqlParamters used to execute the command.</param>
        /// <returns>The first column of the first row in the result set, or a null reference if the result set is empty. Returns a maximum of 2033 characters. This object should be converted to the expected type using Convert.To{Type}.</returns>
        public static object ExecuteScalar(SqlConnection conn, SqlCommand cmd, SqlTransaction tran, CommandType cmdType, string cmdText, params SqlParameter[] commandParameters)
        {
            PrepareCommand(cmd, conn, cmdType, cmdText, tran, commandParameters);
            return cmd.ExecuteScalar();

        }

        /// <summary>
        /// Prepare a command for execution
        /// </summary>
        /// <param name="cmd">SqlCommand object</param>
        /// <param name="conn">SqlConnection object</param>
        /// <param name="trans">SqlTransaction object</param>
        /// <param name="cmdType">Cmd type e.g. stored procedure or text</param>
        /// <param name="cmdText">Command text, e.g. Select * from Products</param>
        /// <param name="cmdParms">SqlParameters to use in the command</param>
        public static void PrepareCommand(SqlCommand cmd, SqlConnection conn, CommandType cmdType, string cmdText, SqlTransaction trans = null, SqlParameter[] cmdParms = null)
        {
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            cmd.CommandType = cmdType;
            if (trans != null)
                cmd.Transaction = trans;

            if (cmdParms != null)
            {
                foreach (SqlParameter parm in cmdParms)
                    if (parm != null)
                        cmd.Parameters.Add(parm);
            }
            if (conn.State != ConnectionState.Open)
                conn.Open();
        }

        /// <summary>
        /// Asynchronously prepare a command for execution
        /// </summary>
        /// <param name="cmd">SqlCommand object</param>
        /// <param name="conn">SqlConnection object</param>
        /// <param name="trans">SqlTransaction object</param>
        /// <param name="cmdType">Cmd type e.g. stored procedure or text</param>
        /// <param name="cmdText">Command text, e.g. Select * from Products</param>
        /// <param name="cmdParms">SqlParameters to use in the command</param>
        public static async Task PrepareCommandAsync(SqlCommand cmd, SqlConnection conn, CommandType cmdType, string cmdText, SqlTransaction trans = null, SqlParameter[] cmdParms = null)
        {
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            cmd.CommandType = cmdType;
            if (trans != null)
                cmd.Transaction = trans;

            if (cmdParms != null)
            {
                foreach (SqlParameter parm in cmdParms)
                    if (parm != null)
                        cmd.Parameters.Add(parm);
            }
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync().ConfigureAwait(false);
        }
    }
}