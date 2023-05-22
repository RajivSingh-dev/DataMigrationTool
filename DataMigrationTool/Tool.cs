using Microsoft.Extensions.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;

namespace ConsoleApp
{
    class Tool
    {
        private readonly IConfiguration _configuration;

        public Tool(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void Migrate() {

            Console.WriteLine("Enter source table name:");
            string inputSourceTable = Console.ReadLine();

            Console.WriteLine("Enter destination table name:");
            string inputDestinationTable = Console.ReadLine();

            if(!inputSourceTable.Equals(inputDestinationTable))
                Console.WriteLine($"Table name does not match. Source: {inputSourceTable}, Destination: {inputDestinationTable}");

            Console.WriteLine("Enter source column name:");
            string inputSourceColumn = Console.ReadLine();
            
            Console.WriteLine("Enter destination column name:");
            string inputDestinationColumn = Console.ReadLine();

            if (!inputSourceColumn.Equals(inputDestinationColumn))
                Console.WriteLine($"Column name does not match. Source: {inputSourceColumn}, Destination: {inputDestinationColumn}");

            Console.WriteLine("Enter source id:");
            int inputSourceId;
            int.TryParse(Console.ReadLine(), out inputSourceId);

            SqlConnection sourceConnection = null;
            SqlConnection destinationConnection = null;
            SqlConnection destinationWriteConnection = null;

            SqlCommand  sourceCommand = null;
            SqlCommand destinationCommand = null;
            SqlCommand destinationWriteCommand = null;

            try
            {

                string sourceConnectionString = _configuration.GetConnectionString("sourceConnectionString");
                string destinationConnectionString = _configuration.GetConnectionString("destinationConnectionString");

                sourceConnection = new SqlConnection(sourceConnectionString);
                sourceConnection.Open();

                destinationConnection = new SqlConnection(destinationConnectionString);
                destinationConnection.Open();

                destinationWriteConnection = new SqlConnection(destinationConnectionString);
                destinationWriteConnection.Open();


                 sourceCommand = sourceConnection.CreateCommand();
                 destinationCommand = destinationConnection.CreateCommand();
                 destinationWriteCommand = destinationWriteConnection.CreateCommand();

                sourceCommand.CommandText = "SELECT * FROM " + inputSourceTable + " WHERE " + inputSourceColumn + " = @id";
                sourceCommand.Parameters.AddWithValue("@id", inputSourceId);

                using (SqlDataReader sourceDataReader = sourceCommand.ExecuteReader())
                {

                    int rowsAffected = 0;
                    int columnCount = sourceDataReader.FieldCount;
                    destinationCommand.CommandText = "SELECT * FROM " + inputDestinationTable + " WHERE " + inputSourceColumn + "= @id";
                    destinationCommand.Parameters.AddWithValue("@id", inputSourceId);
                    destinationWriteCommand.Parameters.AddWithValue("@id", inputSourceId);

                    using (SqlDataReader destinationDataReader = destinationCommand.ExecuteReader())
                    {

                        StringBuilder destinationWriteCommandText = new StringBuilder();
                        if (IsSchemaMatches(sourceDataReader, destinationDataReader, inputSourceColumn, out string isIdentity))
                        {
                            if (destinationDataReader.Read())
                            {
                                if (isIdentity.Equals(inputSourceColumn))
                                {
                                    destinationWriteCommandText.Append("UPDATE " + inputDestinationTable + " SET ");

                                    StringBuilder columnsToUpdate = new StringBuilder();

                                    if (sourceDataReader.Read())
                                        for (int i = 0; i < columnCount; i++)
                                        {
                                            string colName = sourceDataReader.GetName(i);
                                            if (!sourceDataReader[colName].Equals(destinationDataReader[colName]))
                                            {
                                                columnsToUpdate.Append(colName + " = " + "@p" + i);
                                                destinationWriteCommand.Parameters.AddWithValue("@p" + i, sourceDataReader[colName]);
                                                columnsToUpdate.Append(",");
                                            }

                                        }

                                    if (columnsToUpdate.Length > 0)
                                    {
                                        columnsToUpdate.Length -= 1;
                                        destinationWriteCommandText.Append(columnsToUpdate);
                                        destinationWriteCommandText.Append(" WHERE " + inputDestinationColumn + "= @id");
                                        destinationWriteCommand.CommandText = destinationWriteCommandText.ToString();
                                    }
                                    else
                                        Console.WriteLine("Destination row contains same value");
                                rowsAffected = destinationWriteCommand.ExecuteNonQuery();
                                }
                                else
                                {
                                    Console.WriteLine("Destination column should be identity");
                                }
                                
                            }
                            else
                            {

                                List<string> columnNames = new List<string>();
                                List<string> parameterNames = new List<string>();

                                for (int i = 0; i < columnCount; i++)
                                {
                                    if (!isIdentity.Equals(sourceDataReader.GetName(i)))
                                    {
                                        columnNames.Add(sourceDataReader.GetName(i));
                                        parameterNames.Add("@p" + i);
                                    }
                                }

                                destinationWriteCommandText.Append("Insert into " + inputDestinationTable +" (");
                                destinationWriteCommandText.Append(string.Join(", ", columnNames));
                                destinationWriteCommandText.Append(") values (");
                                destinationWriteCommandText.Append(string.Join(", ", parameterNames));
                                destinationWriteCommandText.Append(");");

                                destinationWriteCommand.CommandText = destinationWriteCommandText.ToString();

                                if (sourceDataReader.HasRows)
                                {
                                    while (sourceDataReader.Read())
                                    {
                                        for (int i = 0; i < parameterNames.Count; i++)
                                            destinationWriteCommand.Parameters.AddWithValue(parameterNames[i], sourceDataReader[columnNames[i]]);

                                        rowsAffected += destinationWriteCommand.ExecuteNonQuery();
                                        destinationWriteCommand.Parameters.Clear();
                                    }

                                }
                            }
                        }
                        else
                        {
                            Console.WriteLine("Schema not matching");
                        }
                    }
                    destinationCommand.Parameters.Clear();
                    Console.WriteLine($"{rowsAffected} row inserted into DestinationTable based on matching id value.");



                }

            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
            finally
            {
                if(sourceConnection != null)
                  sourceConnection.Dispose();
                if(destinationConnection != null)
                  destinationConnection.Dispose();
                if(destinationWriteConnection!= null) 
                  destinationWriteConnection.Dispose();
            }
        }

         bool IsSchemaMatches(SqlDataReader sourceDataReader, SqlDataReader destinationDataReader, string sourceColumn, out string isIdentity)
        {
            isIdentity = "";

            bool rValue = false;



            IList<DbColumn> sourceColumns = sourceDataReader.GetColumnSchema();
            IList<DbColumn> destColumns = destinationDataReader.GetColumnSchema();

            if (sourceColumns.Count != destColumns.Count)
            { return false; }

            foreach (DbColumn srcColumn in sourceColumns)
            {
                rValue = false;
                foreach (DbColumn destColumn in destColumns)
                {
                    if (srcColumn.IsIdentity.GetValueOrDefault() && destColumn.IsIdentity.GetValueOrDefault())
                        isIdentity = destColumn.ColumnName;

                    if (
                         !srcColumn.ColumnSize.Equals(destColumn.ColumnSize) ||
                         !srcColumn.NumericPrecision.Equals(destColumn.NumericPrecision) ||
                         !srcColumn.NumericScale.Equals(destColumn.NumericScale) ||
                         !srcColumn.DataTypeName.Equals(destColumn.DataTypeName) ||
                         !srcColumn.IsIdentity.Equals(destColumn.IsIdentity)
                         )
                        break;
                }

                rValue = true;
            }


            return rValue;
        }


    }
}
