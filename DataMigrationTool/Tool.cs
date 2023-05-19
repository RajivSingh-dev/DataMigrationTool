
using DataMigrationTool;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Primitives;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Dynamic;
using System.Reflection.PortableExecutable;
using System.Resources;
using System.Text;

namespace ConsoleApp
{
    class Test
    {
        static void Main(string[] args)
        {
            /*Console.WriteLine("Enter the source table name:");
            string inputSourceTable = Console.ReadLine();
            Console.WriteLine("Enter the destination table name:");
            string inputDestinationTable = Console.ReadLine();
            Console.WriteLine("Enter the source column name:");
            string inputSourceColumn = Console.ReadLine();
            Console.WriteLine("Enter the destination column name:");
            string inputDestinationColumn = Console.ReadLine();
            Console.WriteLine("Enter the source ID:");
            string inputSourceId = Console.ReadLine();*/

            string inputSourceTable = "test";
            string inputDestinationTable = "test";
            string inputSourceColumn = "Id";
            string inputDestinationColumn = "Id";
            int inputSourceId = 10;



            try
            {

                string sourceConnectionString = "Data Source=.;Initial Catalog=Live;Integrated Security=True";
                string destinationConnectionString = "Data Source=.;Initial Catalog=ksa;Integrated Security=True";

                using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
                {
                    sourceConnection.Open();

                    using (SqlCommand sourceCommand = new SqlCommand())
                    {
                        sourceCommand.Connection = sourceConnection;

                        sourceCommand.CommandText = "SELECT * FROM "+ inputSourceTable +" WHERE Id = @id";

                        sourceCommand.Parameters.AddWithValue("@id", inputSourceId);

                        using (SqlDataReader sourceDataReader = sourceCommand.ExecuteReader())
                        {


                            using (SqlConnection destinationConnection = new SqlConnection(destinationConnectionString))
                            {
                                destinationConnection.Open();

                                using (SqlCommand destinationCommand = new SqlCommand())
                                {
                                    destinationCommand.Connection = destinationConnection;

                                    int columnCount = sourceDataReader.FieldCount;
                                    destinationCommand.CommandText = "SELECT * FROM "+ inputDestinationTable + " WHERE " + inputSourceColumn + "= @id";
                                    destinationCommand.Parameters.AddWithValue("@id", inputSourceId);

                                    using (SqlDataReader destinationDataReader = destinationCommand.ExecuteReader())
                                    {

                                        StringBuilder destinationCommandText = new StringBuilder();

                                        if (IsSchemaMatches(sourceDataReader, destinationDataReader, inputSourceColumn, out bool isIdentity))
                                        {
                                           
                                            if (destinationDataReader.Read())
                                            {

                                                if (isIdentity)
                                                {
                                                    destinationCommandText.Append("UPDATE "+ inputDestinationTable + " SET ");

                                                    StringBuilder columnsToUpdate = new StringBuilder();

                                                    if(sourceDataReader.Read())
                                                    for (int i = 0; i < columnCount; i++)
                                                    {
                                                        string colName = sourceDataReader.GetName(i);
                                                        if (!sourceDataReader[colName].Equals(destinationDataReader[colName]))
                                                        {
                                                            columnsToUpdate.Append(colName + " = " + "@p" + i);
                                                            destinationCommand.Parameters.AddWithValue("@p" + i, sourceDataReader[colName]);
                                                            columnsToUpdate.Append(",");
                                                        }

                                                    }

                                                    if(columnsToUpdate.Length > 0)
                                                    {
                                                       columnsToUpdate.Length -= 1;
                                                       destinationCommandText.Append(columnsToUpdate);
                                                       destinationCommandText.Append(" WHERE " + inputSourceColumn + "= @id");
                                                       destinationCommand.CommandText = destinationCommandText.ToString(); 
                                                    }
                                                    else
                                                      Console.WriteLine("Destination row contains same value");

                                                   
                                                }
                                            }
                                            else
                                            {  
                                                List<string> columnNames = new List<string>();
                                                List<string> parameterNames = new List<string>();


                                                for (int i = 0; i < columnCount; i++)
                                                    {
                                                        if (isIdentity && sourceDataReader.GetName(i).Equals(inputSourceColumn))
                                                            continue;
                                                        columnNames.Add(sourceDataReader.GetName(i));
                                                        parameterNames.Add("@p" + i);
                                                    }

                                                destinationCommandText.Append("Insert into test (");
                                                destinationCommandText.Append(string.Join(", ", columnNames));
                                                destinationCommandText.Append(") values (");
                                                destinationCommandText.Append(string.Join(", ", parameterNames));
                                                destinationCommandText.Append(");");

                                                destinationCommand.CommandText = destinationCommandText.ToString();

                                                while (sourceDataReader.Read())
                                                {
                                                    int j = 0;
                                                    for (int i = 0; i < columnCount; i++,j++)
                                                    {
                                                        if (isIdentity && sourceDataReader.GetOrdinal(inputSourceColumn) == i)
                                                        {
                                                            j--;
                                                            continue; 
                                                        }
                                                        destinationCommand.Parameters.AddWithValue(parameterNames[j], sourceDataReader[columnNames[j]]);
                                                    }
                                                    destinationCommand.ExecuteNonQuery();
                                                }
                                            }
                                        }
                                        else
                                        {
                                            Console.WriteLine("Schema not matching");
                                        }
                                    }
                                    int rowsAffected = destinationCommand.ExecuteNonQuery();
                                    destinationCommand.Parameters.Clear();
                                    Console.WriteLine($"{rowsAffected} row inserted into DestinationTable based on matching id value.");

                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }


        static bool IsSchemaMatches(SqlDataReader sourceDataReader, SqlDataReader destinationDataReader,string sourceColumn, out bool isIdentity)
        {
            isIdentity = false;

            bool rValue = false;

            DataTable sourceTable = sourceDataReader.GetSchemaTable();
            DataTable destTable = destinationDataReader.GetSchemaTable();

            IList<DbColumn> sourceColumns = sourceDataReader.GetColumnSchema();
            IList<DbColumn> destColumns = destinationDataReader.GetColumnSchema();

            if(sourceColumns.Count != destColumns.Count)
            { return false; }

            foreach(DbColumn srcColumn in sourceColumns) 
            {
                rValue= false;
                foreach (DbColumn destColumn in destColumns )
                {
                    if(srcColumn.ColumnName.Equals(destColumn.ColumnName)) 
                    {
                        if(srcColumn.IsIdentity.GetValueOrDefault() && destColumn.IsIdentity.GetValueOrDefault())
                            isIdentity= true;

                       if (
                            !srcColumn.ColumnSize.Equals(destColumn.ColumnSize) || 
                            !srcColumn.NumericPrecision.Equals(destColumn.NumericPrecision) || 
                            !srcColumn.NumericScale.Equals(destColumn.NumericScale) || 
                            !srcColumn.DataTypeName.Equals(destColumn.DataTypeName) || 
                            !srcColumn.IsIdentity.Equals(destColumn.IsIdentity) 
                            )
                           break;
                    
                    }

                }

                rValue = true;
            }


            return rValue;
        }




    }
}
