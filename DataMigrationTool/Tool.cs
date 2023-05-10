
using DataMigrationTool;
using Microsoft.Extensions.Primitives;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Reflection.PortableExecutable;
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
            int inputSourceId = 3; 



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

                        sourceCommand.CommandText = "SELECT * FROM test WHERE Id = @id";

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

                                    Dictionary<string, object> sourceDictionary = new Dictionary<string, object>();
                                    Dictionary<string, object> destinationDictionary = new Dictionary<string, object>();

                                    if (sourceDataReader.Read())
                                    {
                                        for (int i = 0; i < columnCount; i++)
                                            sourceDictionary.Add(sourceDataReader.GetName(i), sourceDataReader.GetValue(i));
                                    }

                                    destinationCommand.CommandText = "SELECT * FROM test WHERE " + inputSourceColumn + "= @id";

                                    destinationCommand.Parameters.AddWithValue("@id", inputSourceId);

                                    using (SqlDataReader destinationDataReader = destinationCommand.ExecuteReader())
                                    {

                                        StringBuilder destinationCommandText = new StringBuilder();
                                        if(IsSchemaMatches(sourceDataReader,destinationDataReader))
                                        {

                                            



                                        }
                                        

                                        if (destinationDataReader.Read())
                                        {
                                            for (int i = 0; i < columnCount; i++)
                                            {
                                                destinationDictionary.Add(destinationDataReader.GetName(i), destinationDataReader.GetValue(i));
                                            }
                                            destinationCommandText.Append("UPDATE test set ");

                                            var keys = sourceDictionary.Keys.Where(key => sourceDictionary[key] != destinationDictionary[key]).ToList();

                                            for (int i = 0; i < keys.Count; i++)
                                            {
                                                if (keys[i] == inputSourceColumn)
                                                    continue;
                                                string key = keys[i];
                                                object value = sourceDictionary[key];

                                                destinationCommandText.Append(key + " = @" + key);
                                                destinationCommand.Parameters.AddWithValue("@" + key, value);
                                                if (i < keys.Count - 1)
                                                    destinationCommandText.Append(", ");
                                            }

                                            destinationCommandText.Append(" where " + inputDestinationColumn + " = @id");

                                            destinationCommand.CommandText = destinationCommandText.ToString();
                                        }
                                        else
                                        {

                                            List<string> columnNames = new List<string>();
                                            List<string> parameterNames = new List<string>();
                                            for (int i = 0; i < columnCount; i++)
                                            {
                                                columnNames.Add(sourceDataReader.GetName(i));
                                                parameterNames.Add("@p" + i);
                                            }

                                            destinationCommandText.Append("Insert into ksa (");
                                            destinationCommandText.Append(string.Join(", ", columnNames));
                                            destinationCommandText.Append(") values (");
                                            destinationCommandText.Append(string.Join(", ", parameterNames));
                                            destinationCommandText.Append(");");

                                            for (int i = 0; i < columnCount; i++)
                                            {
                                                destinationCommand.Parameters.AddWithValue(parameterNames[i], sourceDataReader.GetValue(i));
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
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

     
        static bool IsSchemaMatches(SqlDataReader sourceDataReader,SqlDataReader destinationDataReader)
        {


            DataView sourceView = new DataView(sourceDataReader.GetSchemaTable());
            sourceView.Sort = "ColumnName";
            DataTable sortedSourceTable = sourceView.ToTable();

            DataView destView = new DataView(destinationDataReader.GetSchemaTable());
            destView.Sort = "ColumnName";
            DataTable sortedDestTable = destView.ToTable();

            return true;
        }

   

    }
}
