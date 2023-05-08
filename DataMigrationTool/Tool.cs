
using DataMigrationTool;
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
            int inputSourceId = 2; 



            try
            {
             
            string sourceConnectionString = "Data Source=.;Initial Catalog=Live;Integrated Security=True";
            string destinationConnectionString = "Data Source=.;Initial Catalog=ksa;Integrated Security=True";
                DataTable schema1 = GetTableSchema(sourceConnectionString, "test");
                DataTable schema2 = GetTableSchema(destinationConnectionString, "test");

               SchemaValidation schemaValidation = CompareTableSchema(schema1, schema2);

                string sourceQuery = "SELECT * FROM test WHERE id = @id"; // Use a parameter for the id
                string destinationQuery = "IF EXISTS (SELECT * FROM test WHERE id = @id) UPDATE test SET value = @value WHERE id = @id ELSE INSERT INTO test (id, value) VALUES (@id, @value)";

                if (schemaValidation.IsIdentity)
                {
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

                                     Dictionary<string,object> sourceDictionary = new Dictionary<string,object>();
                                     Dictionary<string,object> destinationDictionary = new Dictionary<string,object>();

                                        if (sourceDataReader.Read())
                                        {
                                            for (int i = 0; i < columnCount; i++)
                                                sourceDictionary.Add(sourceDataReader.GetName(i),sourceDataReader.GetValue(i));
                                        }

                                        destinationCommand.CommandText = "SELECT * FROM test WHERE "+ inputSourceColumn + "= @id";

                                        destinationCommand.Parameters.AddWithValue("@id", inputSourceId);

                                        using (SqlDataReader destinationDataReader = destinationCommand.ExecuteReader())
                                        {

                                            if (destinationDataReader.Read())
                                            {
                                                for (int i = 0; i < columnCount; i++)
                                                {
                                                    destinationDictionary.Add(destinationDataReader.GetName(i), destinationDataReader.GetValue(i));
                                                }
                                            }
                                        }
                                        StringBuilder destinationCommandText = new StringBuilder();
                                        if (destinationDictionary.Count == 0)
                                        {

                                        }
                                        else
                                        { 
                                            destinationCommandText.Append("UPDATE test set ");

                                        var keys = sourceDictionary.Keys.Where(key => sourceDictionary[key] != destinationDictionary[key]).ToList();

                                        for (int i = 0; i < keys.Count; i++)
                                        {
                                                if (keys[i] == inputSourceColumn)
                                                    continue;
                                            string key = keys[i];
                                            object value = sourceDictionary[key];
                                                
                                                    destinationCommandText.Append(key + " = @" + key);
                                                    destinationCommand.Parameters.AddWithValue("@"+ key, value);
                                            if (i < keys.Count - 1)
                                                destinationCommandText.Append(", ");
                                        }

                                        
                                        }

                                        

                                        destinationCommandText.Append(" where " + inputDestinationColumn + " = @id");
                                            
                                            destinationCommand.CommandText = destinationCommandText.ToString();
                                            


                                            int rowsAffected = destinationCommand.ExecuteNonQuery();

destinationCommand.Parameters.Clear();
                                            Console.WriteLine($"{rowsAffected} row inserted into DestinationTable based on matching id value.");

                                        }


                                    }
                                }

                            }


                        }

                }
                else
                {

                }






            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }

        static DataTable GetTableSchema(string connectionString, string tableName)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand command = new SqlCommand("SELECT * FROM " + tableName, connection);
                SqlDataReader reader = command.ExecuteReader(CommandBehavior.SchemaOnly);
                DataTable schema = reader.GetSchemaTable();
                reader.Close();
                return schema;
            }
        }

        static SchemaValidation CompareTableSchema(DataTable schema1, DataTable schema2)
        {
            SchemaValidation schemaValidation = new SchemaValidation();

           List<string> sourceColumns  = schema1.AsEnumerable().Select(row => Convert.ToString(row["ColumnName"])).ToList();

           List<string> destinationColumns  = schema2.AsEnumerable().Select(row => Convert.ToString(row["ColumnName"])).ToList();

            schemaValidation.IsSameColumnns = sourceColumns.SequenceEqual(destinationColumns);

           List<string> sourceDatatypes = schema1.AsEnumerable().Select(row => Convert.ToString(row["DataType"])).ToList();

           List<string> destinationDataypes = schema2.AsEnumerable().Select(row => Convert.ToString(row["DataType"])).ToList();

            schemaValidation.IsSameDatatypes = sourceDatatypes.SequenceEqual(destinationDataypes);

            List<string> sourceIdentity = schema1.AsEnumerable().Select(row => Convert.ToString(row["IsIdentity"])).ToList();

            List<string> destinationIdentity = schema1.AsEnumerable().Select(row => Convert.ToString(row["IsIdentity"])).ToList();

            schemaValidation.IsIdentity = sourceIdentity.SequenceEqual(destinationIdentity);

            return schemaValidation;
        }
   

    }
}
