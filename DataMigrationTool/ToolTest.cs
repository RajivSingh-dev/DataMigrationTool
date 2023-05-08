using System;
using System.Data;
using System.Data.SqlClient;

namespace ConsoleApp
{
    class ToolTest
    {
        static void Test(string[] args)
        {
            Console.WriteLine("Enter the source table name:");
            string inputSourceTable = Console.ReadLine();
            Console.WriteLine("Enter the destination table name:");
            string inputDestinationTable = Console.ReadLine();
            Console.WriteLine("Enter the source column name:");
            string inputSourceColumn = Console.ReadLine();
            Console.WriteLine("Enter the destination column name:");
            string inputDestinationColumn = Console.ReadLine();
            Console.WriteLine("Enter the source ID:");
            string inputSourceId = Console.ReadLine();
            Console.WriteLine("Enter the destination ID:");
            string inputDestinationId = Console.ReadLine();

            string connectionString = "Data Source=.;Initial Catalog=TestDB;Integrated Security=True";

            string insertDataCommand = $@"
                INSERT INTO {inputDestinationTable} ({inputDestinationColumn})
                SELECT {inputSourceColumn}
                FROM {inputSourceTable}
                WHERE {inputSourceId} = {inputDestinationId}";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                DataTable sourceSchema = connection.GetSchema("Columns", new string[] { null, null, inputSourceTable });

                DataTable destinationSchema = connection.GetSchema("Columns", new string[] { null, null, inputDestinationTable });

                if (sourceSchema.Rows.Count > 0 && destinationSchema.Rows.Count > 0)
                {
                    DataRow sourceRow = sourceSchema.Select($"COLUMN_NAME = '{inputSourceColumn}'").FirstOrDefault();

                    DataRow destinationRow = destinationSchema.Select($"COLUMN_NAME = '{inputDestinationColumn}'").FirstOrDefault();

                    if (sourceRow != null && destinationRow != null)
                    {
                        string sourceType = sourceRow["DATA_TYPE"].ToString();

                        string destinationType = destinationRow["DATA_TYPE"].ToString();

                        if (sourceType == destinationType)
                        {
                            bool destinationIdentity = (bool)destinationRow["IsIdentity"];

                            if (!destinationIdentity)
                            {
                                using (SqlCommand command = new SqlCommand(insertDataCommand, connection))
                                {
                                    int rowsAffected = command.ExecuteNonQuery();

                                    Console.WriteLine($"Successfully inserted {rowsAffected} row(s) from {inputSourceTable}.{inputSourceColumn} to {inputDestinationTable}.{inputDestinationColumn}.");
                                }
                            }
                            else
                            {
                                Console.WriteLine($"Cannot insert data into {inputDestinationTable}.{inputDestinationColumn} because it is an identity column.");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Cannot insert data from {inputSourceTable}.{inputSourceColumn} to {inputDestinationTable}.{inputDestinationColumn} because they have different data types.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Cannot find {inputSourceTable}.{inputSourceColumn} or {inputDestinationTable}.{inputDestinationColumn} in the database.");
                    }
                }
                else
                {
                    Console.WriteLine($"Cannot find {inputSourceTable} or {inputDestinationTable} in the database.");
                }

                connection.Close();
            }
        }
    }
}
