/*
 * Copyright (c) 2023 Gustavo Santos
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */


using System;

namespace SQLBulkInsert
{
    using System.Data;
    using System.Data.SqlClient;

    internal class Program
    {
        private static void Main()
        {
            string connectionString = GetConnectionStringFromUser();

            if (TestDatabaseConnection(connectionString))
            {
                Console.WriteLine("Connection to the database successful.");

                int totalRows = GetTotalRowsFromUser();

                Generate(connectionString, totalRows);
                //CopyFromTableAtoTableB();
            }
            else
            {
                Console.WriteLine("Connection to the database failed. Exiting the application.");
            }
        }

        private static string GetConnectionStringFromUser()
        {
            Console.Write("Enter database connection string: ");
            return Console.ReadLine();
        }


        private static bool TestDatabaseConnection(string connectionString)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database connection test failed: {ex.Message}");
                return false;
            }
        }

        private static int GetTotalRowsFromUser()
        {
            Console.Write("Enter the total number of rows to generate: ");
            int totalRows;
            while (!int.TryParse(Console.ReadLine(), out totalRows) || totalRows <= 0)
            {
                Console.Write("Invalid input. Please enter a valid positive integer: ");
            }
            return totalRows;
        }

        private static void Generate(string connectionString, int totalRows)
        {
            int batchSize = 10000; // Set your desired batch size

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                int totalBatches = (int)Math.Ceiling((double)totalRows / batchSize);

                for (int batchNumber = 1; batchNumber <= totalBatches; batchNumber++)
                {
                    //TODO table is not dynamic
                    DataTable tbl = GenerateDataTable(batchSize);

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.ColumnMappings.Add("RecipientId", "RecipientId");
                        bulkCopy.ColumnMappings.Add("RecipientEmail", "RecipientEmail");
                        bulkCopy.ColumnMappings.Add("Subject", "Subject");
                        bulkCopy.ColumnMappings.Add("Content", "Content");
                        bulkCopy.ColumnMappings.Add("Status", "Status");
                        bulkCopy.ColumnMappings.Add("CreationDateTime", "CreationDateTime");

                        try
                        {
                            bulkCopy.DestinationTableName = "EmailMessage";
                            bulkCopy.WriteToServer(tbl);

                            Console.WriteLine($"Batch {batchNumber} completed successfully.");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error in batch {batchNumber}: {ex.Message}");
                        }
                    }
                }

                connection.Close();
            }

            Console.WriteLine("All batches completed. Press Enter to finish.");
            Console.ReadLine();
        }

        private static DataTable GenerateDataTable(int batchSize)
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("RecipientId", typeof(int)));
            tbl.Columns.Add(new DataColumn("RecipientEmail", typeof(string)));
            tbl.Columns.Add(new DataColumn("Subject", typeof(string)));
            tbl.Columns.Add(new DataColumn("Content", typeof(string)));
            tbl.Columns.Add(new DataColumn("Status", typeof(int)));
            tbl.Columns.Add(new DataColumn("CreationDateTime", typeof(DateTime)));

            DateTime endDate = DateTime.Now;
            DateTime startDate = DateTime.Now.AddYears(-2);

            Random random = new Random();

            for (int i = 0; i < batchSize; i++)
            {
                DataRow dr = tbl.NewRow();
                dr["RecipientId"] = 1;
                dr["RecipientEmail"] = "dummy@domain.com";
                dr["Subject"] = $"Dummy Subject #{i}";
                dr["Content"] = $"Dummy Content <strong>#{i}</strong>";
                dr["Status"] = 2;
                dr["CreationDateTime"] = startDate + TimeSpan.FromDays(random.NextDouble() * (endDate - startDate).TotalDays);

                tbl.Rows.Add(dr);
            }

            return tbl;
        }

        private static void CopyFromTableAtoTableB(string connectionString)
        {
            // Open a sourceConnection to the AdventureWorks database.
            using (SqlConnection sourceConnection =
                       new SqlConnection(connectionString))
            {
                sourceConnection.Open();

                // Perform an initial count on the destination table.
                SqlCommand commandRowCount = new SqlCommand(
                    "SELECT COUNT(*) FROM " +
                    "dbo.EmailMessage;",
                    sourceConnection);
                long countStart = System.Convert.ToInt32(
                    commandRowCount.ExecuteScalar());
                Console.WriteLine("Starting row count = {0}", countStart);

                // Get data from the source table as a SqlDataReader.
                SqlCommand commandSourceData = new SqlCommand(
                    "SELECT ProductID, Name, " +
                    "ProductNumber " +
                    "FROM Production.Product;", sourceConnection);
                SqlDataReader reader =
                    commandSourceData.ExecuteReader();

                // Open the destination connection. In the real world you would
                // not use SqlBulkCopy to move data from one table to the other
                // in the same database. This is for demonstration purposes only.
                using (SqlConnection destinationConnection =
                           new SqlConnection(connectionString))
                {
                    destinationConnection.Open();

                    // Set up the bulk copy object.
                    // Note that the column positions in the source
                    // data reader match the column positions in
                    // the destination table so there is no need to
                    // map columns.
                    using (SqlBulkCopy bulkCopy =
                               new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy.DestinationTableName =
                            "dbo.BulkCopyDemoMatchingColumns";

                        try
                        {
                            // Write from the source to the destination.
                            bulkCopy.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        finally
                        {
                            // Close the SqlDataReader. The SqlBulkCopy
                            // object is automatically closed at the end
                            // of the using block.
                            reader.Close();
                        }
                    }

                    // Perform a final count on the destination
                    // table to see how many rows were added.
                    long countEnd = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());
                    Console.WriteLine("Ending row count = {0}", countEnd);
                    Console.WriteLine("{0} rows were added.", countEnd - countStart);
                    Console.WriteLine("Press Enter to finish.");
                    Console.ReadLine();
                }
            }
        }
    }
}