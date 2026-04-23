using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using NCalc;

namespace DynamicCalcEngine
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string connectionString = @"Server=DESKTOP-4G5A9E;Database=CalculationProject;Trusted_Connection=True;TrustServerCertificate=True;";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("Connection successful. Loading data...");

                    // Load data table (values to calculate on)
                    DataTable dataTable = new DataTable();
                    using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT data_id, a, b, c, d FROM t_data", connection))
                    {
                        adapter.Fill(dataTable);
                    }

                    // Load formulas table
                    DataTable targilTable = new DataTable();
                    using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT targil_id, targil, tnai, targil_false FROM t_targil", connection))
                    {
                        adapter.Fill(targilTable);
                    }

                    // Loop over each formula
                    foreach (DataRow targilRow in targilTable.Rows)
                    {
                        int tId = Convert.ToInt32(targilRow["targil_id"]);
                        string formula = targilRow["targil"].ToString();
                        string tnai = targilRow["tnai"]?.ToString();
                        string tFalse = targilRow["targil_false"]?.ToString();

                        // Temp table to store results before bulk insert
                        DataTable resultsToInsert = new DataTable();
                        resultsToInsert.Columns.Add("data_id", typeof(int));
                        resultsToInsert.Columns.Add("targil_id", typeof(int));
                        resultsToInsert.Columns.Add("method", typeof(string));
                        resultsToInsert.Columns.Add("result", typeof(double));

                        Console.WriteLine($"Calculating Formula {tId}...");
                        Stopwatch sw = Stopwatch.StartNew();

                        // Loop over each data row
                        foreach (DataRow row in dataTable.Rows)
                        {
                            // Build final expression (with optional condition)
                            string finalExpr = formula;
                            if (!string.IsNullOrEmpty(tnai))
                            {
                                finalExpr = $"if({tnai}, {formula}, {tFalse})";
                            }

                            // Normalize function names to match NCalc
                            finalExpr = finalExpr.Replace("POWER", "Pow", StringComparison.OrdinalIgnoreCase)
                                                 .Replace("SQRT", "Sqrt", StringComparison.OrdinalIgnoreCase)
                                                 .Replace("ABS", "Abs", StringComparison.OrdinalIgnoreCase)
                                                 .Replace("ROUND", "Round", StringComparison.OrdinalIgnoreCase)
                                                 .Replace("EXP", "Exp", StringComparison.OrdinalIgnoreCase)
                                                 .Replace("LOG", "Log", StringComparison.OrdinalIgnoreCase);

                            // Create expression and inject parameters
                            Expression e = new Expression(finalExpr);

                            e.Parameters["a"] = row["a"];
                            e.Parameters["b"] = row["b"];
                            e.Parameters["c"] = row["c"];
                            e.Parameters["d"] = row["d"];

                            try
                            {
                                // Evaluate expression
                                double calcResult = Convert.ToDouble(e.Evaluate());
                                // Add to results table
                                resultsToInsert.Rows.Add(row["data_id"], tId, "C#", calcResult);
                            }
                            catch (Exception ex)
                            {
                                // Log error and stop current formula
                                Console.WriteLine($"Error evaluating formula {tId}: {ex.Message}. Expression: {finalExpr}");
                                break;
                            }
                        }

                        // Bulk insert results into DB
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = "t_results";
                            bulkCopy.ColumnMappings.Add("data_id", "data_id");
                            bulkCopy.ColumnMappings.Add("targil_id", "targil_id");
                            bulkCopy.ColumnMappings.Add("method", "method");
                            bulkCopy.ColumnMappings.Add("result", "result");

                            bulkCopy.WriteToServer(resultsToInsert);
                        }

                        // Save execution time
                        sw.Stop();
                        double runTime = sw.Elapsed.TotalSeconds;

                        string logQuery = "INSERT INTO t_log (targil_id, method, run_time) VALUES (@tid, 'C#', @time)";
                        using (SqlCommand logCmd = new SqlCommand(logQuery, connection))
                        {
                            logCmd.Parameters.AddWithValue("@tid", tId);
                            logCmd.Parameters.AddWithValue("@time", runTime);
                            logCmd.ExecuteNonQuery();
                        }

                        Console.WriteLine($"Formula {tId} completed in {runTime:F4} seconds.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }

            Console.WriteLine("Process finished. Press any key to exit.");
            Console.ReadKey();
        }
    }
}