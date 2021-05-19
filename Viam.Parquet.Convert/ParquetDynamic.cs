using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Viam.Parquet.Convert
{
    /// <summary>
    /// Create parquet file dynamically from csv.
    /// </summary>
    public class ParquetDynamic
    {
        /// <summary>
        /// Create dictionary for save csv information
        /// </summary>
        /// <param name="dictionary">new dictionary created</param>
        /// <param name="columns">Quantity of columns for get information.</param>
        public static void CreateDictionary(Dictionary<string, List<string>> dictionary, int columns)
        {
            try
            {
                for (int i = 0; i < columns; i++)
                    {
                        dictionary.Add(i.ToString(), new List<string>());
                    }
            }
            catch (Exception ex)
            {
                Log.Error($"ERROR CreateDictionary: {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Get header.
        /// </summary>
        /// <param name="delimiter"></param>
        /// <param name="pathCsv"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        public static string[] GetHeaderFromCsv(string delimiter, string pathCsv,int columns)
        {
            try
            {
                short i = 0;
                string[] values = new string[columns];
                using (var csvReader = new StreamReader(File.OpenRead(pathCsv)))
                {
                    while (!csvReader.EndOfStream && i < 1)
                    {
                        var line = csvReader.ReadLine();
                        values = line.Split(delimiter);

                        i++;
                    }
                }

                return values;
            }catch(Exception ex)
            {
                Log.Error($"ERROR GetHeaderFromCsv: {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Open Csv file and save into parquet file
        /// </summary>
        /// <param name="pathCsv">Cvs path.</param>
        /// <param name="delimiter">Delimiter for Csv file.</param>
        /// <param name="parquetPath">Parquet path.</param>
        /// <param name="columns">Quantity of columns for get information.</param>
        public static void CopyFromCsv(string pathCsv, string delimiter, string parquetPath,int columns,string columnType)
        {
            try
            {
                
                Dictionary<string, List<string>> dictionary = new Dictionary<string, List<string>>();
                CreateDictionary(dictionary, columns);
                var typesColumn = columnType.Split(';');
                var typesArray = new string[typesColumn.Length];
                var columnsArray = new string[typesColumn.Length];
                int i = 0;
                bool isCsvError = false;

                foreach (string cType in typesColumn)
                {
                    var newType = cType.Split(":");
                    if (newType.Length > 0)
                    {
                        typesArray[i] = newType[1];
                        columnsArray[i] = newType[0];
                        i++;
                    }
                }

                i = 0;
                int row = 2;
                using (var csvReader = new StreamReader(File.OpenRead(pathCsv)))
                {
                   
                    while (!csvReader.EndOfStream)
                    {
                        var line = csvReader.ReadLine();
                        if (i != 0)
                        {

                            string[] values = line.Split(delimiter);

                            if (values.Length == columns)
                            {
                                GetModelArray(values, dictionary, columns, typesArray, columnsArray, row);
                            }else
                            {
                                isCsvError = true;
                            }

                            row++;
                        }
                        i++;
                    }
                }

                if (isCsvError)
                {
                    Log.Error($"Bad column number in Csv file");
                    throw new Exception("Bad column number in Csv file");
                }
                   var header = GetHeaderFromCsv(delimiter, pathCsv, columns);
                if (File.Exists(parquetPath))
                {
                    Append(dictionary, columns, parquetPath, true, header, typesArray, columnsArray);
                }
                else
                {
                    Writer(dictionary, columns, parquetPath, header, typesArray, columnsArray);
                }
            }catch(Exception ex)
            {
                Log.Error( $"ERROR CopyFromCsv: {ex}");
                // Console.WriteLine("ERROR");
                throw ex;
            }
        }

        /// <summary>
        /// Add new lines to parquet file. Get file stream.
        /// </summary>
        /// <param name="dictionary">dictionary whit data.</param>
        /// <param name="columns">Quantitry columns.</param>
        /// <param name="parquetPath">Parquet path</param>
        /// <param name="isAppend">new line.</param>
        public static void Append(Dictionary<string, List<string>> dictionary, int columns, string parquetPath, bool isAppend, string[] header, string[] typesArray, string[] columnsArray)
        {
            try
            {
                using var fileStream = new FileStream(parquetPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                GenerateParquet(dictionary, columns, fileStream, isAppend, header, typesArray, columnsArray);
            }catch(Exception ex)
            {
                Log.Error($"ERROR Append: {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Create new file. Get file stream.
        /// </summary>
        /// <param name="dictionary">dictionary whit data.</param>
        /// <param name="columns">Quantitry columns.</param>
        /// <param name="parquetPath">Parquet path</param>
        public static void Writer(Dictionary<string, List<string>> dictionary, int columns, string parquetPath, string[] header, string[] typesArray, string[]  columnsArray)
        {
            using var fileStream = new FileStream(parquetPath, FileMode.Create, FileAccess.Write, FileShare.Write);
            GenerateParquet(dictionary, columns, fileStream, false, header,typesArray, columnsArray);
        }

        public static int GetIndexTypesArray(string[] columnsArray, string column) {
            try
            {
                int index = -1;
                if (columnsArray.Contains(column))
                { int i = 0;
                    foreach (var type in columnsArray)
                    { 
                        if(type == column.ToString())
                        {
                            index = i;
                        }
                        i++;
                    }
                }
            return index;
            }
            catch (Exception ex)
            {
                Log.Error($"ERROR GetIndexTypesArray: {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Generate parquet file.
        /// </summary>
        /// <param name="dictionary">dictionary whit data.</param>
        /// <param name="columns">Quantitry columns.</param>
        /// <param name="parquetPath">Parquet path</param>
        /// <param name="isAppend">new line.</param>
        /// TODO: Add new database type. Example: money, bit,decimal, numeric, etc.
        public static void GenerateParquet(Dictionary<string, List<string>> dictionary,int columns,FileStream fileStream, bool isAppend,string [] header, string[] typesArray, string[] columnsArray)
        {
            try 
            { 

                    var dataColumnsArray = new DataColumn[dictionary.Count];
                    var type = string.Empty;
                    List<int> newListInt = new List<int>();
                    List<Double> newListDecimal = new List<Double>();
                    List<DateTimeOffset> newListDatetime = new List<DateTimeOffset>();
                    for (int i = 0; i < columns; i++)
                    {
                        var index = GetIndexTypesArray(columnsArray, i.ToString());
                        DataColumn dataColumns = null;
                        type = string.Empty;
                        newListInt = new List<int>();
                        newListDatetime = new List<DateTimeOffset>();
                        newListDecimal = new List<double>();
                        if (index !=-1)
                        {
                             type = typesArray[index];
                        }

                        if (type == "int")
                        {
                            try
                            {
                                newListInt = dictionary[i.ToString()].Select(int.Parse).ToList();
                            }
                            catch(Exception ex)
                            {
                                Log.Error($"ERROR GenerateParquet type Int Column:{i.ToString()} : {ex} ");
                                throw ex;
                            }
                           
                        }

                    if (type == "num")
                    {
                        try
                        {
                            newListDecimal = dictionary[i.ToString()].Select(Double.Parse).ToList();
                        }
                        catch (Exception ex)
                        {
                            Log.Error($"ERROR GenerateParquet type Numeric Column:{i.ToString()} : {ex} ");
                            throw ex;
                        }

                    }



                    if (type == "dt")
                        {
                            try
                            {
                     
                               newListDatetime = dictionary[i.ToString()].Select(DateTimeOffset.Parse).ToList();
                            }
                            catch (Exception ex)
                            {
                                Log.Error($"ERROR GenerateParquet type DateTime Column:{i.ToString()} : {ex} ");
                                throw ex;
                            }
                    }
 
                        if(newListInt.Count>0)
                        {
                            dataColumns = new DataColumn(
                                new DataField<int>(header[i]),
                                newListInt.ToArray());
                        }
                        else if(newListDatetime.Count>0)
                        {

                                dataColumns = new DataColumn(
                                  new DataField<DateTimeOffset>(header[i]),
                                  newListDatetime.ToArray());
                         }
                        else if (newListDecimal.Count > 0)
                        {

                            dataColumns = new DataColumn(
                              new DataField<Double>(header[i]),
                              newListDecimal.ToArray());
                        }
                    else
                        {
                            dataColumns = new DataColumn(
                                new DataField<string>(header[i]),
                                dictionary[i.ToString()].ToArray());
                        }
               
                        dataColumnsArray[i] = dataColumns;

                    }
                    var columnDataFields = dataColumnsArray.Select(s => s.Field).ToArray();
                    var schema = new Schema(columnDataFields);            

           
                    using var parquetWriter = new ParquetWriter(schema, fileStream, append: isAppend);
                    using var groupWriter = parquetWriter.CreateRowGroup();

                    for (int i = 0; i < dataColumnsArray.Length; i++)
                    {
                        groupWriter.WriteColumn(dataColumnsArray[i]);
                    }
            }
            catch (Exception ex)
            {
                Log.Error($"ERROR GenerateParquet: {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Convert list to array to get datacolumns and schema for parquet file.
        /// </summary>
        /// <param name="values">lines from csv.</param>
        /// <param name="dictionary">dictionary with arrays.</param>
        /// <param name="columns">Quantity columns</param>
        public static void GetModelArray(string[] values, Dictionary<string, List<string>> dictionary,int columns,string [] typesArray,string [] columnsArray,int row)
        {
            try
            {
                bool isError = false;

                for (int i = 0; i < columns; i++)
                {
                    List<string> list;
                    dictionary.TryGetValue(i.ToString(), out list);
                    if (i < values.Length)
                    {
                        var index = GetIndexTypesArray(columnsArray, i.ToString());
                        string type = string.Empty;
                        if (index != -1)
                        {
                            type = typesArray[index];
                        }
                        if (type == "dt")
                        {
                            var hour = new DateTimeOffset(DateTime.Parse(values[i])).Offset.Hours;                          

                            DateTime originalDate = DateTime.Parse(values[i]);
                            var newDate =originalDate.AddHours(hour);
                            list.Add(newDate.ToString());
                        }else if(type == "num" || type == "int")
                        {
                            try
                            {
                                if(type == "num")
                                {
                                    var con = double.Parse(values[i]);
                                }else
                                    {
                                    var ent = int.Parse(values[i]);
                                } 
                                  
                            }
                            catch(Exception ex)
                            {
                                Log.Error($"ERROR GenerateParquet Comversion double or int row: {row.ToString()}: {ex}");
                                isError = true;

                            }

                            list.Add(values[i]==""?"0": values[i]);
                        }
                        else
                        {
                            list.Add(values[i]);
                        }
                    }
                    dictionary[i.ToString()] = list;
                }
                if(isError)
                {
                    throw new Exception("ERROR GetModelArray Conversions");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"ERROR GetModelArray: {ex}");
                throw ex;
            }
        }
    }
}
