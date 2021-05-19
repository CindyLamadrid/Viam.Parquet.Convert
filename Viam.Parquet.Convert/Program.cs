using System;
using System.Collections.Generic;

namespace Viam.Parquet.Convert
{
    class Program
    {
        static void Main(string[] args)
        {
            string path =string.Empty;
            string delimiter = string.Empty;
            string parquetPath = string.Empty;
            string columnType= string.Empty;
            string countColumn = string.Empty;
            Log.FileName = path;

            if (args.Length!=0)
            {
               
                var parameters = args[0].Split("~");
              
                if (parameters.Length>0)
                {
                  
                    path = parameters[0];
                    delimiter = parameters[1];
                    parquetPath = parameters[2];
                    columnType = parameters[3];
                    countColumn = parameters[4];                
                }               
            }

            ParquetDynamic.CopyFromCsv(path, delimiter, parquetPath, int.Parse(countColumn), columnType);
          
        }
    }

   
}
