using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;

namespace Viam.Parquet.Convert
{
    public class Log
    {
        private const string ALL = "ALL";
        public static string FileName = string.Empty;
        public static void Error(string message)
        {
            string path = ConfigurationManager.AppSettings["LogPath"];
         
            string completePath = $"{ConfigurationManager.AppSettings["LogPath"]}//{ConfigurationManager.AppSettings["LogName"]}";
            bool append = true;

            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
           
            using (StreamWriter sw = new StreamWriter(completePath, append))
            {
                sw.WriteLine($"{FileName} {DateTime.Now.ToString()} {message}");
                sw.Close();
            }

        }

        public static void Debug(string message)
        {
           
                Error(message);
        }
        private static int CountLinesInFile(string path)
        {
            int count = 0;
            using (StreamReader sw = new StreamReader(path))
            {
                string line;
                while ((line = sw.ReadLine()) != null)
                {
                    count++;
                }
            }
            return count;
        }
    }
}
