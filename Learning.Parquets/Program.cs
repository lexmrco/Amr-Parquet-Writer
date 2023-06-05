using Parquet;
using Parquet.Data;
using System;
using System.IO;

namespace Learning.Parquets
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteParquet1();
            //WriteParquet2();
        }

        private static void WriteParquet1()
        {
            DateTime createdAt = DateTime.Now;
            string createdAtStr = string.Format("{0:yyyy-MM-ddTHH:mm:ss.fffZ}", createdAt);
            DataSet dataset = new DataSet(
               new DataField<string>("created-at"),
               new DataField<string>("api-key"),
               new DataField<string>("action"),
               new DataField<string>("table-name"),
               new DataField<string>("record-id"),
               new DataField<string>("company-id"),
               new DataField<string>("user"),
               new DataField<string>("payload"),
               new DataField<int>("year"),
               new DataField<int>("month"),
               new DataField<int>("day"))
            {
                {
                    createdAtStr,
                    "connectin-backend",
                    "CREATE",
                    "public.accounts",
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    null,
                    createdAt.Year,
                    createdAt.Month,
                    createdAt.Day
                }
            };
            string path = @"C:\temp\sample.parquet";
            WriteFile(dataset, path);
            Parquet.Data.DataSet dsAll = ParquetReader.ReadFile(path);
            int count = dsAll.Count;

        }
        private static void WriteParquet2()
        {
            var file = @"C:\temp\sample2.parquet";
            var ds1 = new Parquet.Data.DataSet(new DataField<int>("id"));
            ds1.Add(1);
            ds1.Add(2);
            WriteFile(ds1, file);

            //append to file
            var ds2 = new Parquet.Data.DataSet(new DataField<int>("id"));
            ds2.Add(3);
            ds2.Add(4);
            WriteFile(ds2, file);

            Parquet.Data.DataSet dsAll = ParquetReader.ReadFile(file);
            int count = dsAll.Count;            
        }

        public static void WriteFile(DataSet dataSet, string fileFullPath, CompressionMethod compression = CompressionMethod.Gzip, ParquetOptions formatOptions = null, WriterOptions writerOptions = null)
        {
            bool exists = File.Exists(fileFullPath);

            using Stream fs = exists ? System.IO.File.Open(fileFullPath, FileMode.Open) : System.IO.File.Create(fileFullPath);
            using var writer = new ParquetWriter(fs, formatOptions, writerOptions);
            writer.Write(dataSet, compression, append: exists);
        }
    }
}
