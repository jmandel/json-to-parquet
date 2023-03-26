open Parquet
open System
open FhirParquet
open System.Text.Json
open System.IO

let infile = Environment.GetCommandLineArgs()[1]
let outfile = Environment.GetCommandLineArgs()[2]

let lines =
    File.ReadLines(infile) // Lazily read lines from the file
    |> Seq.map JsonDocument.Parse
    |> Seq.map (fun d -> d.RootElement)

let r =
    infile
    |> inferSchemaFromNdjsonFile
    |> createParquetTable
    |> populateParquetTable lines
    |> writeParquetTableToFile outfile

printfn $"Result: {r}"
