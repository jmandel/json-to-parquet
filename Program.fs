open Parquet
open System
open FhirParquet
open System.Text.Json
open System.IO

let infile = Environment.GetCommandLineArgs()[1]
let outfile = Environment.GetCommandLineArgs()[2]

let lines =
    File.ReadAllLines(infile)
    |> List.ofSeq
    |> List.map JsonDocument.Parse
    |> List.map (fun d -> d.RootElement)


let r =
    infile
    |> inferSchemaFromNdjsonFile
    |> createParquetTable
    |> populateParquetTable lines
    |> writeParquetTableToFile outfile

printfn $"Result: {r}"
