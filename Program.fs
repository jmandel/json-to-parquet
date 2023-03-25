open Parquet
open System
open FhirParquet
open System.Text.Json
open Parquet.Rows
open Parquet.Schema
open System.IO

let fname = Environment.GetCommandLineArgs()[1]
let inferred = inferSchemaFromNdjsonFile (fname)
printInferredSchema inferred 0


let lines =
    File.ReadAllLines(fname)
    |> List.ofSeq
    |> List.map JsonDocument.Parse
    |> List.map (fun d -> d.RootElement)

let t = createParquetTableFromInferredSchema (inferred) (lines)

let fileStream =
    new FileStream("/tmp/ex.parquet", FileMode.Create, FileAccess.Write, FileShare.None)

let s =
    async {
        let! parquetWriter = ParquetWriter.CreateAsync(t.Schema, fileStream) |> Async.AwaitTask
        do! parquetWriter.WriteAsync(t) |> Async.AwaitTask
        parquetWriter.Dispose()
        return true
    }

let r = Async.RunSynchronously(s)
printfn $" T count {t.Count} --> {r}"
