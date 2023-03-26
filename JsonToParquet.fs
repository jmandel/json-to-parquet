module FhirParquet
open Parquet.Schema

open System.Text.Json
open System.IO
open Parquet.Rows
open Parquet

type SchemaType =
    | JsonObject of InferredKVs
    | JsonArray of InferredElts
    | JsonNumber
    | JsonBoolean
    | JsonString
    | JsonNull
    | JsonUnknown

and InferredKVs = Map<string, InferredSchema>
and InferredElts = InferredSchema
and InferredSchema = { Type: SchemaType; Optional: bool }

// Function to recursively print an InferredSchema with depth-based indentation
let rec printInferredSchema (schema: InferredSchema) (depth: int) =
    let indent = String.replicate depth "  "

    printfn $"{indent}Optional? {schema.Optional}"

    match schema.Type with
    | JsonObject kvs ->
        kvs
        |> Map.iter (fun key value ->
            printfn "%sKey: %s" indent key
            printfn "%sValue: " indent
            printInferredSchema value (depth + 1))
    | JsonArray elt ->
        printfn "%sJsonArray of:" indent
        printInferredSchema elt (depth + 1)
    | JsonNumber -> printfn "%sJsonNumber" indent
    | JsonBoolean -> printfn "%sJsonBoolean" indent
    | JsonString -> printfn "%sJsonString" indent
    | _ -> printfn "%sOther" indent

let rec createInferredSchema (element: JsonElement) : InferredSchema =
    let schemaType =
        match element.ValueKind with
        | JsonValueKind.Object ->
            let kvs =
                element.EnumerateObject()
                |> Seq.fold
                    (fun map prop ->
                        let propName = prop.Name
                        let propValue = prop.Value
                        let inferredSchema = createInferredSchema propValue

                        map
                        |> Map.tryFind propName
                        |> function
                            | Some existingSchema ->
                                let mergedSchema = mergeInferredSchema existingSchema inferredSchema
                                Map.add propName mergedSchema map
                            | None -> Map.add propName inferredSchema map)
                    Map.empty

            JsonObject kvs
        | JsonValueKind.Array ->
            let inferredElts =
                element.EnumerateArray()
                |> Seq.fold
                    (fun currentSchema elem ->
                        let elemSchema = createInferredSchema elem
                        mergeInferredSchema currentSchema elemSchema)
                    { Type = JsonUnknown; Optional = false }

            JsonArray inferredElts
        | JsonValueKind.Number -> JsonNumber
        | JsonValueKind.String -> JsonString
        | JsonValueKind.True
        | JsonValueKind.False -> JsonBoolean
        | _ -> JsonNull

    { Type = schemaType; Optional = false }


and mergeInferredSchema (schema1: InferredSchema) (schema2: InferredSchema) : InferredSchema =
    let mergedType, mergedOptional =
        match schema1.Type, schema2.Type with
        | JsonObject kvs1, JsonObject kvs2 ->
            let mergedKvs =
                kvs1
                |> Map.fold
                    (fun map key value ->
                        match map |> Map.tryFind key with
                        | Some value2 ->
                            let mergedValue = mergeInferredSchema value value2
                            Map.add key mergedValue map
                        | None -> Map.add key value map)
                    kvs2
                |> Map.fold
                    (fun map key value ->
                        match kvs1.ContainsKey key && kvs2.ContainsKey key with
                        | true -> Map.add key value map
                        | false -> Map.add key { Optional = true; Type = value.Type } map)
                    Map.empty

            JsonObject mergedKvs, schema1.Optional || schema2.Optional
        | JsonArray elts1, JsonArray elts2 ->
            let mergedElts = mergeInferredSchema elts1 elts2
            JsonArray mergedElts, schema1.Optional || schema2.Optional
        | JsonUnknown, other -> other, schema2.Optional
        | other, JsonUnknown -> other, schema1.Optional
        | _ when schema1.Type = JsonNull -> schema2.Type, true
        | _ when schema2.Type = JsonNull -> schema1.Type, true
        | _ when schema1.Type = schema2.Type -> schema1.Type, schema1.Optional || schema2.Optional
        | _ -> failwith (sprintf "Incompatible InferredSchema: %A and %A" schema1 schema2)

    { Type = mergedType
      Optional = mergedOptional }


let inferSchemaFromFile (filePath: string) : InferredSchema =
    let jsonString = File.ReadAllText filePath
    let document = JsonDocument.Parse(jsonString)
    createInferredSchema (document.RootElement)

let iss = inferSchemaFromFile ("example.json")
printInferredSchema iss 0

let inferSchemaFromSeq (lines: seq<string>) : InferredSchema =
    let rec loop (currentSchema: InferredSchema) (remainingLines: seq<string>) =
        match Seq.tryHead remainingLines with
        | None -> currentSchema
        | Some line ->
            let jsonElement = JsonDocument.Parse(line).RootElement
            let schemaForLine = createInferredSchema jsonElement
            let updatedSchema = mergeInferredSchema currentSchema schemaForLine
            loop updatedSchema (Seq.skip 1 remainingLines)

    loop { Type = JsonUnknown; Optional = false } lines

let readLinesLazily (filePath: string) : seq<string> =
    seq {
        use reader = new StreamReader(filePath)
        while not reader.EndOfStream do
            yield reader.ReadLine()
    }

let inferSchemaFromNdjsonFile (filePath: string) : InferredSchema =
    filePath
    |> readLinesLazily
    |> inferSchemaFromSeq

let rec convertInferredSchemaToField (fieldName: string) (inferredSchema: InferredSchema) : Field =
    let r =
        match inferredSchema.Type with
        | JsonObject kvs ->
            let fields =
                kvs |> Map.map convertInferredSchemaToField |> Map.toList |> List.map snd
            StructField(fieldName, Array.ofList fields) :> Field
        | JsonArray elt -> new ListField(fieldName, convertInferredSchemaToField "element" elt)
        | JsonNumber -> new DataField(fieldName, typeof<decimal>, inferredSchema.Optional) 
        | JsonBoolean -> DataField(fieldName, typeof<bool>, inferredSchema.Optional)
        | JsonString -> DataField(fieldName, typeof<string>, inferredSchema.Optional)
        | JsonNull -> DataField(fieldName, typeof<bool>, true)
        | JsonUnknown ->
            failwith (sprintf "Unsupported InferredSchema type for field '%s': %A" fieldName inferredSchema.Type)
    r
let createParquetTable (inferredSchema: InferredSchema) : Table =
    let schemaFields =
        match inferredSchema.Type with
        | JsonObject kvs -> kvs |> Map.map convertInferredSchemaToField |> Map.toList |> List.map snd
        | _ -> failwith "Root schema must be a JsonObject"
    let schema = new ParquetSchema(schemaFields)
    new Table(schema)

let populateParquetTable  (jsonObjects: JsonElement seq) (table: Table): Table =
    let schemaFields = table.Schema.Fields

    let rec extractValue (field: Field) (element: JsonElement) : obj =
        let handleArray (elementType: Field) (element: JsonElement) =
            let elementSeq = element.EnumerateArray() |> Seq.map (extractValue elementType)
            if elementType :? DataField then
                elementSeq |> Seq.toArray |> box
            else
                elementSeq |> Seq.map (fun v -> v :?> Parquet.Rows.Row) |> Seq.toArray |> box

        match element.ValueKind with
        | JsonValueKind.Null -> box (null)
        | _ ->
            match field with
            | :? DataField as f when f.ClrType = typeof<decimal> -> box (element.GetDecimal())
            | :? DataField as f when f.ClrType = typeof<bool> -> box (element.GetBoolean())
            | :? DataField as f when f.ClrType = typeof<string> -> box (element.GetString())
            | :? ListField as f -> handleArray f.Item element
            | :? StructField as f ->
                let fields = f.Fields
                let values =
                    fields
                    |> List.ofSeq
                    |> List.map (fun field -> element.GetProperty(field.Name) |> extractValue field)

                new Row(values) |> box
            | _ -> failwith (sprintf "Unsupported field type: %A" field)

    for jsonObject in jsonObjects do
        let rowValues =
            schemaFields
            |> List.ofSeq
            |> List.map (fun field ->
                let mutable foundElement = JsonElement()

                match jsonObject.TryGetProperty(field.Name, &foundElement) with
                | true -> extractValue field foundElement
                | false ->
                    match field.IsNullable with
                    | true -> null
                    | _ -> failwith (sprintf "Non-optional property '%s' is missing" field.Name))
        table.Add(new Row(rowValues))
    table

let writeParquetTableToFile  (filePath: string) (table: Table): bool =
    async {
        use fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None)
        use! parquetWriter = ParquetWriter.CreateAsync(table.Schema, fileStream) |> Async.AwaitTask
        do! parquetWriter.WriteAsync(table) |> Async.AwaitTask
        return true
    }
    |> Async.RunSynchronously
