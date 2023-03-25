// module josh

open System.Text.Json
// open Parquet
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

let jsonString = """
[{
    "Students": [
        {
            "Name": "Alice",
            "Grade": 90
        },
        {
            "Name": "Bob",
            "Grade": 80
        },
        {
            "Name": "Charlie",
            "HungryFor": "Chicken"
        }
    ]
},{
    "Students": [
        {
            "Name": "Alice",
            "Grade": 90
        },
        {
            "Name": "Bob",
            "Grade": 80,
            "Failed": true
        },
        {
            "Name": "Charlie"
        }
    ]
}, {
    "Teachers": true,
    "Students": null
}]
"""


let document = JsonDocument.Parse(jsonString)

let iss = createInferredSchema (document.RootElement)
printInferredSchema iss 0
