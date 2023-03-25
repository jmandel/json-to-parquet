// module josh

open System.Text.Json
// open Parquet


type InferredSchema =
    | JsonObject of InferredKVs
    | JsonArray of InferredElts
    | JsonNumber
    | JsonBoolean
    | JsonString
    | JsonNull
    | JsonUnknown

and InferredKVs = Map<string, InferredSchema>
and InferredElts = InferredSchema
// Creating a JsonObject with an empty set of InferredKVs

let emptyJsonObject = JsonObject(Map.empty)

let m = Map([ "name", JsonString ])

// Adding a JsonString property to the JsonObject
let jsonObjectWithAString = JsonObject(m)

// Creating a JsonArray containing JsonNumber elements
let jsonArrayWithNumbers = JsonArray(JsonNumber)

// Creating a JsonObject containing a JsonArray
let jsonObjectWithArray = JsonObject(Map([ "numbers", jsonArrayWithNumbers ]))

// Creating a JsonObject containing another JsonObject
let innerJsonObject = JsonObject(Map([ "ageInYears", JsonNumber ]))
let jsonObjectWithNestedObject = JsonObject(Map([ "stats", innerJsonObject ]))

// Function to recursively print an InferredSchema

// Function to recursively print an InferredSchema with depth-based indentation
let rec printInferredSchema (schema: InferredSchema) (depth: int) =
    let indent = String.replicate depth "  "

    match schema with
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


open System.Text.Json

// Incrementally create an InferredSchema for a given JsonElement
let rec createInferredSchema (element: JsonElement) : InferredSchema =
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
                JsonUnknown

        JsonArray inferredElts
    | JsonValueKind.Number -> JsonNumber
    | JsonValueKind.String -> JsonString
    | JsonValueKind.True
    | JsonValueKind.False -> JsonBoolean
    | _ -> JsonNull


// Merge two InferredSchema instances
and mergeInferredSchema (schema1: InferredSchema) (schema2: InferredSchema) : InferredSchema =
    match schema1, schema2 with
    | JsonObject kvs1, JsonObject kvs2 ->
        let mergedKvs =
            kvs1
            |> Map.fold
                (fun map key value ->
                    match kvs2 |> Map.tryFind key with
                    | Some value2 ->
                        let mergedValue = mergeInferredSchema value value2
                        Map.add key mergedValue map
                    | None -> Map.add key value map)
                kvs2

        JsonObject mergedKvs
    | JsonArray elts1, JsonArray elts2 ->
        let mergedElts = mergeInferredSchema elts1 elts2
        JsonArray mergedElts
    | JsonUnknown, other -> other
    | other, JsonUnknown -> other
    | _ when schema1 = schema2 -> schema1
    | _ -> failwith (sprintf "Incompatible InferredSchema: %A and %A" schema1 schema2)


let jsonString =
    """
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
}]
"""


let document = JsonDocument.Parse(jsonString)

let iss = createInferredSchema (document.RootElement)
printInferredSchema iss 0
