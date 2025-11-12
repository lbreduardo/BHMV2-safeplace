import json


def convert_geojson2js(input_geojson):
    file_name = input_geojson.removesuffix(".geojson")
    output_js = file_name + ".js"
    js_variable_name = f"json_{file_name}"

    with open(input_geojson, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    with open(output_js, "w", encoding="utf-8") as f:
        f.write(f"var {js_variable_name} = ")
        json.dump(geojson_data, f, ensure_ascii=False)
        f.write(";")

    print(f"Arquivo '{output_js}' criado com sucesso!")
