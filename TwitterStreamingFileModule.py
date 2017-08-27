import json


def extract_coordinates(coordinate_data, location_data):
    # try:
        if coordinate_data is None:
            bounding_box = location_data['bounding_box']['coordinates'][0]
            lon_sum = 0.0
            lat_sum = 0.0
            for coordinates in bounding_box:
                lon_sum += coordinates[0]
                lat_sum += coordinates[1]
            coord = (lon_sum / 4, lat_sum / 4)
        else:
            coord = tuple(coordinate_data['coordinates'])
    # except TypeError:
    #     coord = (0, 0)
        return coord


def extract_json(json_origin):
    try:
        json_data = json.loads(json_origin)
    except ValueError as e:
        return None
    return json_data


def run(context):
    lines = context.textFileStream("C:\\Users\\Estevan\\PycharmProjects\\Mining\\tweets")
    tweets = lines.map(lambda line: extract_json(line)) \
        .filter(lambda json: json is not None) \
        .map(lambda json: (json["id"],
                           json["created_at"],
                           extract_coordinates(json["coordinates"], json["place"])[0],
                           extract_coordinates(json["coordinates"], json["place"])[1],
                           json["text"]))

    return tweets
