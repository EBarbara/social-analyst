import json


def extract_coordinates(coordinate_data, location_data):
    try:
        if coordinate_data is None:
            bounding_box = location_data['bounding_box']['coordinates']
            lon_sum = 0.0
            lat_sum = 0.0
            for coordinates in bounding_box:
                lon_sum += coordinates[0]
                lat_sum += coordinates[1]
            coord = (lon_sum / 4, lat_sum / 4)
        else:
            coord = tuple(coordinate_data['coordinates'])
    except TypeError:
        coord = (0, 0)
    return coord


def extract_json(json_origin):
    try:
        json_data = json.loads(json_origin)
    except ValueError as e:
        return None
    return json_data


def run(context):
    lines = context.socketTextStream("localhost", 9999)
    tweets = lines.map(lambda line: extract_json(line)) \
        .filter(lambda json: json is not None) \
        .map(lambda json: (json["id"],
                           json["created_at"],
                           extract_coordinates(json["coordinates"], json["place"])[0],
                           extract_coordinates(json["coordinates"], json["place"])[1],
                           json["text"])) \
        .filter(lambda tweet: tweet[2] != 0 or tweet[3] != 0)

    return tweets

