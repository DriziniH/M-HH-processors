import json
import jsonlines
from src.utility import io
from src.conf import properties_showcase as ps


dataeu = {
    "metadata": {
        "region": "USA",
        "carId": "41537170-b79c-49c8-ae09-d3345fea1231",
        "schema": "SCHEMA_EU"
    },
    "data": ps.get_car_data_eu()
}

datausa = {
    "metadata": {
        "region": "USA",
        "carId": "41537170-b79c-49c8-ae09-d3345fea1231",
        "schema": "SCHEMA_EU"
    },
    "data": ps.get_car_data_usa()
}


# io.write_json_lines(
#     'C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\year=2021\\month=1\\day=13\\car.json', dataeu)
# io.write_json_lines(
#     'C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\year=2021\\month=1\\day=13\\car.json', datausa)

data = []
with jsonlines.open('C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\year=2021\\month=1\\day=13\\car.json') as reader:
    for obj in reader:
        data.append(obj)

# with open('C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\year=2021\\month=1\\day=13\\car.txt') as f:
#     for line in f:
#         try:
#             print(line)
#             data.append(json.loads(line))
#         except:
#             pass

# print(data)
