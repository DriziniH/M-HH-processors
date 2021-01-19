import random

MODELS = {
    0: {
        "name": "A-Klasse",
        "labels": ["Benzer", "A", "Oma auto"]
    },
    1: {
        "name": "B-Klasse",
        "labels": ["Benzer", "B"]
    },
    2: {
        "name": "C-Klasse",
        "labels": ["Benzer", "C"]
    },
    3: {
        "name": "E-Klasse",
        "labels": ["Benzer", "E", "Immer Vorfahrt"]
    },
    4: {
        "name": "G-Klasse",
        "labels": ["Benzer", "G", "Umweltzerstörer"]
    },
    5: {
        "name": "S-Klasse",
        "labels": ["Benzer", "S", "Spritzer"]
    },
}

FUEL = {
    0: "gasoline",
    1: "diesel",
    2: "gas",
    3: "electric"
}

INFOTAINMENT_SERVICE = {
    0: "Navigation",
    1: "FM",
    2: "Mobile Phone",
    3: "Information System"
}


def get_car_data_usa():
    model = random.randrange(0, 5)
    return {
        "model": MODELS[model]["name"],
        "labels": MODELS[model]["labels"],
        "fuel": FUEL[random.randrange(0, 3)],


        "mileage_total":  random.uniform(2000.0, 100000.0),
        "mileage": random.uniform(0.0, 500.0),
        "travel_time_total": random.randrange(0, 10000000000),
        "travel_time": random.randrange(0, 500),

        "oil_level": random.uniform(0.0, 100.0),
        "break_fluid_level": random.uniform(0.0, 100.0),
        "fuel_level": random.uniform(0.0, 90.0),

        "health": {
            "engine": random.uniform(0.0, 100.0),
            "breaks": random.uniform(0.0, 100.0),
        },

        "tire_pressure": random.uniform(0.0, 100.0),

        "temperature": {
            "engine": random.uniform(0.0, 100.0),
            "car": random.uniform(0.0, 100.0),
            "breaks": random.uniform(0.0, 100.0),
            "tires": random.uniform(0.0, 100.0),
        },

        "break_power": random.uniform(0.0, 100.0),
        "break": bool(random.getrandbits(1)),

        "gas_power": random.uniform(0.0, 100.0),
        "gas": bool(random.getrandbits(1)),

        "light": bool(random.getrandbits(1)),
        "acc": random.uniform(0.0, 100.0),
        "mph": random.uniform(0.0, 200.0),
        "rpm": random.uniform(0.0, 10000),
        "oxygen_level": random.uniform(0.0, 100.0),
        "pos": {
            "lat": random.uniform(38.0, 45.0),
            "lon": random.uniform(-78.0, -123.0),
        },

        "infotainment": {
            "on": bool(random.getrandbits(1)),
            "service": INFOTAINMENT_SERVICE[random.randrange(0, 3)],
            "volume": random.uniform(0.0, 100.0)
        }
    }


def get_car_data_eu():
    return {
        "model": MODELS[random.randrange(0, 5)]["name"],
        "labels": MODELS[random.randrange(0, 5)]["labels"],
        "fuel": FUEL[random.randrange(0, 3)],

        "kilometer_total":  random.uniform(2000.0, 100000.0),
        "kilometer": random.uniform(0.0, 500.0),
        "travel_time_total": random.randrange(0, 10000000000),
        "travel_time": random.randrange(0, 500),

        "oil_level": random.uniform(0.0, 100.0),
        "break_fluid_level": random.uniform(0.0, 100.0),
        "fuel_level": random.uniform(0.0, 90.0),

        "engine": random.uniform(0.0, 100.0),
        "breaks": random.uniform(0.0, 100.0),

        "tire_pressure": random.uniform(0.0, 100.0),

        "temperature": {
            "engine": random.uniform(0.0, 100.0),
            "car": random.uniform(0.0, 100.0),
            "breaks": random.uniform(0.0, 100.0),
            "tires": random.uniform(0.0, 100.0),
        },

        "break_power": random.uniform(0.0, 100.0),
        "break": bool(random.getrandbits(1)),

        "gas_power": random.uniform(0.0, 100.0),
        "gas": bool(random.getrandbits(1)),

        "light": bool(random.getrandbits(1)),
        "acc": random.uniform(0.0, 100.0),
        "kmh": random.uniform(0.0, 200.0),
        "rpm": random.uniform(0.0, 10000),
        "oxygen_level": random.uniform(0.0, 100.0),
        "pos": {
            "lat": random.uniform(44.0, 54.0),
            "lon":  random.uniform(-2.0, 30.0),
        },

        "infotainment": {
            "on": bool(random.getrandbits(1)),
            "service": INFOTAINMENT_SERVICE[random.randrange(0, 3)],
            "volume": random.uniform(0.0, 100.0)
        }
    }


def get_car_data_china():
    return {

        "模型": "甲级",
        "标签": ["本泽"],
        "燃料": "柴油",

        "千米_总":  random.uniform(2000.0, 100000.0),
        "千米": random.uniform(0.0, 500.0),
        "旅行_时候_总": random.randrange(0, 10000000000),
        "旅行_时候": random.randrange(0, 500),

        "油_层次": random.uniform(0.0, 100.0),
        "断裂_流畅_层次": random.uniform(0.0, 100.0),
        "燃料_层次": random.uniform(0.0, 90.0),

        "发动机": random.uniform(0.0, 100.0),
        "休息": random.uniform(0.0, 100.0),

        "胎压": random.uniform(0.0, 100.0)
    }


IDS_EU = {
    0: "1a7a18f9-b795-4979-a2ca-dbe816fa43a3",
    1: "5c40d7cc-965b-4086-9cf8-666f677100b4",
    2: "49cebce8-4e30-4851-9a46-aacf924542df",
    3: "41537170-b79c-49c8-ae09-d3345fea1231",
    4: "dd4ee74d-a41e-4ea6-b94e-1fccd34d2c33",
    5: "c2e1198f-bb52-4e8b-be0b-9685e85bc81a",
    6: "0094dacf-6654-432f-a6a8-2e02e7b0a6b6",
    7: "e73c95ec-ad1d-4f33-878e-2c7b9a33068f"
}


IDS_USA = {
    0: "e39c8011-ae22-4538-9121-086d6ced9909",
    1: "578e933f-56a0-4821-86e2-81e018deb510",
    2: "bfa3fb4d-219d-4a68-b1f5-1a5490760e67",
    3: "eb0d89fd-e7b7-4412-bc1e-da6196cfa166",
    4: "4ee2d7ba-092a-4fe6-8bec-c5585a94b82f",
    5: "a3391884-f775-4b7c-abe6-c5809802cba6",
    6: "61d80ac9-70a8-4495-8115-d2b9c0cb2df5",
    7: "255dba29-ad27-4ec3-942e-e9246e9117f4"
}

IDS_CHINA = {
    0: "2d5e4339-3754-4f6d-92f7-3b873af7a067",
    1: "503960be-0909-43ce-a394-cf0cadba3983",
    2: "29bc9517-e3a5-4490-9f09-cf2ea8d31ee3",
    3: "12807d7d-448d-49ab-9a38-c66e03dc1c81",
    4: "89436f1c-4bff-4db6-a943-a7faa6dbff4a",
    5: "1730e502-234b-46a7-a389-0526624599dc",
    6: "4fb4a541-f0aa-4bb6-bfc2-007e0f0d06a1",
    7: "c77dea4e-1f9c-4478-a70d-7bc3bb9d9571"
}
