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


IDS = {
    0: "1a7a18f9-b795-4979-a2ca-dbe816fa43a3",
    1: "5c40d7cc-965b-4086-9cf8-666f677100b4",
    2: "49cebce8-4e30-4851-9a46-aacf924542df",
    3: "41537170-b79c-49c8-ae09-d3345fea1231",
    4: "dd4ee74d-a41e-4ea6-b94e-1fccd34d2c33",
    5: "c2e1198f-bb52-4e8b-be0b-9685e85bc81a",
    6: "0094dacf-6654-432f-a6a8-2e02e7b0a6b6",
    7: "e73c95ec-ad1d-4f33-878e-2c7b9a33068f"
}
