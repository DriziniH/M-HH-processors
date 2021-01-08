from threading import Thread
import uuid
import random

from src.car import start_car

start_car(4, "USA", "car-usa", ["car-usa-processed"])
start_car(4, "EU", "car-eu", ["car-eu-processed"])
