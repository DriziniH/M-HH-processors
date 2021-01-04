from threading import Thread
import uuid
import random

from src import producer 
from src import consumer

usa_car_1 = str(uuid.uuid4())
usa_car_2 = str(uuid.uuid4())
usa_car_3 = str(uuid.uuid4())
usa_car_4 = str(uuid.uuid4())

eu_car_1 = str(uuid.uuid4())
eu_car_2 = str(uuid.uuid4())
eu_car_3 = str(uuid.uuid4())
eu_car_4 = str(uuid.uuid4())


p1 = Thread(target = producer.publish_infite, args = ("car-usa",usa_car_1))
p1.start()
p2 = Thread(target = producer.publish_infite, args = ("car-usa",usa_car_2))
p2.start()
p3 = Thread(target = producer.publish_infite, args = ("car-usa",usa_car_3))
p3.start()
p4 = Thread(target = producer.publish_infite, args = ("car-usa",usa_car_4))
p4.start()

p5 = Thread(target = producer.publish_infite, args = ("car-usa",eu_car_1))
p5.start()
p6 = Thread(target = producer.publish_infite, args = ("car-usa",eu_car_2))
p6.start()
p7 = Thread(target = producer.publish_infite, args = ("car-usa",eu_car_3))
p7.start()
p8 = Thread(target = producer.publish_infite, args = ("car-usa",eu_car_4))
p8.start()


c1 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], usa_car_1))
c1.start()
c2 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], usa_car_2))
c2.start()
c3 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], usa_car_3))
c3.start()
c4 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], usa_car_4))
c4.start()

c5 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], eu_car_1))
c5.start()
c6 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], eu_car_2))
c6.start()
c7 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], eu_car_3))
c7.start()
c8 = Thread(target = consumer.consume_log, args = (["car-usa-processed"], eu_car_4))
c8.start()
