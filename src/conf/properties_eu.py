from src import constants as c

from enum import Enum
import uuid

CONF = {
    c.REGION_NAME: "EU",

    # DL Paths
    c.DL_PATHS: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW_EVENTS\\CAR\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW\\CAR\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_PROCESSED\\CAR\\",
        c.ANALYZED_CAR: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_ANALYZED\\CAR\\",
        c.ANALYZED_REGION: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_ANALYZED\\REGION\\"
    },
    # Available schemas
    c.SCHEMAS: {
        c.SCHEMA_EU: "src/conf/schemas/schema_eu.json"
    },

    # Kafka Topics
    c.TOPICS: {
        c.TOPIC_RAW: "car-eu",
        c.TOPIC_INFO_CAR: "car-eu-info",
        c.TOPIC_INFO_REGION: "region-eu-info"
    },

    # MongoDB config
    c.DB_NAME: "M-HH-EU",
    c.DB_COLS: {
        c.PROCESSED: "processed",
        c.ANALYZED_REGION: "analysis"
    }
}
