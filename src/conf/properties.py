from enum import Enum
import src.conf.constants as c
import uuid


REGIONS = {
    # USA Properties
    c.REGION_USA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_RAW_EVENTS\\CAR\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_RAW\\CAR\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_ANALYZED\\CAR\\",
        c.SCHEMAS: {
            c.SCHEMA_USA: "src/conf/schemas/schema_usa.json",
            c.SCHEMA_EU: "src/conf/schemas/schema_eu.json"
        },
        c.TOPICS: {
            c.TOPIC_RAW: "car-usa",
            c.TOPIC_PROCESSED: "car-usa-processed",
            c.TOPIC_INFO_CAR: "car-usa-info",
            c.TOPIC_INFO_REGION: "region-usa-info"
        }
    },

    # EU Properties
    c.REGION_EU: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW_EVENTS\\CAR\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW\\CAR\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_PROCESSED\\CAR\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_ANALYZED\\CAR\\",
        c.SCHEMAS: {
            c.SCHEMA_EU: "src/conf/schemas/schema_eu.json"
        },
        c.TOPICS: {
            c.TOPIC_RAW: "car-eu",
            c.TOPIC_PROCESSED: "car-eu-processed",
            c.TOPIC_INFO_CAR: "car-eu-info",
            c.TOPIC_INFO_REGION: "region-eu-info"
        }
    },

    # CHINA Properties
    c.REGION_CHINA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_RAW_EVENTS\\CAR\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_RAW\\CAR\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_PROCESSED\\CAR\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_ANALYZED\\CAR\\",
        c.SCHEMAS: {
            c.SCHEMA_CHINA: "src/conf/schemas/schema_china.json"
        },
        c.TOPICS: {
            c.TOPIC_RAW: "car-china",
            c.TOPIC_PROCESSED: "car-china-processed",
            c.TOPIC_INFO_CAR: "car-china-info",
            c.TOPIC_INFO_REGION: "region-china-info"
        }
    }
}
