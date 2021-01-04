from enum import Enum
import src.conf.constants as c


class Bucket_Paths(Enum):
    RAW_EVENTS = c.RAW_EVENTS
    RAW = c.RAW
    PROCESSED = c.PROCESSED
    ANALYZED = c.ANALYZED


SCHEMA = {
    c.SCHEMA_USA: {
        c.SCHEMA_PATH: "src/conf/schemas/schema_usa.json",
        c.SCHEMA_TOPIC : "car-usa-processed"
    },
    c.SCHEMA_EU: {
        c.SCHEMA_PATH: "src/conf/schemas/schema_eu.json",
        c.SCHEMA_TOPIC : "car-eu-processed"
    },
    c.SCHEMA_CHINA: {
        c.SCHEMA_PATH: "src/conf/schemas/schema_china.json",
        c.SCHEMA_TOPIC : "car-china-processed"
    }
}

REGIONS = {
    # USA Properties
    c.REGION_USA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_USA: SCHEMA[c.SCHEMA_USA],
            c.SCHEMA_EU: SCHEMA[c.SCHEMA_EU]
        }
    },

    # EU Properties
    c.REGION_EU: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_EU_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_EU: SCHEMA[c.SCHEMA_EU]
        }
    },

    # CHINA Properties
    c.REGION_CHINA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_CHINA_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_EU: SCHEMA[c.SCHEMA_EU]
        }
    }
}
