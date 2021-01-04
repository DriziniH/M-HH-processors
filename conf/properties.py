from enum import Enum
import src.constants as c


class Bucket_Paths(Enum):
    RAW_EVENTS = c.RAW_EVENTS
    RAW = c.RAW
    PROCESSED = c.PROCESSED
    ANALYZED = c.ANALYZED


SCHEMAS = {
    c.SCHEMA_USA: "src/schema/schemas/schema_usa.json",
    c.SCHEMA_EU: "src/schema/schemas/schema_eu.json",
    c.SCHEMA_CHINA: "src/schema/schemas/schema_china.json"
}

REGIONS = {
    # USA Properties
    c.REGION_USA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_USA_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_USA_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_USA_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_USA_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_USA: SCHEMAS[c.SCHEMA_USA],
            c.SCHEMA_EU: SCHEMAS[c.SCHEMA_EU]
        }
    },

    # EU Properties
    c.REGION_EU: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_EU_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_EU_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_EU_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_EU_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_EU: SCHEMAS[c.SCHEMA_EU]
        }
    },

    # CHINA Properties
    c.REGION_CHINA: {
        c.RAW_EVENTS: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_CHINA_RAW_EVENTS\\",
        c.RAW: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_CHINA_RAW\\",
        c.PROCESSED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_CHINA_PROCESSED\\",
        c.ANALYZED: "C:\\Showcase\\Projekt\\M-HH-DataLake-Simulation\\data-lake\\S3_CHINA_ANALYZED\\",
        c.SCHEMAS: {
            c.SCHEMA_EU: SCHEMAS[c.SCHEMA_EU]
        }
    }
}
