from src import constants as c

from enum import Enum
import uuid

CONF = {
    # Available schemas
    c.SCHEMAS: {
        c.SCHEMA_EU: "src/conf/schemas/schema_eu.json"
    },

    # Kafka Topics
    c.TOPICS: {
        c.TOPIC_INGEST: "car-eu",
        c.TOPIC_ANALYSIS_CAR: "car-eu-analysis",
        c.TOPIC_ANALYSIS_REGION: "region-eu-analysis"
    },

    # MongoDB config
    c.DB_NAME: "M-HH-EU",
    c.DB_COLS: {
        c.PROCESSED: "processed",
        c.ANALYZED_REGION: "analysis"
    }
}
