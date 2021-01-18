from src import constants as c

from enum import Enum
import uuid

CONF = {

    # Available schemas
    c.SCHEMAS: {
        c.SCHEMA_USA: "src/conf/schemas/schema_usa.json"
    },

    # Kafka Topics
    c.TOPICS: {
        c.TOPIC_INGEST: "car-usa",
        c.TOPIC_ANALYSIS_CAR: "car-usa-analysis",
        c.TOPIC_ANALYSIS_REGION: "region-usa-analysis"
    },

    # MongoDB config
    c.DB_NAME: "M-HH-USA",
    c.DB_COLS: {
        c.PROCESSED: "processed",
        c.ANALYZED_REGION: "analysis"
    }
}
