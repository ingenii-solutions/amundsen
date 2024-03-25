import logging
from os import getenv
from uuid import uuid4

from apache_atlas.client.base_client import AtlasClient
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.atlas_search_data_extractor import AtlasSearchDataExtractor
from databuilder.extractor.snowflake_metadata_extractor import SnowflakeMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_atlas_csv_loader import FsAtlasCSVLoader
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.atlas_csv_publisher import AtlasCSVPublisher
from databuilder.publisher.elasticsearch_constants import (
    DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, USER_ELASTICSEARCH_INDEX_MAPPING,
)
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

ATLAS_CREATE_BATCH_SIZE = 5
ATLAS_SEARCH_CHUNK_SIZE = 5
ATLAS_DETAILS_CHUNK_SIZE = 5
ATLAS_PROCESS_POOL_SIZE = 1

elasticsearch_client = Elasticsearch([
    {
        "host": getenv("CREDENTIALS_ELASTICSEARCH_PROXY_HOST", "localhost"),
        "port": getenv("CREDENTIALS_ELASTICSEARCH_PROXY_PORT", 9200)
    }
])

Base = declarative_base()

atlas_host = getenv("CREDENTIALS_ATLAS_PROXY_HOST", "localhost")
atlas_port = getenv("CREDENTIALS_ATLAS_PROXY_PORT", 21000)
atlas_endpoint = f"http://{atlas_host}:{atlas_port}"

atlas_user = "admin"
atlas_password = "admin"

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Disable snowflake logging
logging.getLogger("snowflake.connector.network").disabled = True

SNOWFLAKE_DATABASE = "SNOWFLAKE"
SNOWFLAKE_IGNORED_SCHEMAS = ["\'DVCORE\'", "\'INFORMATION_SCHEMA\'", "\'STAGE_ORACLE\'"]

# todo: connection string needs to change
def connection_string():
    # Refer this doc: https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    # for supported connection parameters and configurations
    user = getenv("CREDENTIALS_SNOWFLAKE_USER")
    password = getenv("CREDENTIALS_SNOWFLAKE_PASSWORD")
    account = getenv("CREDENTIALS_SNOWFLAKE_ACCOUNT")
    warehouse = getenv("CREDENTIALS_SNOWFLAKE_WAREHOUSE")
    role = getenv("CREDENTIALS_SNOWFLAKE_ROLE")
    return f'snowflake://{user}:{password}@{account}/{SNOWFLAKE_DATABASE}?warehouse={warehouse}&role={role}'


def register_entity_types():
    """
    Register custom Atlas Entity Types.
    """

    job_config = ConfigFactory.from_dict({
        f"{AtlasCSVPublisher.ATLAS_CLIENT}": AtlasClient(atlas_endpoint,
                                                         (atlas_user, atlas_password)),
        f"{AtlasCSVPublisher.ENTITY_DIR_PATH}": "/tmp",
        f"{AtlasCSVPublisher.RELATIONSHIP_DIR_PATH}": "/tmp",
        f"{AtlasCSVPublisher.ATLAS_ENTITY_CREATE_BATCH_SIZE}": ATLAS_CREATE_BATCH_SIZE,
        f"{AtlasCSVPublisher.REGISTER_ENTITY_TYPES}": True
    })

    publisher = AtlasCSVPublisher()

    publisher.init(job_config)

def create_sample_snowflake_job():
    where_clause = f"WHERE c.TABLE_SCHEMA not in ({','.join(SNOWFLAKE_IGNORED_SCHEMAS)}) \
            AND c.TABLE_SCHEMA not like 'STAGE_%' \
            AND c.TABLE_SCHEMA not like 'HIST_%' \
            AND c.TABLE_SCHEMA not like 'SNAP_%' \
            AND lower(c.COLUMN_NAME) not like 'dw_%';"

    tmp_folder = '/var/tmp/amundsen/tables'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeMetadataExtractor()
    csv_loader = FsAtlasCSVLoader()

    task = DefaultTask(extractor=sql_extractor,
                       loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        f"extractor.snowflake.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}": connection_string(),
        f"extractor.snowflake.{SnowflakeMetadataExtractor.SNOWFLAKE_DATABASE_KEY}": SNOWFLAKE_DATABASE,
        f"extractor.snowflake.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}": where_clause,
        f"loader.filesystem_csv_atlas.{FsAtlasCSVLoader.ENTITY_DIR_PATH}": node_files_folder,
        f"loader.filesystem_csv_atlas.{FsAtlasCSVLoader.RELATIONSHIP_DIR_PATH}": relationship_files_folder,
        f"loader.filesystem_csv_atlas.{FsAtlasCSVLoader.SHOULD_DELETE_CREATED_DIR}": True,
        f"publisher.atlas_csv_publisher.{AtlasCSVPublisher.ATLAS_CLIENT}": AtlasClient(atlas_endpoint,
                                                                                       (atlas_user, atlas_password)),
        f"publisher.atlas_csv_publisher.{AtlasCSVPublisher.ENTITY_DIR_PATH}": node_files_folder,
        f"publisher.atlas_csv_publisher.{AtlasCSVPublisher.RELATIONSHIP_DIR_PATH}": relationship_files_folder,
        f"publisher.atlas_csv_publisher.{AtlasCSVPublisher.ATLAS_ENTITY_CREATE_BATCH_SIZE}": ATLAS_CREATE_BATCH_SIZE,
        f"publisher.atlas_csv_publisher.{AtlasCSVPublisher.REGISTER_ENTITY_TYPES}": False
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=AtlasCSVPublisher())
    return job


def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   entity_type='table',
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_{uuid}`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param entity_type:                Entity type handed to the `Neo4jSearchDataExtractor` class, used to determine
                                       Cypher query to extract data from Neo4j. Defaults to `table`.
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = "/var/tmp/amundsen/search_data.json"

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=AtlasSearchDataExtractor(),
                       transformer=NoopTransformer())

    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = f"{entity_type}_{uuid4()}"

    job_config = ConfigFactory.from_dict({
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_URL_CONFIG_KEY}": atlas_host,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_PORT_CONFIG_KEY}": atlas_port,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_PROTOCOL_CONFIG_KEY}": "http",
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_VALIDATE_SSL_CONFIG_KEY}": False,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_USERNAME_CONFIG_KEY}": atlas_user,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_PASSWORD_CONFIG_KEY}": atlas_password,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_SEARCH_CHUNK_SIZE_KEY}": ATLAS_SEARCH_CHUNK_SIZE,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ATLAS_DETAILS_CHUNK_SIZE_KEY}": ATLAS_DETAILS_CHUNK_SIZE,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.PROCESS_POOL_SIZE_KEY}": ATLAS_PROCESS_POOL_SIZE,
        f"extractor.atlas_search_data.{AtlasSearchDataExtractor.ENTITY_TYPE_KEY}": entity_type.title(),
        "loader.filesystem.elasticsearch.file_path": extracted_search_data_path,
        "loader.filesystem.elasticsearch.mode": "w",
        "publisher.elasticsearch.file_path": extracted_search_data_path,
        "publisher.elasticsearch.mode": "r",
        "publisher.elasticsearch.client": elasticsearch_client,
        "publisher.elasticsearch.new_index": elasticsearch_new_index_key,
        "publisher.elasticsearch.doc_type": "_doc",
        "publisher.elasticsearch.alias": elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if elasticsearch_mapping:
        job_config.put(f"publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY}",
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)

    # Do not remove this step unless you already kickstarted your Atlas with custom entity types.
    register_entity_types()

    job = create_sample_snowflake_job()
    job.launch()

    job_es_table = create_es_publisher_sample_job(
        elasticsearch_index_alias="table_search_index",
        entity_type="table")
    job_es_table.launch()

    job_es_user = create_es_publisher_sample_job(
        elasticsearch_index_alias="user_search_index",
        entity_type="user",
        elasticsearch_mapping=USER_ELASTICSEARCH_INDEX_MAPPING)
    job_es_user.launch()

    job_es_dashboard = create_es_publisher_sample_job(
        elasticsearch_index_alias="dashboard_search_index",
        entity_type="dashboard",
        elasticsearch_mapping=DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)
    job_es_dashboard.launch()
