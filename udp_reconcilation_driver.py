# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("batchId", "")
dbutils.widgets.text("rawContainer", "")
dbutils.widgets.text("sanitizedContainer", "")
dbutils.widgets.text("loggingContainer", "")
dbutils.widgets.text("configsContainer", "")
dbutils.widgets.text("applicationName", "")
dbutils.widgets.text("loadGroupName", "")
dbutils.widgets.text("sourceSystemName", "")
dbutils.widgets.text("datasetName", "")
dbutils.widgets.text("loadType", "")
dbutils.widgets.text("dbName", "")
dbutils.widgets.text("dbHostName", "")
dbutils.widgets.text("dbPortNo", "")
dbutils.widgets.text("servicePrincipalClientId", "")
dbutils.widgets.text("servicePrincipalClientSecret", "")
dbutils.widgets.text("storageAccount", "")
dbutils.widgets.text("tenantId", "")
dbutils.widgets.text("secretScopeName", "")
dbutils.widgets.text("phaseName", "")
dbutils.widgets.text("loadCreatedBy", "")
dbutils.widgets.text("batchStartTime", "")
dbutils.widgets.text("datasetFileFormat", "")
dbutils.widgets.text("sourceLayer", "")
dbutils.widgets.text("targetLayer", "")
dbutils.widgets.text("measureType", "")
dbutils.widgets.text("dataRead", "")
dbutils.widgets.text("dataWritten", "")

# COMMAND ----------

from typing import Dict
import inspect

from common.constants import udp_constants
from common.model.udp_batch_entity import UDPBatchEntity
from common.model.udp_file_conn_details import UDPFileConnDetails
from common.model.udp_db_conn_details import UDPDBConnDetails,SQLDBAuthTypeEnum
from common.core.udp_common import UDPCommon
from common.core.udp_data_layer import UDPDataLayer
from reconcilation.udp_reconcilation import UDPReconcilation
from ingestion.utils.udp_ingestion_utilities import UDPIngestionUtilities
from ingestion.udp_ingestion_exception import  FailToReadConfigData
from reconcilation.constants import reconcilation_constants
from ingestion.constants import ingestion_constants

# COMMAND ----------

application_name = dbutils.widgets.get("applicationName")
load_group_name = dbutils.widgets.get("loadGroupName")
source_system_name = dbutils.widgets.get("sourceSystemName")
dataset_name = dbutils.widgets.get("datasetName")
load_type = dbutils.widgets.get("loadType")
raw_layer_container = dbutils.widgets.get("rawContainer")
sanitized_layer_container = dbutils.widgets.get("sanitizedContainer")
logging_container = dbutils.widgets.get("loggingContainer")
configs_container = dbutils.widgets.get("configsContainer")
dataset_file_format = dbutils.widgets.get("datasetFileFormat")
storage_account = dbutils.widgets.get("storageAccount")
tenant_id = dbutils.widgets.get("tenantId")
batch_id = dbutils.widgets.get("batchId")
load_created_by = dbutils.widgets.get("loadCreatedBy")
source_layer = dbutils.widgets.get("sourceLayer")
target_layer = dbutils.widgets.get("targetLayer")
batch_start_time = dbutils.widgets.get("batchStartTime")
scope_name = dbutils.widgets.get("secretScopeName")
service_principle_client_id = dbutils.widgets.get("servicePrincipalClientId")
service_principle_client_secret = dbutils.widgets.get("servicePrincipalClientSecret")
db_host_name = dbutils.widgets.get("dbHostName")
db_port_no = dbutils.widgets.get("dbPortNo")
db_name = dbutils.widgets.get("dbName")
measure_type = dbutils.widgets.get("measureType")
data_read = dbutils.widgets.get("dataRead")
data_written = dbutils.widgets.get("dataWritten")


service_principal_client_id = dbutils.secrets.get(scope=scope_name,
                                                  key=service_principle_client_id
                                                  )
service_principal_client_secret = dbutils.secrets.get(
    scope=scope_name,
    key=service_principle_client_secret
)

# COMMAND ----------

from pyspark.sql.dataframe import DataFrame


class UDPReconcilationWrapper:
    """
    UDPReconcilationDriver is the driver class responsible for data
    reconcilation.

    get source target df -- get source and target dataframe
    reconcile -- Perform reconcilation at various stages
    """
    _target_df: object

    def __init__(self, raw_layer: UDPDataLayer,
                 sanitized_layer: UDPDataLayer,
                 logging_layer: UDPDataLayer,
                 configs_layer: UDPDataLayer,
                 source_layer: str,
                 target_layer: str,
                 dataset_file_format: str
                 ):
        self._raw_layer = raw_layer
        self._sanitized_layer = sanitized_layer
        self._logging_layer = logging_layer
        self._configs_layer = configs_layer
        self._source_layer = source_layer
        self._target_layer = target_layer
        self._source_df = self._get_source_target_df(source_layer, dataset_file_format)
        self._target_df = self._get_source_target_df(target_layer, "")

    def _get_file_reading_configs(self, raw_file_path: str) -> Dict[str, str]:
        """
        _get_file_reading_configs is a method to load any file configurations
        stored for the incoming file feeds.
        :param: raw_file_path
        :return: Dict[str, str]
        """
        try:
            rawfile_configs = UDPIngestionUtilities.get_file_reading_configs(
                spark, raw_file_path
            )

            if "header" not in rawfile_configs.keys():
                rawfile_configs["header"] = "True"

            return rawfile_configs
        except Exception as e:
            raise FailToReadConfigData(
                ingestion_constants.exception_msg_template_with_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    argument_values=raw_file_path,
                    exception_msg=str(e)
                )
            )

    def _get_source_target_df(self, phase_name: str, dataset_file_format: str) -> DataFrame:
        """
        Get source and target dataframe using phaseName.

        :param phaseName: Phase name
        :return df: Pyspark dataframe

        Returns:
            object: DataFrame
        """
        if phase_name == udp_constants.raw_phase_name:
            raw_file_format_configs = self._get_file_reading_configs(
                self._configs_layer.config_path.raw_entity_file_config_path
            )
            df = (
                spark.read.format(dataset_file_format)
                .options(**raw_file_format_configs)
                .load(
                    self._raw_layer.config_path.raw_entity_path
                    + str(self._raw_layer.batch_entity.batch_id)
                    + "/*"
                )
            )

        elif phase_name == udp_constants.sanitized_phase_name:
            df = (
                spark.read.format("delta")
                .load(self._sanitized_layer.config_path.sanitized_entity_path)
                .filter(
                    " {} = '{}' ".format(
                        udp_constants.batch_id,
                        self._sanitized_layer.batch_entity.batch_id
                    )
                )
            )

        # This will get enhanced for other layers
        else:
            df = None

        if df is not None:
            new_cols = [
                UDPCommon.replace_special_chars(self._sanitized_layer.entity_config.replace_special_chars_from_col_names_flag,
                                                self._sanitized_layer.entity_config.trim_special_chars_from_col_names_flag,
                                                col,
                                                self._sanitized_layer.entity_config.replacement_char,
                                                self._sanitized_layer.entity_config.chars_to_replace_from_column_names
                                                ) for col in df.columns
            ]
            df = df.toDF(*new_cols)

        return df

    def add_reconcile_logs(self) -> None:
        reconcile_stage = self._raw_layer.batch_entity.phase_name
        recon_result = []
        recon_log = (
            self._raw_layer.batch_entity.batch_id,
            self._raw_layer.batch_entity.application_name,
            self._raw_layer.batch_entity.source_system_name,
            self._raw_layer.batch_entity.load_group_name,
            self._raw_layer.batch_entity.dataset_name,
            measure_type,
            data_read,
            data_written,
        )
        recon_result.append(recon_log)

        obj_recon = UDPReconcilation(
            spark,
            self._raw_layer.batch_entity.batch_id,
            self._raw_layer.batch_entity.application_name,
            self._raw_layer.batch_entity.source_system_name,
            self._raw_layer.batch_entity.load_group_name,
            self._raw_layer.batch_entity.dataset_name,
            self._configs_layer.config_path.recon_entity_config_path,
            self._logging_layer.config_path.recon_entity_log_path,
            False
        )

        obj_recon.log_reconcile_metrics(reconcile_stage, recon_result)

    def reconcile(self) -> None:
        """
        Perform reconcilation between source and target dataframe at various
        reconcilation stages.

        :retrun: None
        """
        reconcile_stage = self._sanitized_layer.batch_entity.phase_name

        obj_recon = UDPReconcilation(
            spark,
            self._sanitized_layer.batch_entity.batch_id,
            self._sanitized_layer.batch_entity.application_name,
            self._sanitized_layer.batch_entity.source_system_name,
            self._sanitized_layer.batch_entity.load_group_name,
            self._sanitized_layer.batch_entity.dataset_name,
            self._configs_layer.config_path.recon_entity_config_path,
            self._logging_layer.config_path.recon_entity_log_path,
            False
        )

        obj_recon.perform_reconciliation(
            self._source_df, self._target_df, reconcile_stage
        )

# COMMAND ----------

raw_layer_file_conn_details = UDPFileConnDetails(storage_account,
                                                 raw_layer_container,
                                                 dataset_file_format)
sanitized_layer_file_conn_details = UDPFileConnDetails(storage_account,
                                                       sanitized_layer_container,
                                                       dataset_file_format)
logging_file_conn_details = UDPFileConnDetails(storage_account,
                                               logging_container,
                                               dataset_file_format)
configs_file_conn_details = UDPFileConnDetails(storage_account,
                                               configs_container,
                                               dataset_file_format)

# Azure SQL Database connection details
database_connection_details = {'host': db_host_name,
                               'port': db_port_no,
                               'database_name': db_name,
                               'username': '',
                               'password': ''
                               }
# Service Principal details
service_principal_details = {'client_id': service_principal_client_id,
                             'client_secret': service_principal_client_secret
                             }

# Connection details object having Azure SQL connection details.
conn_details_obj = UDPDBConnDetails(
    sql_db_auth_type=SQLDBAuthTypeEnum.SERVICE_PRINCIPAL_AUTHENTICATION,
    database_details=database_connection_details,
    service_principal_details=service_principal_details,
    tenant_id=tenant_id
)

# COMMAND ----------

phase_name = "Reconcile {} vs {}".format(source_layer, target_layer)

# Batch load details
batch_load_details = {
    'application_name': application_name,
    'load_group_name': load_group_name,
    'source_system_name': source_system_name,
    'dataset_name': dataset_name,
    'batch_id': str(batch_id),
    'batch_start_time': batch_start_time,
    'phase_name': phase_name,
    'load_type': load_type,
    'load_created_by': 'reconciliation_framework'
}

# Current batch entity details object
batch_entity_object = UDPBatchEntity(batch_load_details)

raw_data_layer_object = UDPDataLayer(spark,
                                     conn_details_obj,
                                     raw_layer_file_conn_details,
                                     batch_entity_object
                                     )

sanitized_data_layer_object = UDPDataLayer(spark,
                                           conn_details_obj,
                                           sanitized_layer_file_conn_details,
                                           batch_entity_object
                                           )

logging_data_layer_object = UDPDataLayer(spark,
                                         conn_details_obj,
                                         logging_file_conn_details,
                                         batch_entity_object
                                         )

configs_data_layer_object = UDPDataLayer(spark,
                                         conn_details_obj,
                                         configs_file_conn_details,
                                         batch_entity_object
                                         )


reconciliation = UDPReconcilationWrapper(raw_data_layer_object,
                                        sanitized_data_layer_object,
                                        logging_data_layer_object,
                                        configs_data_layer_object,
                                        source_layer,
                                        target_layer,
                                        dataset_file_format
                                        )
if phase_name == reconcilation_constants.recon_phase_name:
    reconciliation.add_reconcile_logs()
else:
    reconciliation.reconcile()
