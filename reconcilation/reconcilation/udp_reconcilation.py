"""
This file contains the class and methods used for processing UDP Reconcilation.

class UDPReconcilation: class responsible for data reconcilation
"""
import inspect
from typing import List, Tuple

from common.core.udp_delta_table_utils import UDPDeltaTableUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp
from pyspark.sql.types import Row

# These functions are used dynamically by the reconciliation engine
# Don't remove the below imports
from pyspark.sql.functions import min, max, sum, avg, mean, count, to_date, to_timestamp
from pyspark.sql.session import SparkSession

from reconcilation.udp_reconcilation_exception import (
    LoadReconcileConfigErr,
    LogReconcileMetricsErr,
    PerformReconciliationErr
)
from reconcilation.utils.udp_recon_core import UDPReconCore
from reconcilation.utils.udp_recon_utilities import UDPReconUtilities
from reconcilation.constants import reconcilation_constants
from common.constants import udp_constants
from common.udp_common_exception import InputValueError


class UDPReconcilation:
    """
    UDPReconcilation is the class responsible for data reconcilation.

    _rowcount_check -- checks row count
    _aggregate_check -- check aggregations
    _load_reconcile_config -- load reconcilation config
    log_reconcile_metrics -- log reconcilation metrics
    perform_reconciliation -- perform reconciliation
    """

    def __init__(
            self,
            spark: SparkSession,
            batch_id: str,
            application_name: str,
            source_system_name: str,
            load_group_name: str,
            dataset_name: str,
            recon_config_path: str,
            recon_delta_log_path: str,
            overwrite_log_path_flag: bool = False
    ):
        self.spark = spark
        self._application_name = application_name
        self._load_group_name = load_group_name
        self._dataset_name = dataset_name
        self._source_system_name = source_system_name
        self._batch_id = batch_id
        self._recon_config_path = recon_config_path
        self._recon_delta_log_path = recon_delta_log_path
        self._overwrite_log_path_flag = overwrite_log_path_flag

    def _rowcount_check(
            self, df_source: DataFrame, df_target: DataFrame
    ) -> Tuple[str, str, str, str, str, str]:
        """
        _rowcount_checkis a method to check row count.

        :param df_source: source pyspark dataframe
        :param df_target: target pyspark dataframe
        :retrun: None
        """
        source_count = df_source.count()
        target_count = df_target.count()

        res_row = (
            self._batch_id,
            self._application_name,
            self._source_system_name,
            self._load_group_name,
            self._dataset_name,
            "count",
            str(source_count),
            str(target_count)
        )

        return res_row

    def _aggregate_check(
            self, df_source: DataFrame, df_target: DataFrame, agg_config: list
    ) -> List[Tuple[str, str, str, str, str, str]]:
        """
        _aggregate_check is a method to check sum.

        :param df_source: source pyspark dataframe
        :param df_target: target pyspark dataframe
        :param agg_config: aggregation config
        :return: None
        """
        src_value_dict = UDPReconCore.get_agg(df_source, agg_config)
        target_value_dict = UDPReconCore.get_agg(df_target, agg_config)
        agg_log = []

        for key in src_value_dict:
            source_value = src_value_dict[key]
            target_value = target_value_dict[key]
            result_row = (
                self._batch_id,
                self._application_name,
                self._source_system_name,
                self._load_group_name,
                self._dataset_name,
                str(key),
                str(source_value),
                str(target_value)
            )
            agg_log.append(result_row)

        return agg_log

    def _get_config_value(self, config: Row, column_name: str) -> str:
        """
        _get_config_value is a method to get the value from the Row object

        :param config: Row object
        :param column_name: column name as string
        :return: value in the dictionary if available else NA
        """
        try:
            value = config[column_name]
            if value is None:
                value = reconcilation_constants.measure_na_value
        except ValueError:
            # Dont throw exception. Return default value
            value = reconcilation_constants.measure_na_value
        except Exception:
            # Dont throw exception. Return default value
            value = reconcilation_constants.measure_na_value
        return value

    def _load_reconcile_config(self) -> list:
        """
        _load_reconcile_config is a method to generated the aggregated expressions 
        which are defined in the recon configuration json.

        :return: list of aggregated expressions
        """
        recon_config = []

        try:
            recon_config = UDPReconUtilities.get_reconcile_config(
                self.spark, self._recon_config_path
            )
        except Exception as load_recon_config_err:
            raise LoadReconcileConfigErr(
                reconcilation_constants.exception_msg_template_without_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    exception_msg=str(load_recon_config_err)
                )
            )

        agg_exp = []
        for conf in recon_config:
            # Get the key attributes from the config
            col_name = conf[reconcilation_constants.measure_column]
            agg_name = conf[reconcilation_constants.measure_type]

            # Get the optional attributes from the config else use the default values
            dtype_name = self._get_config_value(conf, reconcilation_constants.measure_data_type)
            dtype_format = self._get_config_value(conf, reconcilation_constants.measure_datetime_format)

            # If a datatype hint has been of Date data type
            if dtype_name == reconcilation_constants.measure_date_data_type:
                # if a date format hint also has been given then use it else use default date format
                dtype_format = (
                    reconcilation_constants.measure_default_date_format
                    if dtype_format == reconcilation_constants.measure_na_value
                    else dtype_format
                )
                exp = reconcilation_constants.cast_and_agg_date_column.format(
                    agg=agg_name, colname=col_name, dtype=dtype_name, dtype_format=dtype_format
                )
            # If a datatype hint has been of timestamp data type
            elif dtype_name == reconcilation_constants.measure_timestamp_data_type:
                # if a timestamp format hint also has been given then use it else use default timestamp format
                dtype_format = (
                    reconcilation_constants.measure_default_datetime_format
                    if dtype_format == reconcilation_constants.measure_na_value else dtype_format
                )
                exp = reconcilation_constants.cast_and_agg_timestamp_column.format(
                    agg=agg_name, colname=col_name, dtype=dtype_name, dtype_format=dtype_format
                )
            # If no datatype hint has been given then don't do any type casting. 
            elif dtype_name == reconcilation_constants.measure_na_value:
                exp = reconcilation_constants.agg_only.format(
                    agg=agg_name, col=col_name
                )
            else:
                # If a datatype hint has been which is neither date or timestamp 
                exp = reconcilation_constants.cast_and_agg_column.format(
                    agg=agg_name, colname=col_name, dtype=dtype_name
                )

            agg_exp.append(eval(exp))

        return agg_exp

    def log_reconcile_metrics(
            self, reconcile_stage_name: str, recon_result_log: List[Tuple[str, str, str, str, str, str]]
    ) -> DataFrame:
        log_schema = [
            udp_constants.batch_id,
            udp_constants.application_name,
            udp_constants.source_system_name,
            udp_constants.load_group_name,
            udp_constants.dataset_name,
            reconcilation_constants.audit_measure_type,
            reconcilation_constants.source_measure_value,
            reconcilation_constants.target_measure_value
        ]
        df_reconcile_log = self.spark.createDataFrame(
            data=recon_result_log, schema=log_schema
        )

        # Add Audit Column
        df_reconcile_log = (
            df_reconcile_log.withColumn(
                reconcilation_constants.recon_status,
                when(
                    col(reconcilation_constants.source_measure_value) == col(reconcilation_constants.target_measure_value),
                    lit("Pass")
                ).otherwise(lit("Fail"))
            )
            .withColumn(reconcilation_constants.reconcile_stage, lit(reconcile_stage_name))
            .withColumn(reconcilation_constants.recon_log_date, current_timestamp())
            .withColumn(udp_constants.created_by, lit('reconciliation_framework'))
        )
        
        audit_cols = [
            udp_constants.created_timestamp,
            udp_constants.created_by,
            udp_constants.batch_id,
            udp_constants.application_name,
            udp_constants.source_system_name,
            udp_constants.load_group_name,
            udp_constants.dataset_name
        ]
        all_cols = \
            audit_cols + [column for column in df_reconcile_log.columns if column not in audit_cols]

        df_reconcile_log = df_reconcile_log.select(all_cols)

        try:
            if self._recon_delta_log_path:
                # Save to Recon Log Delta table
                # path provided for reconciliation metrics logging.
                # Do nothing if no path is provided
                UDPDeltaTableUtils.write_to_delta_table(
                    self.spark,
                    self._recon_delta_log_path,
                    df_reconcile_log,
                    self._overwrite_log_path_flag
                )
        except ValueError as ve:
            raise InputValueError(
                reconcilation_constants.exception_msg_template_with_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    argument_values=reconcile_stage_name,
                    exception_msg=str(ve)
                )
            )
        except Exception as e:
            raise LogReconcileMetricsErr(
                reconcilation_constants.exception_msg_template_with_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    argument_values=reconcile_stage_name,
                    exception_msg=str(e)
                )
            )

        return df_reconcile_log

    def perform_reconciliation(
            self,
            df_source: DataFrame,
            df_target: DataFrame,
            reconcile_stage_name: str
    ) -> DataFrame:
        """
        perform_reconciliation is a method to perform reconcilation.

        :param df_source: source pyspark dataframe
        :param df_target: target pyspark dataframe
        :param reconcile_stage_name: reconcilation stage name
        :retrun: DataFrame
        """
        recon_result = []
        try:
            agg_config = self._load_reconcile_config()
            # Default Count check should be done everytime
            res_count = self._rowcount_check(df_source, df_target)
            recon_result.append(res_count)

            if len(agg_config) > 0:
                res_agg = self._aggregate_check(
                    df_source, df_target, agg_config
                )
                recon_result.extend(res_agg)
        except ValueError as ve:
            raise InputValueError(
                reconcilation_constants.exception_msg_template_with_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    argument_values=reconcile_stage_name,
                    exception_msg=str(ve)
                )
            )
        except Exception as perform_recon_err:
            recon_error = (
                self._batch_id,
                self._application_name,
                self._source_system_name,
                self._load_group_name,
                self._dataset_name,
                reconcilation_constants.error_recon,
                "-1",
                "-1"
            )
            recon_result.append(recon_error)
            raise PerformReconciliationErr(
                reconcilation_constants.exception_msg_template_with_args.format(
                    class_name=self.__class__.__name__,
                    method_name=inspect.currentframe().f_code.co_name,
                    argument_values=reconcile_stage_name,
                    exception_msg=str(perform_recon_err)
                )
            )

        return self.log_reconcile_metrics(reconcile_stage_name, recon_result)
