"""
This file contains the class for processing reconciliation constants

class _UDPReconcilationConstants: assigns constant values used for UDP data reconcilation framework
"""
import sys
import os
import yaml


class _UDPReconcilationConstants:
    class ConstError(TypeError):
        pass

    def __setattr__(self, key, value) -> None:
        if key in self.__dict__:
            raise self.ConstError("Can't rebind const(%s)" % key)
        self.__dict__[key] = value


sys.modules[__name__] = _UDPReconcilationConstants()

# Load the YAML file containing constant values
yamlFile = os.path.join(
    os.path.dirname(__file__) + "/reconcilation_constants_repo.yaml"
)
with open(yamlFile) as file:
    constants = yaml.safe_load(file)

# Set constant values from yaml file to property
from . import reconcilation_constants

reconcilation_constants.perform_reconciliation_error_message = constants[
    "perform_reconciliation_error_message"
]
reconcilation_constants.load_reconcile_config_error_message = constants[
    "load_reconcile_config_error_message"
]
reconcilation_constants.log_reconcile_metrics_error_message = constants[
    "log_reconcile_metrics_error_message"
]
reconcilation_constants.error_recon = constants["error_recon"]
reconcilation_constants.agg_only = constants["agg_only"]
reconcilation_constants.cast_and_agg_column = constants["cast_and_agg_column"]
reconcilation_constants.cast_and_agg_date_column = constants["cast_and_agg_date_column"]
reconcilation_constants.cast_and_agg_timestamp_column = constants["cast_and_agg_timestamp_column"]
reconcilation_constants.measure_column = constants["measure_column"]
reconcilation_constants.measure_type = constants["measure_type"]
reconcilation_constants.measure_data_type = constants["measure_data_type"]
reconcilation_constants.measure_string_data_type = constants["measure_string_data_type"]
reconcilation_constants.measure_date_data_type = constants["measure_date_data_type"]
reconcilation_constants.measure_timestamp_data_type = constants["measure_timestamp_data_type"]
reconcilation_constants.measure_datetime_format = constants["measure_datetime_format"]
reconcilation_constants.measure_default_date_format = constants["measure_default_date_format"]
reconcilation_constants.measure_default_datetime_format = constants["measure_default_datetime_format"]
reconcilation_constants.measure_na_value = constants["measure_na_value"]
reconcilation_constants.exception_msg_template_with_args = constants[
    "exception_msg_template_with_args"
]
reconcilation_constants.exception_msg_template_without_args = constants[
    "exception_msg_template_without_args"
]
reconcilation_constants.recon_phase_name = constants["recon_phase_name"]
reconcilation_constants.source_measure_value = constants["source_measure_value"]
reconcilation_constants.target_measure_value = constants["target_measure_value"]
reconcilation_constants.audit_measure_type = constants["audit_measure_type"]
reconcilation_constants.recon_status = constants["recon_status"]
reconcilation_constants.reconcile_stage = constants["reconcile_stage"]
reconcilation_constants.recon_log_date = constants["recon_log_date"]
