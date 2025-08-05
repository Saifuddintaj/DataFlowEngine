"""
This file contains the classes and methods used for reconcilation framework errors.

class PerformReconciliationErr(Exception): failed to perform reconcilation error
class LoadReconcileConfigErr(Exception): failed to load reconcilation configuration error
class LogReconcileMetricsErr(Exception): failed to log reconcilation metrics error
"""
from reconcilation.constants import reconcilation_constants


class PerformReconciliationErr(Exception):
    """
    PerformReconciliationErr is the user defined exception class which is used to raise
    exceptions while performing reconcilation.

    """
    def __init__(
        self,
        message=reconcilation_constants.perform_reconciliation_error_message
    ):
        super().__init__(message)


class LoadReconcileConfigErr(Exception):
    """
    LoadReconcileConfigErr is the user defined exception class which is used to raise
    exceptions while loading reconcilation configuration.

    """
    def __init__(
        self,
        message=reconcilation_constants.load_reconcile_config_error_message
    ):
        super().__init__(message)


class LogReconcileMetricsErr(Exception):
    """
    LogReconcileMetricsErr is the user defined exception class which is used to raise
    exceptions while logging reconcilation metrics.

    """
    def __init__(
        self,
        message=reconcilation_constants.log_reconcile_metrics_error_message
    ):
        super().__init__(message)
