"""
This file contains the class and method used for processing UDP Reconcilation Utilities.

class UDPReconUtilities: class for the reconcilation framework
"""

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pyspark.sql.utils

# These functions are used dynamically by the reconciliation engine
# Don't remove the below imports
from pyspark.sql.functions import min, max, sum, avg, mean, count

from reconcilation.udp_reconcilation_exception import LoadReconcileConfigErr
from common.core.udp_common import UDPCommon


class UDPReconUtilities:
    """
    UDPReconUtilities is the utility class for the reconcilation framework.

    get reconcile config -- get reconcilation configuration
    """

    @staticmethod
    def get_reconcile_config(
        spark: SparkSession, recon_entity_config_path: DataFrame
    ) -> list:
        """
        get_reconcile_configis a method to get reconciliation configuration.


        :param spark: Sparksession
        :param recon_entity_config_path: config entity path
        :return: reconcilation list
        """
        recon_list = []
        try:
            if len(recon_entity_config_path) > 1:
                # read config if a path is passed
                recon_config_df = (
                    spark.read.option("multiline", "true")
                    .format("json")
                    .load(recon_entity_config_path)
                )

                recon_list = recon_config_df.collect()
            else:
                recon_list = []
        except pyspark.sql.utils.AnalysisException as ae:
            if "Path does not exist" in str(ae):
                # Don't raise exception if the file is not present.
                # Only raise exception if the file couldn't be read properly
                recon_list = []
            else:
                raise LoadReconcileConfigErr
        except Exception:
            recon_list = []
            raise LoadReconcileConfigErr

        return recon_list
