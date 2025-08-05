"""
This file contains the classes and methods used for processing
UDP Reconcilation Core.

class UDPReconCore: class for data reconcilation related reusable methods.
"""

from pyspark.sql.dataframe import DataFrame

# These functions are used dynamically by the reconciliation engine
# Don't remove the below imports
from pyspark.sql.functions import min, max, sum, avg, mean, count


# These below imports are referred dynamically during aggregate computation.
class UDPReconCore:
    """
    udpReconcilationUtil is the class responsible for data reconcilation
    related reusable methods.

    get_count -- to get count of rows
    get_agg -- to get aggregation based on configuration
    """

    @staticmethod
    def get_count(sdf: DataFrame) -> int:
        """
        get_count is a method to calculate count rows of a dataframe.

        :param sdf: pyspark dataframe
        :retrun: count with datatype int
        """
        return sdf.count()

    @staticmethod
    def get_agg(sdf: DataFrame, agg_config: list) -> dict:
        """
        get_agg is a method to calculate aggregation based on config.

        :param sdf: pyspark dataframe
        :param agg_config: aggregation config
        :return: aggregate value with type dict
        """
        sdf_agg = sdf.agg(*agg_config)
        sdf_agg_list = sdf_agg.collect()
        agg_value_dict = {}
        for col in sdf_agg.columns:
            try:
                agg_value = sdf_agg_list[0][col]
            except Exception:
                agg_value = 0.0

            agg_value_dict[col] = str(agg_value)
        return agg_value_dict
