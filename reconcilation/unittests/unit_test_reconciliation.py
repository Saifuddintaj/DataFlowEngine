import json
import os
import unittest
import uuid

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from common.constants import udp_constants
from reconcilation.constants import reconcilation_constants
from reconcilation.udp_reconcilation import UDPReconcilation
from reconcilation.udp_reconcilation_exception import PerformReconciliationErr


class UnitTestReconciliation(unittest.TestCase):
    """
    This class demo how to setup test case and run test use python unittest module.
    """

    # This method will be executed only once for this test case class.
    # It will execute before all test methods. Must decorated with
    # @classmethod.
    
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.config(
                "spark.jars.packages", "io.delta:delta-core_2.12:0.8.0"
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
        cls.dbutils = DBUtils(cls.spark)
        currentwd = os.getcwd()

        local_file_path = "file:" + currentwd + "/unittests/configFiles/"
        cls.dbutils.fs.cp(local_file_path, "dbfs:/FileStore/ReconConfig/", True)
        
        print("setUpClass execute. ")

    @classmethod
    def drop_delta_table(cls, delta_path):
        try:
            cls.dbutils.fs.rm(delta_path, True)
            # dbutils.fs.rm(delta_path, True)
        except Exception:
            # Dont fail if the passed path does not exists.
            # This is used to clean up any delta table created during tests
            pass

    # Similar with setupClass method, it will be executed after all test
    # method run.
    @classmethod
    def tearDownClass(cls):
        recon_log_path = "dbfs:/tmp/test_LogExternalReconOutputToDelta/"
        cls.drop_delta_table(recon_log_path)
        recon_log_path = "dbfs:/tmp/test_ReconLogConfigSettingsCheck/"
        cls.drop_delta_table(recon_log_path)
        cls.spark.stop()
        print("tearDownClass execute. ")

    # This method will be executed before each test function.
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.default_config_dbfs_path = "./ReconConfigSample.json"
        print("setUp method execute. ")

    # This method will be executed after each test function.
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        print("tearDown method execute. ")

    def runDataReconciliation(
        self,
        source_data,
        source_schema,
        target_data,
        target_schema,
        recon_config_path="",
        recon_log_path="",
        batch_id=-1
    ):
        source_system_name = "TESTSource"
        dataset_name = "BU"
        application_name = "test_application"
        load_group_name = "test_load_group"
        reconcile_stage_name = "Test vs Target"
        df_source = self.spark.createDataFrame(
            data=source_data, schema=source_schema
        )
        df_target = self.spark.createDataFrame(
            data=target_data, schema=target_schema
        )

        self.expected_col_name_list = [
            udp_constants.created_timestamp,
            udp_constants.created_by,
            udp_constants.batch_id,
            udp_constants.application_name,
            udp_constants.source_system_name,
            udp_constants.load_group_name,
            udp_constants.dataset_name,
            reconcilation_constants.audit_measure_type,
            reconcilation_constants.source_measure_value,
            reconcilation_constants.target_measure_value,
            reconcilation_constants.recon_status,
            reconcilation_constants.reconcile_stage
        ]
        obj_recon = UDPReconcilation(
            self.spark,
            batch_id,
            application_name,
            source_system_name,
            load_group_name,
            dataset_name,
            recon_config_path,
            recon_log_path
        )
        df_recon_output = obj_recon.perform_reconciliation(
            df_source, df_target, reconcile_stage_name
        )

        return df_recon_output

    def test_ReconLogDataModel(self):
        sample_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        df_recon_output = self.runDataReconciliation(
            sample_data, sample_schema, sample_data, sample_schema
        )
        report_col_name_list = df_recon_output.columns
        self.assertEqual(
            report_col_name_list,
            self.expected_col_name_list,
            "Recon Metrics Data Model Not matching with expected columns"
        )

    def test_ReconLogDefaultCountRowCheck(self):
        sample_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        df_recon_output = self.runDataReconciliation(
            sample_data, sample_schema, sample_data, sample_schema
        )

        default_count_rows = df_recon_output.filter(f"{reconcilation_constants.audit_measure_type} = 'count'").count()
        self.assertEqual(
            default_count_rows,
            1,
            "Recon Metrics default for measure type count not present"
        )

    def test_ReconLogDefaultCountRowDataCheck(self):
        sample_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        df_recon_output = self.runDataReconciliation(
            sample_data, sample_schema, sample_data, sample_schema
        )

        count_row = df_recon_output.filter(f"{reconcilation_constants.audit_measure_type} = 'count'").collect()[0]
        source_rows = count_row[reconcilation_constants.source_measure_value]
        target_rows = count_row[reconcilation_constants.target_measure_value]
        recon_report_status = count_row[reconcilation_constants.recon_status]

        expected_report_status = "Pass" if source_rows == target_rows else "Fail"

        self.assertEqual(
            expected_report_status,
            recon_report_status,
            "Recon Metrics report status is incorrect for measure type count"
        )

    def test_ReconLogDefaultCountRowMismatchDataCheck(self):
        sample_source_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_target_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        df_recon_output = self.runDataReconciliation(
            sample_source_data, sample_schema, sample_target_data, sample_schema
        )

        count_row = df_recon_output.filter(f"{reconcilation_constants.audit_measure_type} = 'count'").collect()[0]
        # print(count_row)
        source_rows = int(count_row[reconcilation_constants.source_measure_value])
        target_rows = int(count_row[reconcilation_constants.target_measure_value])
        recon_report_status = count_row[reconcilation_constants.recon_status]

        expected_report_status = "Fail"
        self.assertEqual(
            expected_report_status,
            recon_report_status,
            "Recon Metrics report status is incorrect for measure type count"
        )
        self.assertEqual(
            (source_rows - target_rows),
            1,
            "Recon Metrics report measure values for measure type count as incorrect"
        )

    def createDummyConfig(self, config, config_path):
        with open(config_path, "w") as f:
            json.dump(config, f)

    def test_ReconLogNonReconMismatchSchemaCheck(self):
        sample_source_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_source_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        sample_target_schema = StructType(
            [
                StructField("FirstNM", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        config_dbfs_path = "dbfs:/FileStore/ReconConfig/test_ReconLogNonReconMismatchSchemaCheck.json"
        df_recon_output = self.runDataReconciliation(
            sample_source_data,
            sample_source_schema,
            sample_source_data,
            sample_target_schema,
            config_dbfs_path
        )
        sum_rows = df_recon_output.filter(f"{reconcilation_constants.audit_measure_type} = 'sum_salary'")
        sum_recon_rows_count = sum_rows.count()
        sum_row_list = sum_rows.collect()[0]

        source_rows = sum_row_list[reconcilation_constants.source_measure_value]
        target_rows = sum_row_list[reconcilation_constants.target_measure_value]
        recon_report_status = sum_row_list[reconcilation_constants.recon_status]

        expected_report_status = "Pass"

        self.assertEqual(
            sum_recon_rows_count,
            1,
            "Recon Metrics for measure sum(salary) should be a single in row"
        )
        self.assertEqual(
            source_rows,
            target_rows,
            "Recon Metrics for measure sum(salary) are expected to be matching."
        )
        self.assertEqual(
            recon_report_status,
            expected_report_status,
            "Recon Metrics status for measure sum(salary) should be PASS"
        )

    def test_ReconLogConfigSettingsCheck(self):
        sample_data = [
            ("James", "", "Smith", "1", "M", 1),
            ("Michael", "Rose", "", "10", "M", 10),
            ("Robert", "", "Williams", "100", "M", 100),
            ("Maria", "Anne", "Jones", "1000", "F", 1000),
            ("Jen", "Mary", "Brown", "10000", "F", 10000)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        config_path = (
            "dbfs:/FileStore/ReconConfig/test_ReconLogConfigSettingsCheck.json"
        )

        recon_log_path = "dbfs:/tmp/test_ReconLogConfigSettingsCheck/"

        batch_id = str(uuid.uuid4())

        self.runDataReconciliation(
            sample_data,
            sample_schema,
            sample_data,
            sample_schema,
            config_path,
            recon_log_path,
            batch_id
        )
        df_recon_output = (
            self.spark.read.format("delta")
            .load(recon_log_path)
            .filter("{} = '{}'".format(udp_constants.batch_id, batch_id))
        )

        count_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'count'"
        ).count()
        sum_salary_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'sum_salary'"
        ).count()
        max_salary_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'max_salary'"
        ).count()
        sum_id_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'sum_id'"
        ).count()
        error_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'error-recon'"
        ).count()

        self.assertEqual(
            count_recon_rows_count,
            1,
            "Recon Metrics for measure count should be 1 row by default."
        )
        self.assertEqual(
            sum_salary_recon_rows_count,
            1,
            "Recon Metrics for measure sum_salary should be present."
        )
        self.assertEqual(
            error_recon_rows_count,
            0,
            "Recon Metrics should not have any error-recon rows"
        )
        self.assertEqual(
            max_salary_recon_rows_count,
            1,
            "Recon Metrics for measure max_salary should be present."
        )
        self.assertEqual(
            sum_id_recon_rows_count,
            1,
            "Recon Metrics for measure sum_id should be present."
        )

    def test_LogExternalReconOutputToDelta(self):
        recon_result = []
        source_system_name = "TESTSource"
        dataset_name = "BU"
        application_name = "test_application"
        load_group_name = "test_load_group"
        reconcile_stage_name = "Test vs Target"
        batch_id = str(uuid.uuid4())

        recon_default = (
            batch_id,
            application_name,
            source_system_name,
            load_group_name,
            dataset_name,
            "ADF verified consistency",
            "NA",
            "NA"
        )
        recon_result.append(recon_default)

        config_path = ""
        recon_log_path = "dbfs:/tmp/test_LogExternalReconOutputToDelta/"

        obj_recon = UDPReconcilation(
            self.spark,
            batch_id,
            application_name,
            source_system_name,
            load_group_name,
            dataset_name,
            config_path,
            recon_log_path
        )
        obj_recon.log_reconcile_metrics(reconcile_stage_name, recon_result)

        df_recon_output = (
            self.spark.read.format("delta")
            .load(recon_log_path)
            .filter("{} = '{}'".format(udp_constants.batch_id, batch_id))
        )
        adf_recon_rows_count = df_recon_output.filter(
            f"{reconcilation_constants.audit_measure_type} = 'ADF verified consistency' and recon_status = 'Pass'"
        ).count()
        self.assertEqual(
            adf_recon_rows_count,
            1,
            "ADF Verified Consistency should be 1 row by default with PASS status"
        )

    def test_WrongAggregationFunctionRaiseCheck(self):
        sample_source_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_source_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        sample_target_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        config = [{"MeasureType": "summation", "MeasureColumn": "salary"}]

        config_dbfs_path = self.default_config_dbfs_path
        self.createDummyConfig(config, config_dbfs_path)

        with self.assertRaises(PerformReconciliationErr):
            self.runDataReconciliation(
                sample_source_data,
                sample_source_schema,
                sample_source_data,
                sample_target_schema,
                config_dbfs_path
            )

    def test_WrongJSONConfigRaiseCheck(self):
        sample_source_data = [
            ("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
        ]

        sample_source_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        sample_target_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        config = [{"MeasureType": "count", "MeasureColumn": ["salary", "id"]}]

        config_dbfs_path = self.default_config_dbfs_path
        self.createDummyConfig(config, config_dbfs_path)

        with self.assertRaises(PerformReconciliationErr):
            self.runDataReconciliation(
                sample_source_data,
                sample_source_schema,
                sample_source_data,
                sample_target_schema,
                config_dbfs_path
            )

    def test_WrongMeasureColumnKeyCheck(self):
        sample_data = [
            ("James", "", "Smith", "1", "M", 1),
            ("Michael", "Rose", "", "10", "M", 10),
            ("Robert", "", "Williams", "100", "M", 100),
            ("Maria", "Anne", "Jones", "1000", "F", 1000),
            ("Jen", "Mary", "Brown", "10000", "F", 10000)
        ]

        sample_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
                StructField("id", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True)
            ]
        )

        # config json should have
        config = [
            {"MeasureType": "sum", "MeasureColumnName": "salary"},
            {"MeasureType": "max", "MeasureColumnName": "salary"},
            {"MeasureType": "sum", "MeasureColumnName": "id"},
            {"MeasureType": "avg", "MeasureColumnName": "salary"},
            {"MeasureType": "min", "MeasureColumnName": "id"}
        ]

        recon_log_path = "dbfs:/tmp/ReconLog/"
        config_dbfs_path = self.default_config_dbfs_path
        self.createDummyConfig(config, config_dbfs_path)

        batch_id = str(uuid.uuid4())

        with self.assertRaises(PerformReconciliationErr):
            self.runDataReconciliation(
                sample_data,
                sample_schema,
                sample_data,
                sample_schema,
                config_dbfs_path,
                recon_log_path,
                batch_id
            )
