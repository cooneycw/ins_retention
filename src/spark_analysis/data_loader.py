"""
PySpark Data Loader Module

Provides functionality to load and prepare data using PySpark DataFrames.
Optimized for large-scale insurance data processing.
"""

import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, DateType, TimestampType
)
from pyspark.sql.functions import col, to_date, when, isnan, isnull


class InforceDataLoader:
    """
    PySpark-based data loader for inforce monthly view data.
    
    This class provides methods to load, validate, and prepare the inforce
    monthly view CSV data using PySpark DataFrames for efficient processing.
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the InforceDataLoader.
        
        Args:
            spark_session: Optional SparkSession. If None, creates a new one.
        """
        self.spark = spark_session or self._create_spark_session()
        self.config = self._load_config()
        self.schema = self._define_schema()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure a SparkSession for data processing."""
        return SparkSession.builder \
            .appName("RetentionAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = Path("config/policy_system_config.yaml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _define_schema(self) -> StructType:
        """
        Define the schema for the inforce monthly view data.
        
        Returns:
            StructType: PySpark schema for the inforce data
        """
        return StructType([
            StructField("inforce_yy", IntegerType(), True),
            StructField("inforce_mm", IntegerType(), True),
            StructField("policy", StringType(), True),
            StructField("policy_expiry_date", StringType(), True),  # Will convert to date
            StructField("client_tenure_days", IntegerType(), True),
            StructField("vehicle_no", StringType(), True),
            StructField("vehicle_vin", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("model_year", IntegerType(), True),
            StructField("driver_no", StringType(), True),
            StructField("driver_license_no", StringType(), True),
            StructField("driver_name", StringType(), True),
            StructField("driver_type", StringType(), True),
            StructField("assignment_type", StringType(), True),
            StructField("exposure_factor", DoubleType(), True),
            StructField("driver_age", IntegerType(), True),
            StructField("premium_rate", DoubleType(), True),
            StructField("premium_paid", DoubleType(), True)
        ])
    
    def load_inforce_data(self, file_path: Optional[str] = None) -> DataFrame:
        """
        Load the inforce monthly view data from CSV file.
        
        Args:
            file_path: Optional path to CSV file. If None, uses config path.
            
        Returns:
            DataFrame: PySpark DataFrame with inforce data
        """
        if file_path is None:
            inforce_path = Path(self.config['file_paths']['data']['inforce']) / \
                          self.config['data_files']['inforce']['monthly_view']
            file_path = str(inforce_path)
        
        print(f"Loading inforce data from: {file_path}")
        
        # Load CSV with defined schema
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.schema) \
            .csv(file_path)
        
        # Convert policy_expiry_date to proper date format
        df = df.withColumn(
            "policy_expiry_date", 
            to_date(col("policy_expiry_date"), "yyyy-MM-dd")
        )
        
        # Cache the DataFrame for better performance
        df.cache()
        
        print(f"Loaded {df.count()} records")
        return df
    
    def validate_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate the loaded data and return summary statistics.
        
        Args:
            df: PySpark DataFrame to validate
            
        Returns:
            Dict containing validation results and statistics
        """
        print("Validating inforce data...")
        
        # Basic statistics
        total_records = df.count()
        total_policies = df.select("policy").distinct().count()
        total_vehicles = df.select("vehicle_no").distinct().count()
        total_drivers = df.select("driver_no").distinct().count()
        
        # Date range
        date_range = df.select(
            col("inforce_yy").alias("year"),
            col("inforce_mm").alias("month")
        ).distinct().orderBy("year", "month").collect()
        
        min_date = f"{date_range[0]['year']}-{date_range[0]['month']:02d}"
        max_date = f"{date_range[-1]['year']}-{date_range[-1]['month']:02d}"
        
        # Check for null values in key columns
        null_counts = {}
        key_columns = ["policy", "vehicle_no", "driver_no", "premium_paid"]
        
        for col_name in key_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count
        
        # Premium statistics
        premium_stats = df.select("premium_paid").describe().collect()
        premium_summary = {row['summary']: row['premium_paid'] for row in premium_stats}
        
        validation_results = {
            "total_records": total_records,
            "total_policies": total_policies,
            "total_vehicles": total_vehicles,
            "total_drivers": total_drivers,
            "date_range": {"min": min_date, "max": max_date},
            "null_counts": null_counts,
            "premium_summary": premium_summary
        }
        
        # Print validation summary
        print(f"  - Total records: {total_records:,}")
        print(f"  - Unique policies: {total_policies:,}")
        print(f"  - Unique vehicles: {total_vehicles:,}")
        print(f"  - Unique drivers: {total_drivers:,}")
        print(f"  - Date range: {min_date} to {max_date}")
        print(f"  - Null values in key columns: {null_counts}")
        
        return validation_results
    
    def get_data_summary(self, df: DataFrame) -> DataFrame:
        """
        Get a summary DataFrame with key statistics.
        
        Args:
            df: PySpark DataFrame to summarize
            
        Returns:
            DataFrame: Summary statistics
        """
        return df.select(
            col("inforce_yy"),
            col("inforce_mm"),
            col("policy"),
            col("vehicle_type"),
            col("driver_type"),
            col("premium_paid"),
            col("driver_age")
        ).describe()
    
    def filter_by_date_range(self, df: DataFrame, start_year: int, start_month: int, 
                           end_year: int, end_month: int) -> DataFrame:
        """
        Filter DataFrame by date range.
        
        Args:
            df: Input DataFrame
            start_year: Start year
            start_month: Start month
            end_year: End year
            end_month: End month
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        return df.filter(
            ((col("inforce_yy") > start_year) | 
             ((col("inforce_yy") == start_year) & (col("inforce_mm") >= start_month))) &
            ((col("inforce_yy") < end_year) | 
             ((col("inforce_yy") == end_year) & (col("inforce_mm") <= end_month)))
        )
    
    def get_monthly_inforce(self, df: DataFrame) -> DataFrame:
        """
        Get monthly inforce policy counts.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Monthly inforce statistics
        """
        return df.groupBy("inforce_yy", "inforce_mm") \
            .agg({
                "policy": "countDistinct",
                "premium_paid": "mean",
                "driver_age": "mean"
            }) \
            .orderBy("inforce_yy", "inforce_mm")
    
    def close(self):
        """Close the SparkSession."""
        if self.spark:
            self.spark.stop()
