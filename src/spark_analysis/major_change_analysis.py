"""
PySpark Major Change Analysis Module

Detects major changes in policies using PySpark DataFrames.
Major changes include vehicle additions/substitutions, driver changes, etc.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lag, when, isnan, isnull, sum as spark_sum, count, 
    countDistinct, avg, max as spark_max, min as spark_min,
    row_number, window, lit, coalesce, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
from pathlib import Path
from typing import Dict, Any


class MajorChangeAnalysis:
    """
    PySpark-based major change detection for insurance policies.
    
    Major changes include:
    - Vehicle additions/substitutions
    - Driver changes (excluding normal aging)
    - Policy cancellations/reinstatements
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize the major change analysis.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def detect_major_changes(self, df: DataFrame) -> DataFrame:
        """
        Detect major changes for each policy-month combination.
        
        Args:
            df: Input DataFrame with inforce data
            
        Returns:
            DataFrame with major_change flag added
        """
        print("  - Detecting major changes using PySpark...")
        
        # Define window for policy-level ordering
        policy_window = Window.partitionBy("policy").orderBy("inforce_yy", "inforce_mm")
        
        # Add row number for each policy's records
        df_with_row_num = df.withColumn("row_num", row_number().over(policy_window))
        
        # Add lag columns for comparison with previous month
        df_with_lags = df_with_row_num.withColumn(
            "prev_vehicle_no", lag("vehicle_no", 1).over(policy_window)
        ).withColumn(
            "prev_vehicle_vin", lag("vehicle_vin", 1).over(policy_window)
        ).withColumn(
            "prev_vehicle_type", lag("vehicle_type", 1).over(policy_window)
        ).withColumn(
            "prev_model_year", lag("model_year", 1).over(policy_window)
        ).withColumn(
            "prev_driver_no", lag("driver_no", 1).over(policy_window)
        ).withColumn(
            "prev_driver_license_no", lag("driver_license_no", 1).over(policy_window)
        ).withColumn(
            "prev_driver_name", lag("driver_name", 1).over(policy_window)
        ).withColumn(
            "prev_driver_type", lag("driver_type", 1).over(policy_window)
        ).withColumn(
            "prev_driver_age", lag("driver_age", 1).over(policy_window)
        )
        
        # Detect major changes
        df_with_changes = df_with_lags.withColumn(
            "vehicle_change",
            when(col("row_num") == 1, lit(True))  # First record is always a change
            .when(
                (col("vehicle_no") != col("prev_vehicle_no")) |
                (col("vehicle_vin") != col("prev_vehicle_vin")) |
                (col("vehicle_type") != col("prev_vehicle_type")) |
                (col("model_year") != col("prev_model_year")),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "driver_change",
            when(col("row_num") == 1, lit(True))  # First record is always a change
            .when(
                (col("driver_no") != col("prev_driver_no")) |
                (col("driver_license_no") != col("prev_driver_license_no")) |
                (col("driver_name") != col("prev_driver_name")) |
                (col("driver_type") != col("prev_driver_type")) |
                (spark_abs(col("driver_age") - col("prev_driver_age")) > 1),  # Significant age change
                lit(True)
            ).otherwise(lit(False))
        )
        
        # Combine changes into major_change flag
        df_with_major_changes = df_with_changes.withColumn(
            "major_change",
            when(col("vehicle_change") | col("driver_change"), lit(True))
            .otherwise(lit(False))
        )
        
        # Drop helper columns
        final_df = df_with_major_changes.drop(
            "row_num", "prev_vehicle_no", "prev_vehicle_vin", "prev_vehicle_type",
            "prev_model_year", "prev_driver_no", "prev_driver_license_no",
            "prev_driver_name", "prev_driver_type", "prev_driver_age",
            "vehicle_change", "driver_change"
        )
        
        print("  - Major change detection completed")
        return final_df
    
    def generate_change_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate a summary of major changes detected.
        
        Args:
            df: DataFrame with major_change flag
            
        Returns:
            Dictionary with change statistics
        """
        print("  - Generating major change summary...")
        
        # Overall statistics
        total_records = df.count()
        records_with_changes = df.filter(col("major_change") == True).count()
        change_percentage = (records_with_changes / total_records) * 100 if total_records > 0 else 0
        
        # Policy-level statistics
        policy_stats = df.groupBy("policy").agg(
            count("*").alias("total_records"),
            spark_sum(when(col("major_change") == True, 1).otherwise(0)).alias("change_count")
        )
        
        total_policies = policy_stats.count()
        policies_with_changes = policy_stats.filter(col("change_count") > 0).count()
        policy_change_rate = (policies_with_changes / total_policies) * 100 if total_policies > 0 else 0
        
        # Yearly changes
        yearly_changes = df.groupBy("inforce_yy").agg(
            count("*").alias("total_records"),
            spark_sum(when(col("major_change") == True, 1).otherwise(0)).alias("change_count")
        ).withColumn("change_rate", (col("change_count") / col("total_records")) * 100)
        
        # Monthly changes
        monthly_changes = df.groupBy("inforce_yy", "inforce_mm").agg(
            count("*").alias("total_records"),
            spark_sum(when(col("major_change") == True, 1).otherwise(0)).alias("change_count")
        ).withColumn("change_rate", (col("change_count") / col("total_records")) * 100)
        
        summary = {
            'total_records': total_records,
            'records_with_changes': records_with_changes,
            'change_percentage': change_percentage,
            'total_policies': total_policies,
            'policies_with_changes': policies_with_changes,
            'policy_change_rate': policy_change_rate
        }
        
        # Print summary
        print(f"    Total records: {total_records:,}")
        print(f"    Records with major changes: {records_with_changes:,} ({change_percentage:.1f}%)")
        print(f"    Total policies: {total_policies:,}")
        print(f"    Policies with changes: {policies_with_changes:,} ({policy_change_rate:.1f}%)")
        
        return summary
    
    def save_updated_dataframe(self, df: DataFrame, output_path: str) -> None:
        """
        Save the DataFrame with major change flags to CSV.
        
        Args:
            df: DataFrame with major_change flag
            output_path: Path to save the CSV file
        """
        print(f"  - Saving updated DataFrame to {output_path}")
        
        # Sort by policy, year, month for consistent output
        sorted_df = df.orderBy("policy", "inforce_yy", "inforce_mm")
        
        # Write to CSV
        sorted_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        print("  - DataFrame saved successfully")
