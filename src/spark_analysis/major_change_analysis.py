"""
PySpark Major Change Analysis Module

Detects major changes in policies using PySpark DataFrames.
Major changes include vehicle additions/substitutions, driver changes, etc.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lag, when, isnan, isnull, sum as spark_sum, count, 
    countDistinct, avg, max as spark_max, min as spark_min,
    row_number, window, lit, coalesce, abs as spark_abs,
    concat, collect_set
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
        
        Major changes are detected by comparing the set of driver-vehicle assignments
        between consecutive months for each policy.
        
        Args:
            df: Input DataFrame with inforce data
            
        Returns:
            DataFrame with major_change flag added
        """
        print("  - Detecting major changes using PySpark...")
        
        # Create a unique identifier for each driver-vehicle assignment
        df_with_assignment_id = df.withColumn(
            "assignment_id", 
            concat(col("vehicle_no"), lit("_"), col("driver_no"))
        )
        
        # Group by policy and month to get the set of assignments for each month
        monthly_assignments = df_with_assignment_id.groupBy("policy", "inforce_yy", "inforce_mm") \
            .agg(collect_set("assignment_id").alias("assignments"))
        
        # Define window for policy-level ordering
        policy_window = Window.partitionBy("policy").orderBy("inforce_yy", "inforce_mm")
        
        # Add lag column to compare with previous month's assignments
        monthly_assignments_with_lag = monthly_assignments.withColumn(
            "prev_assignments", lag("assignments", 1).over(policy_window)
        )
        
        # Detect major changes by comparing assignment sets
        monthly_changes = monthly_assignments_with_lag.withColumn(
            "major_change",
            when(col("prev_assignments").isNull(), lit(False))  # First month is not a change
            .when(col("assignments") != col("prev_assignments"), lit(True))  # Different assignments = change
            .otherwise(lit(False))  # Same assignments = no change
        )
        
        # Join back to original DataFrame to add major_change flag to all records
        final_df = df_with_assignment_id.join(
            monthly_changes.select("policy", "inforce_yy", "inforce_mm", "major_change"),
            ["policy", "inforce_yy", "inforce_mm"],
            "left"
        ).drop("assignment_id")  # Remove the temporary assignment_id column
        
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
