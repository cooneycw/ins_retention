"""
Base Analysis Module for PySpark

Provides foundational analysis functions and utilities for insurance data analysis.
This module contains common operations that can be reused across different analysis types.
"""

from typing import Dict, List, Any, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, variance, when, isnan, isnull, 
    year, month, dayofmonth, datediff, current_date, lit, 
    row_number, rank, dense_rank, percent_rank, ntile
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class BaseAnalysis:
    """
    Base class for PySpark-based data analysis.
    
    Provides common analysis functions and utilities that can be extended
    by specific analysis modules.
    """
    
    def __init__(self, spark_session: SparkSession):
        """
        Initialize BaseAnalysis.
        
        Args:
            spark_session: Active SparkSession
        """
        self.spark = spark_session
    
    def get_basic_stats(self, df: DataFrame, numeric_columns: List[str]) -> DataFrame:
        """
        Get basic statistics for numeric columns.
        
        Args:
            df: Input DataFrame
            numeric_columns: List of numeric column names
            
        Returns:
            DataFrame: Statistics summary
        """
        stats_exprs = []
        for col_name in numeric_columns:
            stats_exprs.extend([
                avg(col(col_name)).alias(f"{col_name}_mean"),
                spark_min(col(col_name)).alias(f"{col_name}_min"),
                spark_max(col(col_name)).alias(f"{col_name}_max"),
                stddev(col(col_name)).alias(f"{col_name}_stddev")
            ])
        
        return df.select(*stats_exprs)
    
    def get_categorical_summary(self, df: DataFrame, categorical_columns: List[str]) -> Dict[str, DataFrame]:
        """
        Get summary statistics for categorical columns.
        
        Args:
            df: Input DataFrame
            categorical_columns: List of categorical column names
            
        Returns:
            Dict: Dictionary mapping column names to their value counts
        """
        summaries = {}
        for col_name in categorical_columns:
            summaries[col_name] = df.groupBy(col_name) \
                .count() \
                .orderBy(col("count").desc())
        
        return summaries
    
    def calculate_retention_metrics(self, df: DataFrame, 
                                  policy_col: str = "policy",
                                  date_col: str = "inforce_yy",
                                  month_col: str = "inforce_mm") -> DataFrame:
        """
        Calculate basic retention metrics.
        
        Args:
            df: Input DataFrame
            policy_col: Policy identifier column
            date_col: Year column
            month_col: Month column
            
        Returns:
            DataFrame: Retention metrics by period
        """
        # Create a period identifier
        df_with_period = df.withColumn(
            "period", 
            col(date_col) * 100 + col(month_col)
        )
        
        # Get unique policies per period
        policies_per_period = df_with_period.select("period", policy_col) \
            .distinct() \
            .groupBy("period") \
            .agg(countDistinct(policy_col).alias("unique_policies"))
        
        # Calculate retention (policies that appear in consecutive periods)
        window_spec = Window.orderBy("period")
        policies_with_lag = policies_per_period.withColumn(
            "prev_period_policies", 
            col("unique_policies").lag(1).over(window_spec)
        )
        
        retention_metrics = policies_with_lag.withColumn(
            "retention_rate",
            when(col("prev_period_policies").isNull(), lit(1.0))
            .otherwise(col("unique_policies") / col("prev_period_policies"))
        ).withColumn(
            "period_year", 
            col("period") // 100
        ).withColumn(
            "period_month", 
            col("period") % 100
        )
        
        return retention_metrics.select(
            "period_year", "period_month", "unique_policies", 
            "retention_rate"
        ).orderBy("period_year", "period_month")
    
    def calculate_cohort_analysis(self, df: DataFrame,
                                policy_col: str = "policy",
                                cohort_date_col: str = "policy_expiry_date") -> DataFrame:
        """
        Perform cohort analysis on policy data.
        
        Args:
            df: Input DataFrame
            policy_col: Policy identifier column
            cohort_date_col: Date column for cohort definition
            
        Returns:
            DataFrame: Cohort analysis results
        """
        # Define cohorts by year-month of first appearance
        cohort_df = df.groupBy(policy_col) \
            .agg(
                spark_min(col(cohort_date_col)).alias("cohort_date"),
                count("*").alias("total_periods")
            )
        
        # Add cohort period identifier
        cohort_df = cohort_df.withColumn(
            "cohort_period",
            year(col("cohort_date")) * 100 + month(col("cohort_date"))
        )
        
        # Calculate cohort size
        cohort_sizes = cohort_df.groupBy("cohort_period") \
            .agg(countDistinct(policy_col).alias("cohort_size"))
        
        return cohort_sizes.orderBy("cohort_period")
    
    def calculate_premium_analysis(self, df: DataFrame,
                                 premium_col: str = "premium_paid",
                                 group_cols: Optional[List[str]] = None) -> DataFrame:
        """
        Calculate premium analysis metrics.
        
        Args:
            df: Input DataFrame
            premium_col: Premium column name
            group_cols: Optional list of columns to group by
            
        Returns:
            DataFrame: Premium analysis results
        """
        if group_cols is None:
            group_cols = []
        
        agg_exprs = [
            count("*").alias("record_count"),
            countDistinct("policy").alias("unique_policies"),
            avg(col(premium_col)).alias("avg_premium"),
            spark_min(col(premium_col)).alias("min_premium"),
            spark_max(col(premium_col)).alias("max_premium"),
            stddev(col(premium_col)).alias("premium_stddev"),
            spark_sum(col(premium_col)).alias("total_premium")
        ]
        
        if group_cols:
            return df.groupBy(*group_cols).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)
    
    def calculate_age_analysis(self, df: DataFrame,
                             age_col: str = "driver_age",
                             group_cols: Optional[List[str]] = None) -> DataFrame:
        """
        Calculate age-based analysis metrics.
        
        Args:
            df: Input DataFrame
            age_col: Age column name
            group_cols: Optional list of columns to group by
            
        Returns:
            DataFrame: Age analysis results
        """
        if group_cols is None:
            group_cols = []
        
        agg_exprs = [
            count("*").alias("record_count"),
            avg(col(age_col)).alias("avg_age"),
            spark_min(col(age_col)).alias("min_age"),
            spark_max(col(age_col)).alias("max_age"),
            stddev(col(age_col)).alias("age_stddev")
        ]
        
        if group_cols:
            return df.groupBy(*group_cols).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)
    
    def calculate_vehicle_analysis(self, df: DataFrame,
                                 vehicle_type_col: str = "vehicle_type",
                                 model_year_col: str = "model_year") -> Dict[str, DataFrame]:
        """
        Calculate vehicle-related analysis metrics.
        
        Args:
            df: Input DataFrame
            vehicle_type_col: Vehicle type column name
            model_year_col: Model year column name
            
        Returns:
            Dict: Dictionary containing various vehicle analysis results
        """
        results = {}
        
        # Vehicle type distribution
        results["vehicle_type_dist"] = df.groupBy(vehicle_type_col) \
            .agg(
                count("*").alias("count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium")
            ).orderBy(col("count").desc())
        
        # Model year analysis
        results["model_year_analysis"] = df.groupBy(model_year_col) \
            .agg(
                count("*").alias("count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium")
            ).orderBy(model_year_col)
        
        # Vehicle age analysis (current year - model year)
        current_year = 2024  # This could be made configurable
        vehicle_age_analysis = df.withColumn(
            "vehicle_age", 
            current_year - col(model_year_col)
        ).groupBy("vehicle_age") \
            .agg(
                count("*").alias("count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium")
            ).orderBy("vehicle_age")
        
        results["vehicle_age_analysis"] = vehicle_age_analysis
        
        return results
    
    def calculate_temporal_analysis(self, df: DataFrame,
                                  year_col: str = "inforce_yy",
                                  month_col: str = "inforce_mm") -> DataFrame:
        """
        Calculate temporal analysis metrics.
        
        Args:
            df: Input DataFrame
            year_col: Year column name
            month_col: Month column name
            
        Returns:
            DataFrame: Temporal analysis results
        """
        return df.groupBy(year_col, month_col) \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                countDistinct("vehicle_no").alias("unique_vehicles"),
                countDistinct("driver_no").alias("unique_drivers"),
                avg("premium_paid").alias("avg_premium"),
                spark_sum("premium_paid").alias("total_premium")
            ).orderBy(year_col, month_col)
    
    def export_to_csv(self, df: DataFrame, output_path: str, 
                     coalesce: bool = True, header: bool = True) -> None:
        """
        Export DataFrame to CSV file.
        
        Args:
            df: DataFrame to export
            output_path: Output file path
            coalesce: Whether to coalesce to single partition
            header: Whether to include header
        """
        if coalesce:
            df.coalesce(1).write.mode("overwrite").option("header", header).csv(output_path)
        else:
            df.write.mode("overwrite").option("header", header).csv(output_path)
        
        print(f"Data exported to: {output_path}")
    
    def get_data_quality_report(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate a data quality report.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dict: Data quality metrics
        """
        total_records = df.count()
        total_columns = len(df.columns)
        
        # Check for null values
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = {
                "null_count": null_count,
                "null_percentage": (null_count / total_records) * 100
            }
        
        # Check for duplicate records
        duplicate_count = total_records - df.distinct().count()
        
        return {
            "total_records": total_records,
            "total_columns": total_columns,
            "duplicate_records": duplicate_count,
            "duplicate_percentage": (duplicate_count / total_records) * 100,
            "null_analysis": null_counts
        }
