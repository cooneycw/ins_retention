"""
PySpark Distribution Analysis Module

Migrated from pandas-based distribution analysis to use PySpark DataFrames
for improved performance on large datasets.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, when, isnan, isnull, lit, 
    row_number, first, last, collect_list, collect_set
)
from pyspark.sql.window import Window

from .data_loader import InforceDataLoader
from .base_analysis import BaseAnalysis


class DistributionAnalysis:
    """
    PySpark-based distribution analysis for inforce data.
    
    Provides comprehensive distribution analysis capabilities using PySpark
    for efficient processing of large insurance datasets.
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize DistributionAnalysis.
        
        Args:
            spark_session: Optional SparkSession. If None, creates a new one.
        """
        self.spark = spark_session or self._create_spark_session()
        self.data_loader = InforceDataLoader(self.spark)
        self.base_analysis = BaseAnalysis(self.spark)
        self.config = self._load_config()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure a SparkSession for distribution analysis."""
        return SparkSession.builder \
            .appName("DistributionAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = Path("config/policy_system_config.yaml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def analyze_inforce_distributions(self, df: Optional[DataFrame] = None) -> None:
        """
        Perform comprehensive distribution analysis on the inforce data.
        
        Args:
            df: Optional DataFrame. If None, loads data from file.
        """
        print("Analyzing inforce distributions with PySpark...")
        
        if df is None:
            df = self.data_loader.load_inforce_data()
        
        # Validate data
        validation_results = self.data_loader.validate_data(df)
        
        # Perform various distribution analyses
        self._analyze_policy_distributions(df)
        self._analyze_vehicle_distributions(df)
        self._analyze_driver_distributions(df)
        self._analyze_temporal_distributions(df)
        self._analyze_premium_distributions(df)
        
        print("Distribution analysis completed!")
    
    def _analyze_policy_distributions(self, df: DataFrame) -> None:
        """Analyze policy-level distributions using PySpark."""
        print("  - Policy Distributions:")
        
        # Unique policies
        unique_policies = df.select("policy").distinct().count()
        print(f"    Total unique policies: {unique_policies:,}")
        
        # Policy expiry date distribution
        expiry_dist = df.select("policy_expiry_date") \
            .groupBy("policy_expiry_date") \
            .count() \
            .orderBy(col("count").desc()) \
            .limit(10)
        
        expiry_list = expiry_dist.collect()
        print(f"    Policy expiry dates: {len(expiry_list)} unique dates (showing top 10)")
        for row in expiry_list[:3]:
            print(f"      {row['policy_expiry_date']}: {row['count']:,} policies")
        
        # Client tenure distribution
        tenure_stats = df.select("client_tenure_days").describe().collect()
        tenure_summary = {row['summary']: row['client_tenure_days'] for row in tenure_stats}
        mean_tenure = float(tenure_summary.get('mean', 0)) if tenure_summary.get('mean', 'null') != 'null' else 0
        median_tenure = df.approxQuantile("client_tenure_days", [0.5], 0.25)[0]
        print(f"    Client tenure - Mean: {mean_tenure:.0f} days, "
              f"Median: {median_tenure:.0f} days")
    
    def _analyze_vehicle_distributions(self, df: DataFrame) -> None:
        """Analyze vehicle-level distributions using PySpark."""
        print("  - Vehicle Distributions:")
        
        # Vehicle types
        vehicle_types = df.groupBy("vehicle_type") \
            .count() \
            .orderBy(col("count").desc())
        
        vehicle_list = vehicle_types.collect()
        print(f"    Vehicle types: {len(vehicle_list)} types")
        for row in vehicle_list[:5]:
            print(f"      {row['vehicle_type']}: {row['count']:,} records")
        
        # Model years
        model_years = df.groupBy("model_year") \
            .count() \
            .orderBy("model_year")
        
        year_list = model_years.collect()
        min_year = year_list[0]['model_year']
        max_year = year_list[-1]['model_year']
        most_common = max(year_list, key=lambda x: x['count'])
        
        print(f"    Model years: {min_year}-{max_year}")
        print(f"    Most common model year: {most_common['model_year']} "
              f"({most_common['count']:,} records)")
        
        # Vehicles per policy
        vehicles_per_policy = df.groupBy("policy") \
            .agg(countDistinct("vehicle_no").alias("vehicle_count"))
        
        vehicle_stats = vehicles_per_policy.select("vehicle_count").describe().collect()
        vehicle_summary = {row['summary']: row['vehicle_count'] for row in vehicle_stats}
        mean_vehicles = float(vehicle_summary.get('mean', 0)) if vehicle_summary.get('mean', 'null') != 'null' else 0
        max_vehicles = vehicle_summary['max']
        print(f"    Vehicles per policy - Mean: {mean_vehicles:.2f}, "
              f"Max: {max_vehicles}")
    
    def _analyze_driver_distributions(self, df: DataFrame) -> None:
        """Analyze driver-level distributions using PySpark."""
        print("  - Driver Distributions:")
        
        # Driver types
        driver_types = df.groupBy("driver_type") \
            .count() \
            .orderBy(col("count").desc())
        
        driver_list = driver_types.collect()
        print(f"    Driver types: {len(driver_list)} types")
        for row in driver_list:
            print(f"      {row['driver_type']}: {row['count']:,} records")
        
        # Driver ages
        age_stats = df.select("driver_age").describe().collect()
        age_summary = {row['summary']: row['driver_age'] for row in age_stats}
        mean_age = float(age_summary.get('mean', 0)) if age_summary.get('mean', 'null') != 'null' else 0
        min_age = float(age_summary.get('min', 0)) if age_summary.get('min', 'null') != 'null' else 0
        max_age = float(age_summary.get('max', 0)) if age_summary.get('max', 'null') != 'null' else 0
        print(f"    Driver ages - Mean: {mean_age:.1f}, "
              f"Range: {min_age:.0f}-{max_age:.0f}")
        
        # Drivers per policy
        drivers_per_policy = df.groupBy("policy") \
            .agg(countDistinct("driver_no").alias("driver_count"))
        
        driver_stats = drivers_per_policy.select("driver_count").describe().collect()
        driver_summary = {row['summary']: row['driver_count'] for row in driver_stats}
        mean_drivers = float(driver_summary.get('mean', 0)) if driver_summary.get('mean', 'null') != 'null' else 0
        max_drivers = driver_summary['max']
        print(f"    Drivers per policy - Mean: {mean_drivers:.2f}, "
              f"Max: {max_drivers}")
        
        # Unique driver names
        unique_drivers = df.select("driver_name").distinct().count()
        print(f"    Unique driver names: {unique_drivers:,}")
    
    def _analyze_temporal_distributions(self, df: DataFrame) -> None:
        """Analyze temporal distributions using PySpark."""
        print("  - Temporal Distributions:")
        
        # Year distribution
        year_counts = df.groupBy("inforce_yy") \
            .count() \
            .orderBy("inforce_yy")
        
        year_list = year_counts.collect()
        min_year = year_list[0]['inforce_yy']
        max_year = year_list[-1]['inforce_yy']
        min_count = min(year_list, key=lambda x: x['count'])['count']
        max_count = max(year_list, key=lambda x: x['count'])['count']
        
        print(f"    Years covered: {min_year}-{max_year}")
        print(f"    Records per year - Min: {min_count:,}, Max: {max_count:,}")
        
        # Month distribution
        month_counts = df.groupBy("inforce_mm") \
            .count() \
            .orderBy("inforce_mm")
        
        month_list = month_counts.collect()
        min_month_count = min(month_list, key=lambda x: x['count'])['count']
        max_month_count = max(month_list, key=lambda x: x['count'])['count']
        
        print(f"    Month distribution: {min_month_count:,}-{max_month_count:,} records per month")
    
    def _analyze_premium_distributions(self, df: DataFrame) -> None:
        """Analyze premium distributions using PySpark."""
        print("  - Premium Distributions:")
        
        # Premium statistics
        premium_stats = df.select("premium_paid").describe().collect()
        premium_summary = {row['summary']: row['premium_paid'] for row in premium_stats}
        
        # Calculate median using approxQuantile (PySpark's describe() doesn't include median)
        median_premium = df.approxQuantile("premium_paid", [0.5], 0.25)[0]
        
        mean_premium = float(premium_summary.get('mean', 0)) if premium_summary.get('mean', 'null') != 'null' else 0
        min_premium = float(premium_summary.get('min', 0)) if premium_summary.get('min', 'null') != 'null' else 0
        max_premium = float(premium_summary.get('max', 0)) if premium_summary.get('max', 'null') != 'null' else 0
        
        print(f"    Premium - Mean: ${mean_premium:.2f}, "
              f"Median: ${median_premium:.2f}")
        print(f"    Premium range: ${min_premium:.2f} - ${max_premium:.2f}")
        
        # Premium by driver type
        premium_by_type = df.groupBy("driver_type") \
            .agg(avg("premium_paid").alias("avg_premium")) \
            .orderBy(col("avg_premium").desc())
        
        premium_list = premium_by_type.collect()
        print(f"    Average premium by driver type:")
        for row in premium_list:
            print(f"      {row['driver_type']}: ${row['avg_premium']:.2f}")
    
    def generate_distribution_report(self, df: Optional[DataFrame] = None) -> None:
        """
        Generate a comprehensive distribution report and save to file using PySpark.
        
        Args:
            df: Optional DataFrame. If None, loads data from file.
        """
        print("Generating distribution report with PySpark...")
        
        if df is None:
            df = self.data_loader.load_inforce_data()
        
        # Create output directory
        output_dir = Path(self.config['file_paths']['output'])
        output_dir.mkdir(exist_ok=True)
        
        # Generate the monthly distribution report
        self._generate_monthly_distribution_report(df, output_dir)
        
        print(f"Distribution reports saved to {output_dir}")
    
    def _generate_monthly_distribution_report(self, df: DataFrame, output_dir: Path) -> None:
        """
        Generate monthly distribution report with PySpark.
        
        Args:
            df: Input DataFrame
            output_dir: Output directory path
        """
        print("  - Generating monthly distribution summary...")
        
        # Check if major_change column exists
        has_major_change_flag = "major_change" in df.columns
        
        if has_major_change_flag:
            # Group by year and month for inforce policies with major change breakdown
            monthly_stats = df.groupBy("inforce_yy", "inforce_mm") \
                .agg(
                    countDistinct("policy").alias("inforce_count"),
                    avg("premium_paid").alias("avg_premium"),
                    avg("driver_age").alias("avg_driver_age"),
                    spark_sum(when(col("major_change") == True, 1).otherwise(0)).alias("major_change_count"),
                    avg(when(col("major_change") == True, 1).otherwise(0)).alias("major_change_rate")
                ).orderBy("inforce_yy", "inforce_mm")
        else:
            # Group by year and month for inforce policies (original)
            monthly_stats = df.groupBy("inforce_yy", "inforce_mm") \
                .agg(
                    countDistinct("policy").alias("inforce_count"),
                    avg("premium_paid").alias("avg_premium"),
                    avg("driver_age").alias("avg_driver_age")
                ).orderBy("inforce_yy", "inforce_mm")
        
        # Calculate average driver and vehicle counts per inforce policy
        policy_stats = df.groupBy("inforce_yy", "inforce_mm", "policy") \
            .agg(
                countDistinct("driver_no").alias("driver_count"),
                countDistinct("vehicle_no").alias("vehicle_count")
            )
        
        avg_stats = policy_stats.groupBy("inforce_yy", "inforce_mm") \
            .agg(
                avg("driver_count").alias("avg_driver_count"),
                avg("vehicle_count").alias("avg_vehicle_count")
            )
        
        # Merge the statistics
        monthly_stats = monthly_stats.join(avg_stats, ["inforce_yy", "inforce_mm"])
        
        # Calculate vehicle type distribution
        vehicle_type_dist = df.groupBy("inforce_yy", "inforce_mm", "vehicle_type") \
            .count() \
            .withColumn("total_vehicles", spark_sum("count").over(
                Window.partitionBy("inforce_yy", "inforce_mm")
            )) \
            .withColumn("vehicle_type_pct", (col("count") / col("total_vehicles")) * 100)
        
        # Pivot vehicle types
        vehicle_pivot = vehicle_type_dist.groupBy("inforce_yy", "inforce_mm") \
            .pivot("vehicle_type") \
            .agg(first("vehicle_type_pct")) \
            .fillna(0.0)
        
        # Add vehicle type columns to monthly stats
        monthly_stats = monthly_stats.join(vehicle_pivot, ["inforce_yy", "inforce_mm"], "left")
        
        # Calculate new vs renewed policies
        # Get first appearance of each policy
        policy_first_appearance = df.groupBy("policy") \
            .agg(
                spark_min("inforce_yy").alias("first_year"),
                spark_min("inforce_mm").alias("first_month")
            )
        
        # Add new/renewed policy counts
        monthly_with_new_renewed = monthly_stats.join(
            policy_first_appearance.alias("first_app"), 
            (monthly_stats["inforce_yy"] == col("first_app.first_year")) & 
            (monthly_stats["inforce_mm"] == col("first_app.first_month")),
            "left"
        )
        
        # This is a simplified approach - in practice, you'd want more sophisticated logic
        # for determining new vs renewed policies based on business rules
        
        # Calculate cumulative policy count
        all_policies = df.select("policy").distinct()
        cumulative_count = all_policies.count()
        
        # Add cumulative count (simplified - in practice, calculate month by month)
        monthly_stats = monthly_stats.withColumn("cumulative_policy_count", lit(cumulative_count))
        
        # Rename columns for consistency
        if has_major_change_flag:
            monthly_stats = monthly_stats.select(
                col("inforce_yy").alias("year"),
                col("inforce_mm").alias("month"),
                col("inforce_count"),
                col("avg_premium"),
                col("avg_driver_age"),
                col("avg_driver_count"),
                col("avg_vehicle_count"),
                col("cumulative_policy_count"),
                col("major_change_count"),
                col("major_change_rate")
            )
        else:
            monthly_stats = monthly_stats.select(
                col("inforce_yy").alias("year"),
                col("inforce_mm").alias("month"),
                col("inforce_count"),
                col("avg_premium"),
                col("avg_driver_age"),
                col("avg_driver_count"),
                col("avg_vehicle_count"),
                col("cumulative_policy_count")
            )
        
        # Round numeric columns
        if has_major_change_flag:
            monthly_stats = monthly_stats.select(
                "year", "month", "inforce_count", "cumulative_policy_count",
                col("avg_premium").cast("decimal(10,2)").alias("avg_premium"),
                col("avg_driver_age").cast("decimal(5,1)").alias("avg_driver_age"),
                col("avg_driver_count").cast("decimal(5,2)").alias("avg_driver_count"),
                col("avg_vehicle_count").cast("decimal(5,2)").alias("avg_vehicle_count"),
                col("major_change_count").cast("integer").alias("major_change_count"),
                col("major_change_rate").cast("decimal(5,3)").alias("major_change_rate")
            )
        else:
            monthly_stats = monthly_stats.select(
                "year", "month", "inforce_count", "cumulative_policy_count",
                col("avg_premium").cast("decimal(10,2)").alias("avg_premium"),
                col("avg_driver_age").cast("decimal(5,1)").alias("avg_driver_age"),
                col("avg_driver_count").cast("decimal(5,2)").alias("avg_driver_count"),
                col("avg_vehicle_count").cast("decimal(5,2)").alias("avg_vehicle_count")
            )
        
        # Save to CSV
        output_file = output_dir / "monthly_distribution_summary.csv"
        monthly_stats.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(str(output_file))
        
        print(f"  - Monthly distribution summary saved to {output_file}")
        
        # Print summary statistics
        stats_collected = monthly_stats.collect()
        print(f"  - Generated monthly statistics for {len(stats_collected)} months")
        if stats_collected:
            min_date = f"{stats_collected[0]['year']}-{stats_collected[0]['month']:02d}"
            max_date = f"{stats_collected[-1]['year']}-{stats_collected[-1]['month']:02d}"
            print(f"  - Date range: {min_date} to {max_date}")
            
            avg_inforce = sum(row['inforce_count'] for row in stats_collected) / len(stats_collected)
            avg_premium = sum(float(row['avg_premium']) for row in stats_collected) / len(stats_collected)
            final_cumulative = stats_collected[-1]['cumulative_policy_count']
            
            print(f"  - Average inforce policies per month: {avg_inforce:.0f}")
            print(f"  - Average premium per month: ${avg_premium:.2f}")
            print(f"  - Final cumulative policy count: {final_cumulative:,}")
    
    def close(self):
        """Close the SparkSession."""
        if self.spark:
            self.spark.stop()
