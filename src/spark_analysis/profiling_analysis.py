"""
PySpark Profiling Analysis Module

Provides basic data profiling and analysis capabilities using PySpark.
This module focuses on fundamental data exploration and profiling tasks.
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, variance, when, isnan, isnull, lit,
    desc, asc, round as spark_round, concat_ws, collect_list, collect_set
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from .base_analysis import BaseAnalysis


class ProfilingAnalysis:
    """
    PySpark-based data profiling and basic analysis.
    
    Provides comprehensive data profiling capabilities for insurance data
    using PySpark for efficient processing of large datasets.
    """
    
    def __init__(self, spark_session: SparkSession):
        """
        Initialize ProfilingAnalysis.
        
        Args:
            spark_session: Active SparkSession
        """
        self.spark = spark_session
        self.base_analysis = BaseAnalysis(spark_session)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = Path("config/policy_system_config.yaml")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def profile_data_overview(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate a comprehensive data overview profile.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dict: Comprehensive data profile
        """
        print("Generating data overview profile...")
        
        profile = {}
        
        # Basic statistics
        profile['basic_stats'] = self._get_basic_data_stats(df)
        
        # Column analysis
        profile['column_analysis'] = self._analyze_columns(df)
        
        # Data quality metrics
        profile['data_quality'] = self._assess_data_quality(df)
        
        # Temporal analysis
        profile['temporal_analysis'] = self._analyze_temporal_patterns(df)
        
        # Business metrics
        profile['business_metrics'] = self._calculate_business_metrics(df)
        
        return profile
    
    def _get_basic_data_stats(self, df: DataFrame) -> Dict[str, Any]:
        """Get basic data statistics."""
        print("  - Calculating basic data statistics...")
        
        total_records = df.count()
        total_columns = len(df.columns)
        
        # Get unique counts for key dimensions
        unique_policies = df.select("policy").distinct().count()
        unique_vehicles = df.select("vehicle_no").distinct().count()
        unique_drivers = df.select("driver_no").distinct().count()
        
        # Date range
        date_stats = df.select(
            spark_min("inforce_yy").alias("min_year"),
            spark_max("inforce_yy").alias("max_year"),
            spark_min("inforce_mm").alias("min_month"),
            spark_max("inforce_mm").alias("max_month")
        ).collect()[0]
        
        return {
            'total_records': total_records,
            'total_columns': total_columns,
            'unique_policies': unique_policies,
            'unique_vehicles': unique_vehicles,
            'unique_drivers': unique_drivers,
            'date_range': {
                'min_year': date_stats['min_year'],
                'max_year': date_stats['max_year'],
                'min_month': date_stats['min_month'],
                'max_month': date_stats['max_month']
            }
        }
    
    def _analyze_columns(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze individual columns."""
        print("  - Analyzing column characteristics...")
        
        column_analysis = {}
        
        # Numeric columns analysis
        numeric_columns = ['premium_paid', 'driver_age', 'client_tenure_days', 'model_year']
        for col_name in numeric_columns:
            if col_name in df.columns:
                stats = df.select(col_name).describe().collect()
                column_analysis[col_name] = {
                    row['summary']: row[col_name] for row in stats
                }
        
        # Categorical columns analysis
        categorical_columns = ['vehicle_type', 'driver_type', 'assignment_type']
        for col_name in categorical_columns:
            if col_name in df.columns:
                value_counts = df.groupBy(col_name).count().orderBy(desc("count")).limit(10)
                column_analysis[col_name] = {
                    'top_values': [row.asDict() for row in value_counts.collect()],
                    'unique_count': df.select(col_name).distinct().count()
                }
        
        return column_analysis
    
    def _assess_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics."""
        print("  - Assessing data quality...")
        
        total_records = df.count()
        quality_metrics = {}
        
        # Check for null values in key columns
        key_columns = ['policy', 'vehicle_no', 'driver_no', 'premium_paid']
        null_analysis = {}
        
        for col_name in key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_analysis[col_name] = {
                    'null_count': null_count,
                    'null_percentage': (null_count / total_records) * 100
                }
        
        # Check for duplicate records
        duplicate_count = total_records - df.distinct().count()
        
        # Check for data consistency
        consistency_checks = self._check_data_consistency(df)
        
        quality_metrics = {
            'null_analysis': null_analysis,
            'duplicate_records': duplicate_count,
            'duplicate_percentage': (duplicate_count / total_records) * 100,
            'consistency_checks': consistency_checks
        }
        
        return quality_metrics
    
    def _check_data_consistency(self, df: DataFrame) -> Dict[str, Any]:
        """Check data consistency rules."""
        consistency_checks = {}
        
        # Check for negative premiums
        negative_premiums = df.filter(col("premium_paid") < 0).count()
        consistency_checks['negative_premiums'] = negative_premiums
        
        # Check for invalid ages
        invalid_ages = df.filter((col("driver_age") < 16) | (col("driver_age") > 100)).count()
        consistency_checks['invalid_ages'] = invalid_ages
        
        # Check for future model years
        current_year = 2024
        future_model_years = df.filter(col("model_year") > current_year).count()
        consistency_checks['future_model_years'] = future_model_years
        
        # Check for negative tenure
        negative_tenure = df.filter(col("client_tenure_days") < 0).count()
        consistency_checks['negative_tenure'] = negative_tenure
        
        return consistency_checks
    
    def _analyze_temporal_patterns(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze temporal patterns in the data."""
        print("  - Analyzing temporal patterns...")
        
        # Monthly trends
        monthly_trends = df.groupBy("inforce_yy", "inforce_mm") \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium")
            ).orderBy("inforce_yy", "inforce_mm")
        
        # Yearly trends
        yearly_trends = df.groupBy("inforce_yy") \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                countDistinct("vehicle_no").alias("unique_vehicles"),
                countDistinct("driver_no").alias("unique_drivers"),
                avg("premium_paid").alias("avg_premium"),
                spark_sum("premium_paid").alias("total_premium")
            ).orderBy("inforce_yy")
        
        return {
            'monthly_trends': [row.asDict() for row in monthly_trends.collect()],
            'yearly_trends': [row.asDict() for row in yearly_trends.collect()]
        }
    
    def _calculate_business_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate key business metrics."""
        print("  - Calculating business metrics...")
        
        # Premium metrics
        premium_stats = df.select("premium_paid").describe().collect()
        premium_summary = {row['summary']: row['premium_paid'] for row in premium_stats}
        
        # Policy metrics
        policy_metrics = df.groupBy("policy").agg(
            count("*").alias("record_count"),
            countDistinct("vehicle_no").alias("vehicle_count"),
            countDistinct("driver_no").alias("driver_count"),
            avg("premium_paid").alias("avg_premium")
        )
        
        policy_stats = policy_metrics.select(
            "record_count", "vehicle_count", "driver_count", "avg_premium"
        ).describe().collect()
        
        # Vehicle metrics
        vehicle_metrics = df.groupBy("vehicle_no").agg(
            count("*").alias("record_count"),
            countDistinct("driver_no").alias("driver_count"),
            avg("premium_paid").alias("avg_premium")
        )
        
        vehicle_stats = vehicle_metrics.select(
            "record_count", "driver_count", "avg_premium"
        ).describe().collect()
        
        return {
            'premium_summary': premium_summary,
            'policy_metrics': {row['summary']: {
                'record_count': row['record_count'],
                'vehicle_count': row['vehicle_count'],
                'driver_count': row['driver_count'],
                'avg_premium': row['avg_premium']
            } for row in policy_stats},
            'vehicle_metrics': {row['summary']: {
                'record_count': row['record_count'],
                'driver_count': row['driver_count'],
                'avg_premium': row['avg_premium']
            } for row in vehicle_stats}
        }
    
    def generate_profiling_report(self, df: DataFrame, output_dir: Optional[Path] = None) -> None:
        """
        Generate a comprehensive profiling report.
        
        Args:
            df: Input DataFrame
            output_dir: Optional output directory
        """
        print("Generating comprehensive profiling report...")
        
        if output_dir is None:
            output_dir = Path(self.config['file_paths']['output'])
        
        output_dir.mkdir(exist_ok=True)
        
        # Generate profile
        profile = self.profile_data_overview(df)
        
        # Save detailed reports
        self._save_profiling_reports(df, profile, output_dir)
        
        # Print summary
        self._print_profiling_summary(profile)
        
        print(f"Profiling reports saved to {output_dir}")
    
    def _save_profiling_reports(self, df: DataFrame, profile: Dict[str, Any], output_dir: Path) -> None:
        """Save detailed profiling reports to files."""
        
        # Save monthly trends
        monthly_trends = df.groupBy("inforce_yy", "inforce_mm") \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                countDistinct("vehicle_no").alias("unique_vehicles"),
                countDistinct("driver_no").alias("unique_drivers"),
                avg("premium_paid").alias("avg_premium"),
                spark_sum("premium_paid").alias("total_premium")
            ).orderBy("inforce_yy", "inforce_mm")
        
        monthly_file = output_dir / "monthly_profiling_summary.csv"
        monthly_trends.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(str(monthly_file))
        
        # Save vehicle type distribution
        vehicle_dist = df.groupBy("vehicle_type") \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium")
            ).orderBy(desc("record_count"))
        
        vehicle_file = output_dir / "vehicle_type_profiling.csv"
        vehicle_dist.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(str(vehicle_file))
        
        # Save driver type distribution
        driver_dist = df.groupBy("driver_type") \
            .agg(
                count("*").alias("record_count"),
                countDistinct("policy").alias("unique_policies"),
                avg("premium_paid").alias("avg_premium"),
                avg("driver_age").alias("avg_age")
            ).orderBy(desc("record_count"))
        
        driver_file = output_dir / "driver_type_profiling.csv"
        driver_dist.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(str(driver_file))
        
        print(f"  - Monthly profiling summary: {monthly_file}")
        print(f"  - Vehicle type profiling: {vehicle_file}")
        print(f"  - Driver type profiling: {driver_file}")
    
    def _print_profiling_summary(self, profile: Dict[str, Any]) -> None:
        """Print a summary of the profiling results."""
        print("\n" + "="*50)
        print("DATA PROFILING SUMMARY")
        print("="*50)
        
        # Basic stats
        basic = profile['basic_stats']
        print(f"Total Records: {basic['total_records']:,}")
        print(f"Unique Policies: {basic['unique_policies']:,}")
        print(f"Unique Vehicles: {basic['unique_vehicles']:,}")
        print(f"Unique Drivers: {basic['unique_drivers']:,}")
        print(f"Date Range: {basic['date_range']['min_year']}-{basic['date_range']['min_month']:02d} to {basic['date_range']['max_year']}-{basic['date_range']['max_month']:02d}")
        
        # Data quality
        quality = profile['data_quality']
        print(f"\nData Quality:")
        print(f"  Duplicate Records: {quality['duplicate_records']:,} ({quality['duplicate_percentage']:.2f}%)")
        
        # Consistency checks
        consistency = quality['consistency_checks']
        print(f"  Negative Premiums: {consistency['negative_premiums']:,}")
        print(f"  Invalid Ages: {consistency['invalid_ages']:,}")
        print(f"  Future Model Years: {consistency['future_model_years']:,}")
        print(f"  Negative Tenure: {consistency['negative_tenure']:,}")
        
        # Business metrics
        business = profile['business_metrics']
        premium = business['premium_summary']
        print(f"\nPremium Statistics:")
        mean_premium = float(premium.get('mean', 0)) if premium.get('mean', 'null') != 'null' else 0
        median_premium = float(premium.get('50%', 0)) if premium.get('50%', 'null') != 'null' else 0
        min_premium = float(premium.get('min', 0)) if premium.get('min', 'null') != 'null' else 0
        max_premium = float(premium.get('max', 0)) if premium.get('max', 'null') != 'null' else 0
        
        print(f"  Mean: ${mean_premium:.2f}")
        print(f"  Median: ${median_premium:.2f}")
        print(f"  Range: ${min_premium:.2f} - ${max_premium:.2f}")
        
        print("="*50)
