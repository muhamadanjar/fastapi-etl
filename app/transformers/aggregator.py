# ==============================================
# app/transformers/aggregator.py
# ==============================================
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import statistics

from .base_transformer import BaseTransformer, TransformationResult, TransformationStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

class AggregationType(Enum):
    """Types of aggregation operations"""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    MODE = "mode"
    STDDEV = "stddev"
    VARIANCE = "variance"
    PERCENTILE = "percentile"
    FIRST = "first"
    LAST = "last"
    CONCAT = "concat"
    LIST = "list"
    UNIQUE_COUNT = "unique_count"
    GROUP_CONCAT = "group_concat"

class TimeGranularity(Enum):
    """Time-based aggregation granularities"""
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

@dataclass
class AggregationRule:
    """Configuration for aggregation rule"""
    name: str
    aggregation_type: AggregationType
    source_field: str
    target_field: Optional[str] = None
    parameters: Dict[str, Any] = None
    condition: Optional[str] = None
    weight: float = 1.0

@dataclass
class GroupingRule:
    """Configuration for grouping/dimension"""
    field_name: str
    alias: Optional[str] = None
    transformation: Optional[str] = None
    time_granularity: Optional[TimeGranularity] = None
    custom_buckets: Optional[Dict[str, Any]] = None

class Aggregator(BaseTransformer):
    """
    Data aggregation transformer that handles:
    - Group by aggregations (sum, count, avg, min, max, etc.)
    - Time-based aggregations (daily, monthly, yearly summaries)
    - Multi-dimensional aggregations
    - Statistical aggregations (percentiles, standard deviation)
    - Custom aggregation functions
    - Hierarchical aggregations
    - Rolling/window aggregations
    - Pivot table generation
    - Cross-tabulation
    - Data summarization
    """
    
    def __init__(self, db_session, job_execution_id: Optional[str] = None, **kwargs):
        """
        Initialize aggregator
        
        Args:
            db_session: Database session
            job_execution_id: Job execution ID for tracking
            **kwargs: Additional configuration
        """
        super().__init__(db_session, job_execution_id, **kwargs)
        
        # Aggregation configuration
        self.grouping_rules = self._parse_grouping_rules(kwargs.get('grouping_rules', []))
        self.aggregation_rules = self._parse_aggregation_rules(kwargs.get('aggregation_rules', []))
        self.output_format = kwargs.get('output_format', 'records')  # 'records', 'pivot', 'summary'
        
        # Processing options
        self.handle_nulls = kwargs.get('handle_nulls', 'ignore')  # 'ignore', 'zero', 'error'
        self.sort_results = kwargs.get('sort_results', True)
        self.sort_by = kwargs.get('sort_by', [])
        self.limit_results = kwargs.get('limit_results', None)
        
        # Time-based aggregation settings
        self.time_field = kwargs.get('time_field', None)
        self.time_format = kwargs.get('time_format', '%Y-%m-%d')
        self.fill_missing_periods = kwargs.get('fill_missing_periods', False)
        
        # Rolling window settings
        self.enable_rolling = kwargs.get('enable_rolling', False)
        self.rolling_window = kwargs.get('rolling_window', 7)
        self.rolling_aggregations = kwargs.get('rolling_aggregations', [])
        
        # Hierarchical aggregation
        self.enable_hierarchical = kwargs.get('enable_hierarchical', False)
        self.hierarchy_levels = kwargs.get('hierarchy_levels', [])
        
        # Data storage for aggregation
        self.data_buffer = []
        self.aggregated_results = {}
        
        # Custom aggregation functions
        self.custom_aggregators = {
            'weighted_avg': self._weighted_average,
            'harmonic_mean': self._harmonic_mean,
            'geometric_mean': self._geometric_mean,
            'quartiles': self._calculate_quartiles,
            'outlier_count': self._count_outliers,
            'entropy': self._calculate_entropy,
            'correlation': self._calculate_correlation,
        }
        
        # Statistical functions
        self.statistical_functions = {
            'mean': np.mean,
            'std': np.std,
            'var': np.var,
            'skew': self._calculate_skewness,
            'kurtosis': self._calculate_kurtosis,
            'percentile_25': lambda x: np.percentile(x, 25),
            'percentile_75': lambda x: np.percentile(x, 75),
            'iqr': lambda x: np.percentile(x, 75) - np.percentile(x, 25),
        }
    
    def _parse_grouping_rules(self, rules_config: List[Dict]) -> List[GroupingRule]:
        """Parse grouping rules from configuration"""
        rules = []
        
        for config in rules_config:
            rule = GroupingRule(
                field_name=config.get('field'),
                alias=config.get('alias'),
                transformation=config.get('transformation'),
                time_granularity=TimeGranularity(config['time_granularity']) if config.get('time_granularity') else None,
                custom_buckets=config.get('custom_buckets')
            )
            rules.append(rule)
        
        return rules
    
    def _parse_aggregation_rules(self, rules_config: List[Dict]) -> List[AggregationRule]:
        """Parse aggregation rules from configuration"""
        rules = []
        
        for config in rules_config:
            rule = AggregationRule(
                name=config.get('name'),
                aggregation_type=AggregationType(config.get('type', 'sum')),
                source_field=config.get('source_field'),
                target_field=config.get('target_field'),
                parameters=config.get('parameters', {}),
                condition=config.get('condition'),
                weight=config.get('weight', 1.0)
            )
            rules.append(rule)
        
        return rules
    
    async def validate_config(self) -> Tuple[bool, List[str]]:
        """Validate aggregator configuration"""
        errors = []
        
        # Validate grouping rules
        if not self.grouping_rules:
            errors.append("No grouping rules defined")
        
        # Validate aggregation rules
        if not self.aggregation_rules:
            errors.append("No aggregation rules defined")
        
        # Validate time settings if time-based aggregation is used
        time_based_grouping = any(rule.time_granularity for rule in self.grouping_rules)
        if time_based_grouping and not self.time_field:
            errors.append("Time field must be specified for time-based aggregation")
        
        # Validate rolling window settings
        if self.enable_rolling:
            if self.rolling_window <= 0:
                errors.append("Rolling window must be positive")
            if not self.rolling_aggregations:
                errors.append("Rolling aggregations must be specified when rolling is enabled")
        
        # Validate output format
        valid_formats = ['records', 'pivot', 'summary']
        if self.output_format not in valid_formats:
            errors.append(f"Invalid output format: {self.output_format}")
        
        return len(errors) == 0, errors
    
    async def transform_record(self, record: Dict[str, Any]) -> TransformationResult:
        """
        Add record to aggregation buffer
        
        Args:
            record: Input record to add to aggregation
            
        Returns:
            TransformationResult indicating buffer addition
        """
        try:
            # Add record to buffer
            self.data_buffer.append(record.copy())
            
            # Return success but no data yet (aggregation happens in finalize)
            return TransformationResult(
                status=TransformationStatus.SUCCESS,
                data=None,  # No individual record output
                metadata={
                    'added_to_buffer': True,
                    'buffer_size': len(self.data_buffer)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error adding record to aggregation buffer: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Failed to add record to aggregation buffer: {str(e)}"]
            )
    
    async def finalize_aggregation(self) -> TransformationResult:
        """
        Perform final aggregation on buffered data
        
        Returns:
            TransformationResult with aggregated data
        """
        try:
            if not self.data_buffer:
                return TransformationResult(
                    status=TransformationStatus.SUCCESS,
                    data=[],
                    metadata={'message': 'No data to aggregate'}
                )
            
            self.logger.info(f"Starting aggregation of {len(self.data_buffer)} records")
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(self.data_buffer)
            
            # Apply preprocessing
            df = await self._preprocess_data(df)
            
            # Perform main aggregation
            aggregated_data = await self._perform_aggregation(df)
            
            # Apply rolling aggregations if enabled
            if self.enable_rolling:
                aggregated_data = await self._apply_rolling_aggregations(aggregated_data)
            
            # Apply hierarchical aggregations if enabled
            if self.enable_hierarchical:
                aggregated_data = await self._apply_hierarchical_aggregations(aggregated_data)
            
            # Format output
            formatted_results = await self._format_output(aggregated_data)
            
            # Generate statistics
            metadata = await self._generate_aggregation_metadata(df, aggregated_data)
            
            self.logger.info(f"Aggregation completed: {len(formatted_results)} result groups")
            
            return TransformationResult(
                status=TransformationStatus.SUCCESS,
                data=formatted_results,
                metadata=metadata
            )
            
        except Exception as e:
            self.logger.error(f"Error performing aggregation: {str(e)}")
            return TransformationResult(
                status=TransformationStatus.FAILED,
                errors=[f"Aggregation failed: {str(e)}"]
            )
    
    async def _preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess data before aggregation"""
        # Handle missing values
        if self.handle_nulls == 'zero':
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(0)
        elif self.handle_nulls == 'error':
            if df.isnull().any().any():
                raise ValueError("Null values found in data")
        
        # Convert time field if specified
        if self.time_field and self.time_field in df.columns:
            df[self.time_field] = pd.to_datetime(df[self.time_field], format=self.time_format, errors='coerce')
        
        # Apply transformations to grouping fields
        for grouping_rule in self.grouping_rules:
            field_name = grouping_rule.field_name
            
            if field_name not in df.columns:
                continue
            
            # Apply time granularity transformation
            if grouping_rule.time_granularity and field_name == self.time_field:
                df[f"{field_name}_{grouping_rule.time_granularity.value}"] = self._apply_time_granularity(
                    df[field_name], grouping_rule.time_granularity
                )
            
            # Apply custom bucketing
            if grouping_rule.custom_buckets:
                df[f"{field_name}_bucket"] = self._apply_custom_buckets(
                    df[field_name], grouping_rule.custom_buckets
                )
            
            # Apply other transformations
            if grouping_rule.transformation:
                df[f"{field_name}_transformed"] = self._apply_transformation(
                    df[field_name], grouping_rule.transformation
                )
        
        return df
    
    async def _perform_aggregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform main aggregation"""
        # Determine grouping columns
        group_by_columns = []
        for grouping_rule in self.grouping_rules:
            if grouping_rule.alias:
                group_by_columns.append(grouping_rule.alias)
            elif grouping_rule.time_granularity:
                group_by_columns.append(f"{grouping_rule.field_name}_{grouping_rule.time_granularity.value}")
            elif grouping_rule.custom_buckets:
                group_by_columns.append(f"{grouping_rule.field_name}_bucket")
            elif grouping_rule.transformation:
                group_by_columns.append(f"{grouping_rule.field_name}_transformed")
            else:
                group_by_columns.append(grouping_rule.field_name)
        
        # Filter columns that actually exist
        existing_group_columns = [col for col in group_by_columns if col in df.columns]
        
        if not existing_group_columns:
            # No grouping - aggregate entire dataset
            return await self._aggregate_entire_dataset(df)
        
        # Group by specified columns
        grouped = df.groupby(existing_group_columns, as_index=False)
        
        # Apply aggregation rules
        aggregation_dict = {}
        for rule in self.aggregation_rules:
            if rule.source_field not in df.columns:
                continue
            
            # Apply condition filter if specified
            if rule.condition:
                filtered_df = df.query(rule.condition)
                if filtered_df.empty:
                    continue
                filtered_grouped = filtered_df.groupby(existing_group_columns, as_index=False)
                agg_result = self._apply_aggregation_rule(filtered_grouped, rule)
            else:
                agg_result = self._apply_aggregation_rule(grouped, rule)
            
            target_field = rule.target_field or f"{rule.source_field}_{rule.aggregation_type.value}"
            aggregation_dict[target_field] = agg_result
        
        # Combine aggregation results
        if aggregation_dict:
            # Start with first aggregation
            first_key = list(aggregation_dict.keys())[0]
            result_df = aggregation_dict[first_key]
            
            # Merge other aggregations
            for key, agg_df in list(aggregation_dict.items())[1:]:
                result_df = pd.merge(result_df, agg_df, on=existing_group_columns, how='outer')
        else:
            # No valid aggregations
            result_df = df[existing_group_columns].drop_duplicates()
        
        return result_df
    
    async def _aggregate_entire_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate entire dataset without grouping"""
        result_data = {}
        
        for rule in self.aggregation_rules:
            if rule.source_field not in df.columns:
                continue
            
            data = df[rule.source_field]
            
            # Apply condition filter if specified
            if rule.condition:
                filtered_df = df.query(rule.condition)
                if filtered_df.empty:
                    continue
                data = filtered_df[rule.source_field]
            
            # Calculate aggregation
            target_field = rule.target_field or f"{rule.source_field}_{rule.aggregation_type.value}"
            result_data[target_field] = self._calculate_aggregation(data, rule)
        
        # Return single-row DataFrame
        return pd.DataFrame([result_data])
    
    def _apply_aggregation_rule(self, grouped_data, rule: AggregationRule):
        """Apply single aggregation rule to grouped data"""
        if rule.aggregation_type == AggregationType.SUM:
            return grouped_data[rule.source_field].sum().reset_index()
        elif rule.aggregation_type == AggregationType.COUNT:
            return grouped_data[rule.source_field].count().reset_index()
        elif rule.aggregation_type == AggregationType.AVG:
            return grouped_data[rule.source_field].mean().reset_index()
        elif rule.aggregation_type == AggregationType.MIN:
            return grouped_data[rule.source_field].min().reset_index()
        elif rule.aggregation_type == AggregationType.MAX:
            return grouped_data[rule.source_field].max().reset_index()
        elif rule.aggregation_type == AggregationType.MEDIAN:
            return grouped_data[rule.source_field].median().reset_index()
        elif rule.aggregation_type == AggregationType.STDDEV:
            return grouped_data[rule.source_field].std().reset_index()
        elif rule.aggregation_type == AggregationType.VARIANCE:
            return grouped_data[rule.source_field].var().reset_index()
        elif rule.aggregation_type == AggregationType.FIRST:
            return grouped_data[rule.source_field].first().reset_index()
        elif rule.aggregation_type == AggregationType.LAST:
            return grouped_data[rule.source_field].last().reset_index()
        elif rule.aggregation_type == AggregationType.UNIQUE_COUNT:
            return grouped_data[rule.source_field].nunique().reset_index()
        elif rule.aggregation_type == AggregationType.LIST:
            return grouped_data[rule.source_field].apply(list).reset_index()
        elif rule.aggregation_type == AggregationType.CONCAT:
            separator = rule.parameters.get('separator', ',')
            return grouped_data[rule.source_field].apply(lambda x: separator.join(map(str, x))).reset_index()
        else:
            # Default to sum
            return grouped_data[rule.source_field].sum().reset_index()
    
    def _calculate_aggregation(self, data: pd.Series, rule: AggregationRule) -> Any:
        """Calculate aggregation for series data"""
        # Remove nulls if configured
        if self.handle_nulls == 'ignore':
            data = data.dropna()
        
        if data.empty:
            return None
        
        if rule.aggregation_type == AggregationType.SUM:
            return data.sum()
        elif rule.aggregation_type == AggregationType.COUNT:
            return len(data)
        elif rule.aggregation_type == AggregationType.AVG:
            return data.mean()
        elif rule.aggregation_type == AggregationType.MIN:
            return data.min()
        elif rule.aggregation_type == AggregationType.MAX:
            return data.max()
        elif rule.aggregation_type == AggregationType.MEDIAN:
            return data.median()
        elif rule.aggregation_type == AggregationType.MODE:
            return data.mode().iloc[0] if not data.mode().empty else None
        elif rule.aggregation_type == AggregationType.STDDEV:
            return data.std()
        elif rule.aggregation_type == AggregationType.VARIANCE:
            return data.var()
        elif rule.aggregation_type == AggregationType.PERCENTILE:
            percentile = rule.parameters.get('percentile', 50)
            return data.quantile(percentile / 100)
        elif rule.aggregation_type == AggregationType.FIRST:
            return data.iloc[0]
        elif rule.aggregation_type == AggregationType.LAST:
            return data.iloc[-1]
        elif rule.aggregation_type == AggregationType.UNIQUE_COUNT:
            return data.nunique()
        elif rule.aggregation_type == AggregationType.LIST:
            return data.tolist()
        elif rule.aggregation_type == AggregationType.CONCAT:
            separator = rule.parameters.get('separator', ',')
            return separator.join(map(str, data))
        else:
            return data.sum()
    
    def _apply_time_granularity(self, time_series: pd.Series, granularity: TimeGranularity) -> pd.Series:
        """Apply time granularity transformation"""
        if granularity == TimeGranularity.MINUTE:
            return time_series.dt.floor('T')
        elif granularity == TimeGranularity.HOUR:
            return time_series.dt.floor('H')
        elif granularity == TimeGranularity.DAY:
            return time_series.dt.date
        elif granularity == TimeGranularity.WEEK:
            return time_series.dt.to_period('W').dt.start_time
        elif granularity == TimeGranularity.MONTH:
            return time_series.dt.to_period('M').dt.start_time
        elif granularity == TimeGranularity.QUARTER:
            return time_series.dt.to_period('Q').dt.start_time
        elif granularity == TimeGranularity.YEAR:
            return time_series.dt.year
        else:
            return time_series
    
    def _apply_custom_buckets(self, data: pd.Series, bucket_config: Dict[str, Any]) -> pd.Series:
        """Apply custom bucketing to data"""
        bucket_type = bucket_config.get('type', 'range')
        
        if bucket_type == 'range':
            ranges = bucket_config.get('ranges', {})
            labels = bucket_config.get('labels', list(ranges.keys()))
            
            # Create bins
            bins = [-float('inf')] + [r[1] for r in ranges.values()] + [float('inf')]
            return pd.cut(data, bins=bins, labels=labels, include_lowest=True)
        
        elif bucket_type == 'quantile':
            n_buckets = bucket_config.get('n_buckets', 4)
            return pd.qcut(data, q=n_buckets, labels=[f'Q{i+1}' for i in range(n_buckets)])
        
        elif bucket_type == 'custom':
            mapping = bucket_config.get('mapping', {})
            return data.map(mapping).fillna('Other')
        
        else:
            return data
    
    def _apply_transformation(self, data: pd.Series, transformation: str) -> pd.Series:
        """Apply transformation to data"""
        if transformation == 'upper':
            return data.astype(str).str.upper()
        elif transformation == 'lower':
            return data.astype(str).str.lower()
        elif transformation == 'length':
            return data.astype(str).str.len()
        elif transformation == 'round':
            return data.round()
        elif transformation == 'abs':
            return data.abs()
        elif transformation == 'log':
            return np.log(data.replace(0, np.nan))
        else:
            return data
    
    async def _apply_rolling_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply rolling window aggregations"""
        if not self.time_field or self.time_field not in df.columns:
            return df
        
        # Sort by time field
        df = df.sort_values(self.time_field)
        
        for rolling_agg in self.rolling_aggregations:
            source_field = rolling_agg.get('source_field')
            agg_type = rolling_agg.get('type', 'mean')
            window = rolling_agg.get('window', self.rolling_window)
            
            if source_field in df.columns:
                rolling_series = df[source_field].rolling(window=window)
                
                if agg_type == 'mean':
                    df[f"{source_field}_rolling_{agg_type}"] = rolling_series.mean()
                elif agg_type == 'sum':
                    df[f"{source_field}_rolling_{agg_type}"] = rolling_series.sum()
                elif agg_type == 'std':
                    df[f"{source_field}_rolling_{agg_type}"] = rolling_series.std()
                elif agg_type == 'min':
                    df[f"{source_field}_rolling_{agg_type}"] = rolling_series.min()
                elif agg_type == 'max':
                    df[f"{source_field}_rolling_{agg_type}"] = rolling_series.max()
        
        return df
    
    async def _apply_hierarchical_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply hierarchical aggregations"""
        if not self.hierarchy_levels:
            return df
        
        hierarchical_results = []
        
        # Generate aggregations for each hierarchy level
        for level in range(1, len(self.hierarchy_levels) + 1):
            level_grouping = self.hierarchy_levels[:level]
            
            # Group by current level fields
            if all(field in df.columns for field in level_grouping):
                level_grouped = df.groupby(level_grouping)
                
                # Apply same aggregation rules
                for rule in self.aggregation_rules:
                    if rule.source_field in df.columns:
                        agg_result = self._apply_aggregation_rule(level_grouped, rule)
                        agg_result['hierarchy_level'] = level
                        hierarchical_results.append(agg_result)
        
        if hierarchical_results:
            # Combine hierarchical results
            combined_df = pd.concat(hierarchical_results, ignore_index=True)
            return combined_df
        
        return df
    
    async def _format_output(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Format aggregation output"""
        if self.sort_results and self.sort_by:
            # Sort by specified columns
            sort_columns = [col for col in self.sort_by if col in df.columns]
            if sort_columns:
                df = df.sort_values(sort_columns)
        
        if self.limit_results:
            df = df.head(self.limit_results)
        
        if self.output_format == 'records':
            return df.to_dict('records')
        elif self.output_format == 'pivot':
            return self._create_pivot_table(df)
        elif self.output_format == 'summary':
            return self._create_summary_report(df)
        else:
            return df.to_dict('records')
    
    def _create_pivot_table(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Create pivot table from aggregated data"""
        # This is a simplified pivot table creation
        # In production, you might want more sophisticated pivot logic
        
        if len(df.columns) < 3:
            return df.to_dict('records')
        
        # Use first column as index, second as columns, third as values
        index_col = df.columns[0]
        columns_col = df.columns[1]
        values_col = df.columns[2]
        
        try:
            pivot_df = df.pivot_table(
                index=index_col,
                columns=columns_col,
                values=values_col,
                aggfunc='first'
            ).fillna(0)
            
            return pivot_df.reset_index().to_dict('records')
        except Exception:
            # Fallback to regular records
            return df.to_dict('records')
    
    def _create_summary_report(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Create summary report from aggregated data"""
        summary = {
            'total_groups': len(df),
            'columns': list(df.columns),
            'numeric_summaries': {},
            'categorical_summaries': {},
            'data_preview': df.head(10).to_dict('records')
        }
        
        # Numeric column summaries
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            summary['numeric_summaries'][col] = {
                'count': int(df[col].count()),
                'mean': float(df[col].mean()) if not df[col].empty else None,
                'std': float(df[col].std()) if not df[col].empty else None,
                'min': float(df[col].min()) if not df[col].empty else None,
                'max': float(df[col].max()) if not df[col].empty else None,
                'sum': float(df[col].sum()) if not df[col].empty else None
            }
        
        # Categorical column summaries
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            value_counts = df[col].value_counts().head(10)
            summary['categorical_summaries'][col] = {
                'unique_count': int(df[col].nunique()),
                'top_values': value_counts.to_dict()
            }
        
        return [summary]
    
    async def _generate_aggregation_metadata(self, source_df: pd.DataFrame, result_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate metadata about aggregation process"""
        return {
            'source_records': len(source_df),
            'result_groups': len(result_df),
            'aggregation_rules_applied': len(self.aggregation_rules),
            'grouping_rules_applied': len(self.grouping_rules),
            'compression_ratio': len(result_df) / len(source_df) if len(source_df) > 0 else 0,
            'processing_mode': {
                'rolling_enabled': self.enable_rolling,
                'hierarchical_enabled': self.enable_hierarchical,
                'output_format': self.output_format
            },
            'data_quality': {
                'null_handling': self.handle_nulls,
                'missing_data_filled': self.fill_missing_periods,
                'sorted_results': self.sort_results
            }
        }
    
    # Custom aggregation functions
    def _weighted_average(self, values: pd.Series, weights: pd.Series) -> float:
        """Calculate weighted average"""
        return np.average(values, weights=weights)
    
    def _harmonic_mean(self, values: pd.Series) -> float:
        """Calculate harmonic mean"""
        values = values[values > 0]  # Remove non-positive values
        return len(values) / (1.0 / values).sum() if len(values) > 0 else 0
    
