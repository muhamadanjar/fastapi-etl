"""
Date and time utilities for the ETL system.
Provides functions for date manipulation, formatting, and timezone handling.
"""

import re
from datetime import datetime, timedelta, date, time, timezone
from typing import Optional, Union, List, Dict, Any, Tuple
from dateutil import parser, tz
from dateutil.relativedelta import relativedelta
import calendar
import pytz

from app.utils.logger import get_logger

logger = get_logger(__name__)

# Common date formats
COMMON_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y/%m/%d",
    "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m-%d-%Y",
    "%m/%d/%Y",
    "%m-%d-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d %b %Y",
    "%d %B %Y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%S%z"
]

# Default timezone
DEFAULT_TIMEZONE = "UTC"


def get_current_timestamp(timezone_name: Optional[str] = None) -> datetime:
    """
    Get current timestamp with optional timezone.
    
    Args:
        timezone_name: Timezone name (e.g., 'UTC', 'US/Eastern')
        
    Returns:
        Current datetime with timezone
    """
    try:
        if timezone_name:
            tz_obj = pytz.timezone(timezone_name)
            return datetime.now(tz_obj)
        else:
            return datetime.utcnow().replace(tzinfo=pytz.UTC)
            
    except Exception as e:
        logger.log_error("get_current_timestamp", e, {"timezone_name": timezone_name})
        return datetime.utcnow().replace(tzinfo=pytz.UTC)


def format_datetime(dt: datetime, format_string: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime to string.
    
    Args:
        dt: Datetime object to format
        format_string: Format string
        
    Returns:
        Formatted datetime string
    """
    try:
        if dt is None:
            return ""
        
        return dt.strftime(format_string)
        
    except Exception as e:
        logger.log_error("format_datetime", e, {
            "datetime": str(dt),
            "format_string": format_string
        })
        return str(dt) if dt else ""


def parse_datetime(date_string: str, 
                  format_string: Optional[str] = None,
                  timezone_name: Optional[str] = None,
                  fuzzy: bool = True) -> Optional[datetime]:
    """
    Parse string to datetime with multiple format support.
    
    Args:
        date_string: String to parse
        format_string: Specific format to use (if None, try common formats)
        timezone_name: Timezone to apply to naive datetime
        fuzzy: Whether to use fuzzy parsing
        
    Returns:
        Parsed datetime object or None if parsing fails
    """
    try:
        if not date_string or not isinstance(date_string, str):
            return None
        
        date_string = date_string.strip()
        
        # Try specific format first if provided
        if format_string:
            try:
                dt = datetime.strptime(date_string, format_string)
                return _apply_timezone(dt, timezone_name)
            except ValueError:
                pass
        
        # Try dateutil parser (handles many formats automatically)
        try:
            dt = parser.parse(date_string, fuzzy=fuzzy)
            return _apply_timezone(dt, timezone_name)
        except (ValueError, parser.ParserError):
            pass
        
        # Try common formats
        for fmt in COMMON_DATE_FORMATS:
            try:
                dt = datetime.strptime(date_string, fmt)
                return _apply_timezone(dt, timezone_name)
            except ValueError:
                continue
        
        # Try ISO format variations
        iso_variations = [
            date_string.replace('T', ' '),
            date_string.replace('Z', '+00:00'),
            date_string.split('.')[0] if '.' in date_string else date_string
        ]
        
        for variation in iso_variations:
            try:
                dt = parser.parse(variation)
                return _apply_timezone(dt, timezone_name)
            except (ValueError, parser.ParserError):
                continue
        
        logger.log_operation("parse_datetime_failed", {
            "date_string": date_string,
            "format_string": format_string,
            "timezone_name": timezone_name
        })
        
        return None
        
    except Exception as e:
        logger.log_error("parse_datetime", e, {
            "date_string": date_string,
            "format_string": format_string
        })
        return None


def _apply_timezone(dt: datetime, timezone_name: Optional[str]) -> datetime:
    """Apply timezone to datetime object."""
    if timezone_name and dt.tzinfo is None:
        tz_obj = pytz.timezone(timezone_name)
        return tz_obj.localize(dt)
    elif dt.tzinfo is None:
        return pytz.UTC.localize(dt)
    return dt


def convert_timezone(dt: datetime, target_timezone: str) -> datetime:
    """
    Convert datetime to different timezone.
    
    Args:
        dt: Datetime to convert
        target_timezone: Target timezone name
        
    Returns:
        Datetime in target timezone
    """
    try:
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        
        target_tz = pytz.timezone(target_timezone)
        return dt.astimezone(target_tz)
        
    except Exception as e:
        logger.log_error("convert_timezone", e, {
            "datetime": str(dt),
            "target_timezone": target_timezone
        })
        return dt


def get_date_range(start_date: Union[str, datetime, date], 
                  end_date: Union[str, datetime, date],
                  step_days: int = 1) -> List[date]:
    """
    Generate list of dates between start and end date.
    
    Args:
        start_date: Start date
        end_date: End date
        step_days: Step size in days
        
    Returns:
        List of date objects
    """
    try:
        # Convert to date objects
        if isinstance(start_date, str):
            start_date = parse_datetime(start_date).date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
        
        if isinstance(end_date, str):
            end_date = parse_datetime(end_date).date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
        
        dates = []
        current_date = start_date
        
        while current_date <= end_date:
            dates.append(current_date)
            current_date += timedelta(days=step_days)
        
        return dates
        
    except Exception as e:
        logger.log_error("get_date_range", e, {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "step_days": step_days
        })
        return []


def calculate_duration(start_time: datetime, end_time: datetime) -> Dict[str, Any]:
    """
    Calculate duration between two datetime objects.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        
    Returns:
        Dictionary with duration information
    """
    try:
        if not start_time or not end_time:
            return {"error": "Invalid datetime objects"}
        
        duration = end_time - start_time
        total_seconds = duration.total_seconds()
        
        days = duration.days
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return {
            "total_seconds": total_seconds,
            "total_minutes": total_seconds / 60,
            "total_hours": total_seconds / 3600,
            "total_days": total_seconds / 86400,
            "days": days,
            "hours": int(hours),
            "minutes": int(minutes),
            "seconds": int(seconds),
            "human_readable": format_duration(total_seconds)
        }
        
    except Exception as e:
        logger.log_error("calculate_duration", e, {
            "start_time": str(start_time),
            "end_time": str(end_time)
        })
        return {"error": str(e)}


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable duration string
    """
    try:
        if seconds < 0:
            return "Invalid duration"
        
        if seconds < 60:
            return f"{seconds:.2f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.2f} minutes"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.2f} hours"
        else:
            days = seconds / 86400
            return f"{days:.2f} days"
            
    except Exception as e:
        logger.log_error("format_duration", e, {"seconds": seconds})
        return f"{seconds} seconds"


def get_week_boundaries(date_obj: Union[str, datetime, date]) -> Tuple[date, date]:
    """
    Get start and end dates of the week containing the given date.
    
    Args:
        date_obj: Date object or string
        
    Returns:
        Tuple of (week_start, week_end) dates
    """
    try:
        if isinstance(date_obj, str):
            date_obj = parse_datetime(date_obj).date()
        elif isinstance(date_obj, datetime):
            date_obj = date_obj.date()
        
        # Monday as start of week
        days_since_monday = date_obj.weekday()
        week_start = date_obj - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)
        
        return week_start, week_end
        
    except Exception as e:
        logger.log_error("get_week_boundaries", e, {"date_obj": str(date_obj)})
        return date.today(), date.today()


def get_month_boundaries(date_obj: Union[str, datetime, date]) -> Tuple[date, date]:
    """
    Get start and end dates of the month containing the given date.
    
    Args:
        date_obj: Date object or string
        
    Returns:
        Tuple of (month_start, month_end) dates
    """
    try:
        if isinstance(date_obj, str):
            date_obj = parse_datetime(date_obj).date()
        elif isinstance(date_obj, datetime):
            date_obj = date_obj.date()
        
        month_start = date_obj.replace(day=1)
        
        # Get last day of month
        last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
        month_end = date_obj.replace(day=last_day)
        
        return month_start, month_end
        
    except Exception as e:
        logger.log_error("get_month_boundaries", e, {"date_obj": str(date_obj)})
        return date.today(), date.today()


def get_quarter_boundaries(date_obj: Union[str, datetime, date]) -> Tuple[date, date]:
    """
    Get start and end dates of the quarter containing the given date.
    
    Args:
        date_obj: Date object or string
        
    Returns:
        Tuple of (quarter_start, quarter_end) dates
    """
    try:
        if isinstance(date_obj, str):
            date_obj = parse_datetime(date_obj).date()
        elif isinstance(date_obj, datetime):
            date_obj = date_obj.date()
        
        quarter = (date_obj.month - 1) // 3 + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        
        quarter_start = date(date_obj.year, quarter_start_month, 1)
        
        # Get last day of quarter
        quarter_end_month = quarter_start_month + 2
        last_day = calendar.monthrange(date_obj.year, quarter_end_month)[1]
        quarter_end = date(date_obj.year, quarter_end_month, last_day)
        
        return quarter_start, quarter_end
        
    except Exception as e:
        logger.log_error("get_quarter_boundaries", e, {"date_obj": str(date_obj)})
        return date.today(), date.today()


def add_business_days(start_date: Union[str, datetime, date], business_days: int) -> date:
    """
    Add business days to a date (excluding weekends).
    
    Args:
        start_date: Starting date
        business_days: Number of business days to add
        
    Returns:
        Resulting date
    """
    try:
        if isinstance(start_date, str):
            start_date = parse_datetime(start_date).date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
        
        current_date = start_date
        days_added = 0
        
        while days_added < business_days:
            current_date += timedelta(days=1)
            # Monday = 0, Sunday = 6
            if current_date.weekday() < 5:  # Monday to Friday
                days_added += 1
        
        return current_date
        
    except Exception as e:
        logger.log_error("add_business_days", e, {
            "start_date": str(start_date),
            "business_days": business_days
        })
        return start_date if isinstance(start_date, date) else date.today()


def get_business_days_count(start_date: Union[str, datetime, date], 
                           end_date: Union[str, datetime, date]) -> int:
    """
    Count business days between two dates.
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Number of business days
    """
    try:
        if isinstance(start_date, str):
            start_date = parse_datetime(start_date).date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
        
        if isinstance(end_date, str):
            end_date = parse_datetime(end_date).date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
        
        business_days = 0
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Monday to Friday
                business_days += 1
            current_date += timedelta(days=1)
        
        return business_days
        
    except Exception as e:
        logger.log_error("get_business_days_count", e, {
            "start_date": str(start_date),
            "end_date": str(end_date)
        })
        return 0


def is_business_day(date_obj: Union[str, datetime, date]) -> bool:
    """
    Check if a date is a business day (Monday to Friday).
    
    Args:
        date_obj: Date to check
        
    Returns:
        True if business day, False otherwise
    """
    try:
        if isinstance(date_obj, str):
            date_obj = parse_datetime(date_obj).date()
        elif isinstance(date_obj, datetime):
            date_obj = date_obj.date()
        
        return date_obj.weekday() < 5
        
    except Exception as e:
        logger.log_error("is_business_day", e, {"date_obj": str(date_obj)})
        return False


def get_relative_date(base_date: Union[str, datetime, date], **kwargs) -> date:
    """
    Get relative date using relativedelta.
    
    Args:
        base_date: Base date
        **kwargs: Keyword arguments for relativedelta (years, months, days, etc.)
        
    Returns:
        Calculated date
    """
    try:
        if isinstance(base_date, str):
            base_date = parse_datetime(base_date).date()
        elif isinstance(base_date, datetime):
            base_date = base_date.date()
        
        # Convert date to datetime for relativedelta
        base_datetime = datetime.combine(base_date, time.min)
        result_datetime = base_datetime + relativedelta(**kwargs)
        
        return result_datetime.date()
        
    except Exception as e:
        logger.log_error("get_relative_date", e, {
            "base_date": str(base_date),
            "kwargs": kwargs
        })
        return base_date if isinstance(base_date, date) else date.today()


def get_age_in_years(birth_date: Union[str, datetime, date], 
                    reference_date: Optional[Union[str, datetime, date]] = None) -> float:
    """
    Calculate age in years between two dates.
    
    Args:
        birth_date: Birth/start date
        reference_date: Reference date (default: today)
        
    Returns:
        Age in years (with decimals)
    """
    try:
        if isinstance(birth_date, str):
            birth_date = parse_datetime(birth_date).date()
        elif isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        
        if reference_date is None:
            reference_date = date.today()
        elif isinstance(reference_date, str):
            reference_date = parse_datetime(reference_date).date()
        elif isinstance(reference_date, datetime):
            reference_date = reference_date.date()
        
        # Calculate age using relativedelta for accurate year calculation
        age_delta = relativedelta(reference_date, birth_date)
        
        # Convert to decimal years
        age_years = age_delta.years
        age_years += age_delta.months / 12
        age_years += age_delta.days / 365.25
        
        return round(age_years, 2)
        
    except Exception as e:
        logger.log_error("get_age_in_years", e, {
            "birth_date": str(birth_date),
            "reference_date": str(reference_date)
        })
        return 0.0


def validate_date_range(start_date: Union[str, datetime, date], 
                       end_date: Union[str, datetime, date],
                       max_days: Optional[int] = None) -> Dict[str, Any]:
    """
    Validate a date range.
    
    Args:
        start_date: Start date
        end_date: End date
        max_days: Maximum allowed days in range
        
    Returns:
        Validation result dictionary
    """
    try:
        if isinstance(start_date, str):
            start_date = parse_datetime(start_date)
        if isinstance(end_date, str):
            end_date = parse_datetime(end_date)
        
        if not start_date or not end_date:
            return {
                "is_valid": False,
                "error": "Invalid date format"
            }
        
        # Convert to date objects for comparison
        start_date_obj = start_date.date() if isinstance(start_date, datetime) else start_date
        end_date_obj = end_date.date() if isinstance(end_date, datetime) else end_date
        
        if start_date_obj > end_date_obj:
            return {
                "is_valid": False,
                "error": "Start date must be before or equal to end date"
            }
        
        days_diff = (end_date_obj - start_date_obj).days
        
        if max_days and days_diff > max_days:
            return {
                "is_valid": False,
                "error": f"Date range exceeds maximum allowed days ({max_days})",
                "days_diff": days_diff
            }
        
        return {
            "is_valid": True,
            "days_diff": days_diff,
            "start_date": start_date_obj,
            "end_date": end_date_obj
        }
        
    except Exception as e:
        logger.log_error("validate_date_range", e, {
            "start_date": str(start_date),
            "end_date": str(end_date)
        })
        return {
            "is_valid": False,
            "error": str(e)
        }


def get_time_periods(period_type: str, count: int = 12) -> List[Dict[str, Any]]:
    """
    Generate list of time periods (months, quarters, years).
    
    Args:
        period_type: Type of period ('month', 'quarter', 'year')
        count: Number of periods to generate
        
    Returns:
        List of period dictionaries
    """
    try:
        periods = []
        current_date = date.today()
        
        for i in range(count):
            if period_type == 'month':
                period_date = get_relative_date(current_date, months=-i)
                start_date, end_date = get_month_boundaries(period_date)
                period_name = period_date.strftime('%Y-%m')
                
            elif period_type == 'quarter':
                period_date = get_relative_date(current_date, months=-(i*3))
                start_date, end_date = get_quarter_boundaries(period_date)
                quarter_num = (period_date.month - 1) // 3 + 1
                period_name = f"{period_date.year}-Q{quarter_num}"
                
            elif period_type == 'year':
                period_date = get_relative_date(current_date, years=-i)
                start_date = date(period_date.year, 1, 1)
                end_date = date(period_date.year, 12, 31)
                period_name = str(period_date.year)
                
            else:
                raise ValueError(f"Unsupported period type: {period_type}")
            
            periods.append({
                "period_name": period_name,
                "start_date": start_date,
                "end_date": end_date,
                "period_type": period_type
            })
        
        return list(reversed(periods))  # Return in chronological order
        
    except Exception as e:
        logger.log_error("get_time_periods", e, {
            "period_type": period_type,
            "count": count
        })
        return []


def is_datetime_recent(dt: datetime, minutes: int = 5) -> bool:
    """
    Check if datetime is recent (within specified minutes).
    
    Args:
        dt: Datetime to check
        minutes: Minutes threshold
        
    Returns:
        True if recent, False otherwise
    """
    try:
        if not dt:
            return False
        
        # Ensure both datetimes have timezone info
        current_time = get_current_timestamp()
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        if current_time.tzinfo is None:
            current_time = pytz.UTC.localize(current_time)
        
        time_diff = current_time - dt
        return time_diff.total_seconds() <= (minutes * 60)
        
    except Exception as e:
        logger.log_error("is_datetime_recent", e, {
            "datetime": str(dt),
            "minutes": minutes
        })
        return False
    