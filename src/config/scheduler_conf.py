"""
LordFilm Parser - Schedules Configuration
Configuration for daily and weekly parsing schedules
"""

from src.config.config import YEAR_RANGE, DEFAULT_URL, DEFAULT_CATEGORY

# Calculate year range
START_YEAR, END_YEAR = YEAR_RANGE
ALL_YEARS = list(range(START_YEAR, END_YEAR + 1))
RECENT_YEARS = [END_YEAR, END_YEAR - 1]  # Current and previous year

URL_TEMPLATES = {
    'main_catalog': f"{DEFAULT_URL}/{DEFAULT_CATEGORY}/page/{{page}}/",
    'yearly_catalog': f"{DEFAULT_URL}/{DEFAULT_CATEGORY}/{{year}}/page/{{page}}/"
}

# =============================================================================
# Parsing Schedules Configuration
# =============================================================================

PARSING_SCHEDULES = {
    'daily': {
        'time': '03:00',
        'type': 'daily',
        'exclude_days': ['sunday'],  # Skip on weekly parsing day
        'enabled': True,
        'tasks': [
            {
                'name': 'update_existing_ratings',
                'description': 'Update ratings for all existing films in database',
                'task_type': 'update_ratings',
                'priority': 'high'
            },
            {
                'name': 'discover_new_films_main',
                'description': 'Discover new films from main catalog pages',
                'task_type': 'parse_pages',
                'url_template': 'main_catalog',
                'pages': 50,  # From config.DAILY_PAGES
                'priority': 'medium'
            },
            {
                'name': 'discover_new_films_yearly',
                'description': 'Discover new films from recent years catalogs',
                'task_type': 'parse_pages',
                'url_template': 'yearly_catalog',
                'years': RECENT_YEARS,  # [2024, 2025]
                'pages': 50,  # From config.DAILY_PAGES
                'priority': 'medium'
            }
        ]
    },

    'weekly': {
        'time': '04:00',
        'day': 'sunday',
        'type': 'weekly',
        'enabled': True,
        'tasks': [
            {
                'name': 'full_main_catalog',
                'description': 'Complete parsing of main catalog (all years mixed)',
                'task_type': 'parse_pages',
                'url_template': 'main_catalog',
                'pages': 'all',  # Parse all available pages
                'priority': 'high'
            },
            {
                'name': 'yearly_catalogs_complete',
                'description': 'Complete parsing of catalogs by years 2016-2025',
                'task_type': 'parse_pages',
                'url_template': 'yearly_catalog',
                'years': ALL_YEARS,  # [2016, 2017, ..., 2025]
                'pages': 'all',  # Parse all available pages for each year
                'priority': 'high'
            }
        ]
    }
}

# =============================================================================
# Task Execution Configuration
# =============================================================================

TASK_CONFIG = {
    'max_concurrent_tasks': 1,  # Run tasks sequentially
    'task_timeout': 3600,  # 1 hour timeout per task
    'retry_failed_tasks': True,
    'max_task_retries': 2,
    'retry_delay': 300,  # 5 minutes between retries
}

# =============================================================================
# Monitoring & Notifications
# =============================================================================

MONITORING_CONFIG = {
    'enable_health_checks': True,
    'enable_performance_metrics': True,
    'send_completion_notifications': True,
    'send_error_notifications': True,
    'notification_channel': 'telegram',  # or 'log'
}

# =============================================================================
# Schedule Validation
# =============================================================================

VALID_SCHEDULE_TYPES = ['daily', 'weekly']
VALID_TASK_TYPES = ['parse_pages', 'update_ratings']
VALID_PRIORITIES = ['low', 'medium', 'high', 'critical']


def validate_schedules():
    """Validate schedules configuration"""
    for schedule_name, schedule_config in PARSING_SCHEDULES.items():
        assert schedule_name in VALID_SCHEDULE_TYPES, f"Invalid schedule type: {schedule_name}"
        assert 'tasks' in schedule_config, f"Missing tasks in {schedule_name}"

        for task in schedule_config['tasks']:
            assert task['task_type'] in VALID_TASK_TYPES, f"Invalid task type: {task['task_type']}"
            assert task['priority'] in VALID_PRIORITIES, f"Invalid priority: {task['priority']}"


# Validate on import
validate_schedules()