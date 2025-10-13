from datetime import timedelta, datetime


def parse_duration(duration_str):
    # Handle empty or None input
    if not duration_str or duration_str.strip() == '':
        return timedelta(seconds=0)

    # Handle different duration formats
    if duration_str.startswith('PT'):
        # ISO 8601 format: PT42S, PT1M30S, etc.
        duration_str = duration_str[2:]  # Remove 'PT'
        components = ['D', 'H', 'M', 'S']
        values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}

        for component in components:
            if component in duration_str:
                value, duration_str = duration_str.split(component)
                if value:  # Check if value is not empty
                    values[component] = int(value)

        total_duration = timedelta(
            days=values['D'], hours=values['H'], minutes=values['M'], seconds=values['S'])
    elif ':' in duration_str:
        # Time format: :42, 1:30, etc.
        parts = duration_str.split(':')
        if len(parts) == 2:
            # MM:SS format
            minutes = int(parts[0]) if parts[0] else 0
            seconds = int(parts[1]) if parts[1] else 0
            total_duration = timedelta(minutes=minutes, seconds=seconds)
        elif len(parts) == 3:
            # HH:MM:SS format
            hours = int(parts[0]) if parts[0] else 0
            minutes = int(parts[1]) if parts[1] else 0
            seconds = int(parts[2]) if parts[2] else 0
            total_duration = timedelta(
                hours=hours, minutes=minutes, seconds=seconds)
        else:
            # Just seconds
            total_duration = timedelta(
                seconds=int(parts[0]) if parts[0] else 0)
    else:
        # Assume it's just seconds
        try:
            total_duration = timedelta(seconds=int(duration_str))
        except (ValueError, TypeError):
            # If conversion fails, return 0 duration
            total_duration = timedelta(seconds=0)

    return total_duration


def transform_data(data):
    duration_td = parse_duration(data['duration'])
    data['duration'] = (datetime.min + duration_td).time()
    return data
