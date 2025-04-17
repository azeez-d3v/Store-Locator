import re

def decode_cloudflare_email(encoded_email):
    """
    Decode Cloudflare-protected email addresses.
    
    Args:
        encoded_email: Hex-encoded email string from data-cfemail attribute
        
    Returns:
        Decoded email address
    """
    decoded_email = ""
    k = int(encoded_email[:2], 16)
    
    for i in range(2, len(encoded_email)-1, 2):
        decoded_email += chr(int(encoded_email[i:i+2], 16) ^ k)
        
    return decoded_email

def extract_state_postcode(address):
    """
    Extract state and postcode from an Australian address
    
    Args:
        address: Full address string
        
    Returns:
        Tuple of (state, postcode)
    """
    state = None
    postcode = None
    
    # Australian state pattern
    state_pattern = r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b'
    state_match = re.search(state_pattern, address)
    if state_match:
        state = state_match.group(0)
    
    # Australian postcode pattern (4 digits)
    postcode_pattern = r'\b(\d{4})\b'
    postcode_match = re.search(postcode_pattern, address)
    if postcode_match:
        postcode = postcode_match.group(0)
        
    return (state, postcode)

def extract_trading_hours(hours_text, format_type='standard'):
    """
    Extract structured trading hours from text representation
    
    Args:
        hours_text: Text containing opening hours information
        format_type: The format of the hours text ('standard', 'range', etc.)
        
    Returns:
        Dictionary with days as keys and opening/closing times as values
    """
    trading_hours = {}
    
    if format_type == 'standard':
        # Format: "9:00 AM - 5:00 PM"
        if 'closed' in hours_text.lower():
            return {'open': 'Closed', 'closed': 'Closed'}
        
        # Split by dash or hyphen
        hour_parts = re.split(r'–|-', hours_text)
        if len(hour_parts) == 2:
            open_time = hour_parts[0].strip()
            close_time = hour_parts[1].strip()
            return {'open': open_time, 'closed': close_time}
    
    elif format_type == 'range':
        # Format for day ranges like "Monday - Friday: 9:00 AM - 5:00 PM"
        day_hours = hours_text.split(':')
        if len(day_hours) >= 2:
            day_range = day_hours[0].strip()
            hours = ':'.join(day_hours[1:]).strip()  # Rejoin in case there are colons in the time
            
            day_mapping = {
                'monday': 'Monday',
                'tuesday': 'Tuesday',
                'wednesday': 'Wednesday',
                'thursday': 'Thursday',
                'friday': 'Friday',
                'saturday': 'Saturday',
                'sunday': 'Sunday'
            }
            
            if '-' in day_range or '–' in day_range:
                # Handle day range like "Monday - Friday"
                day_parts = re.split(r'–|-', day_range)
                if len(day_parts) == 2:
                    start_day = day_parts[0].strip().lower()
                    end_day = day_parts[1].strip().lower()
                    
                    # Map to standard day names
                    days_in_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                    start_idx = next((i for i, d in enumerate(days_in_order) if start_day in d), -1)
                    end_idx = next((i for i, d in enumerate(days_in_order) if end_day in d), -1)
                    
                    if start_idx != -1 and end_idx != -1:
                        # Process all days in the range
                        for day_idx in range(start_idx, end_idx + 1):
                            day_name = day_mapping[days_in_order[day_idx]]
                            # Parse the hours part
                            trading_hours[day_name] = extract_trading_hours(hours, 'standard')
            else:
                # Single day
                for key, value in day_mapping.items():
                    if key in day_range.lower():
                        trading_hours[value] = extract_trading_hours(hours, 'standard')
                        break
    
    return trading_hours