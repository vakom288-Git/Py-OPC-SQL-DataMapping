def parse_fixed_offset(offset_str):
    # Implementation for parsing fixed timezone offsets
    pass

def get_opc_local_tz():
    # Get local timezone based on environment variable OPC_LOCAL_UTC_OFFSET
    return os.getenv('OPC_LOCAL_UTC_OFFSET', '+00:00')

def ensure_aware_opc_ts(ts):
    # Logic to ensure the timestamp is timezone aware based on the provided rules
    pass

def msr_time_to_iso(ts):
    # Logic for converting msr_time to ISO format
    return ensure_aware_opc_ts(ts).isoformat()

def update_to_db_naive(ts):
    # Logic to convert msr_time based on MEASUREMENT_TIME_MODE
    pass

# Update drain_buffer to handle record['msr_time'] as needed
