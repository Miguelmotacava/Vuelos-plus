import happybase

HBASE_HOST = 'localhost' # Adjust if needed, but normally it's localhost in the user's setup if running streamlit locally

def test_filter():
    try:
        conn = happybase.Connection(HBASE_HOST, port=9090)
        table = conn.table('vuelos')
        
        # Test 1: Daily with prefix
        prefix = "20080101_JFK"
        print(f"Scanning with prefix: {prefix}")
        scanner = table.scan(row_prefix=prefix.encode(), limit=5)
        count = 0
        for k, d in scanner:
            print(f"Found: {k.decode()}")
            count += 1
        print(f"Total found: {count}")

        # Test 2: Monthly with filter
        prefix_m = "200801"
        filt = "SingleColumnValueFilter('route', 'Origin', =, 'binary:JFK')"
        print(f"Scanning with prefix: {prefix_m} and filter: {filt}")
        scanner = table.scan(row_prefix=prefix_m.encode(), filter=filt.encode(), limit=5)
        count = 0
        for k, d in scanner:
            print(f"Found: {k.decode()}")
            count += 1
        print(f"Total found: {count}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_filter()
