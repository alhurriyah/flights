"""
FlyPrivate Flight Data Processor Runner
Handles loading and processing flight data from multiple sources.
"""

from flight_processor import process_flights_data
import pandas as pd
import json
import os

print("FlyPrivate Flight Data Processor")
print("===============================")

# Define all sources and their files with correct filenames
sources_and_files = [
    ('luxaviation', 'luxaviation_luxaviation_captured-list_2024-12-24_23-21-36_e5aca55b-0ad9-432d-906a-38ad67e8dc03.csv'),
    ('catchajet', 'catchajet_catchajet_captured-list_2024-12-25_00-40-51_d454ec5f-85cf-456c-bac9-ce1c41a0258b.csv'),
    ('mirai', 'mirai_mirai_captured-list_2024-12-25_00-55-30_2e04710a-9a39-48f6-ab75-ea9791382edd.csv'),
    ('sovereign', 'sovereign_sovereign_captured-list_2024-12-24_23-44-56_0e4a1c85-92f2-4814-9e20-9cee45d6d246.csv')
]

# Print source files being processed
print("\nProcessing the following sources:")
for source, file in sources_and_files:
    print(f"- {source}: {file}")
    if os.path.exists(file):
        try:
            df = pd.read_csv(file)
            print(f"  Successfully read CSV with {len(df)} rows")
            print(f"  Columns: {', '.join(df.columns)}")
        except Exception as e:
            print(f"  Error reading file: {str(e)}")
    else:
        print(f"  File not found: {file}")

# Process all data and get the combined result
print("\nStarting processing...")
output_filename = 'all_flights_output.js'
try:
    result = process_flights_data(sources_and_files, 'iata-icao.csv')
    
    # Save all results to a single file
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(result)
    except Exception as write_error:
        print(f"Error writing output file: {str(write_error)}")
        traceback.print_exc()
    
    # Count the number of flights
    try:
        # Remove the JavaScript variable declaration to parse as pure JSON
        js_content = result.replace('const hotDeals = ', '').strip().rstrip(';')
        flights = json.loads(js_content)
        source_counts = {}
        for flight in flights:
            operator = flight['operatedBy']
            source_counts[operator] = source_counts.get(operator, 0) + 1
    
        print(f"\nProcessing complete!")
        print(f"Total flights in output: {len(flights)}")
        print("\nFlights per source:")
        for source, count in source_counts.items():
            print(f"- {source}: {count} flights")
    
        # Print sample flight from each source
        if flights:
            print("\nSample flight from each source:")
            for source in set(flight['operatedBy'] for flight in flights):
                sample = next(flight for flight in flights if flight['operatedBy'] == source)
                print(f"\n{source}:")
                print(f"  Route: {sample['origin']} -> {sample['destination']}")
                print(f"  Date: {sample['date']}")
                print(f"  Price: €{sample['flyPrivatePrice']}")
        
    except json.JSONDecodeError as e:
        print(f"\nWarning: Could not parse output for statistics (error: {str(e)})")
        print("Output file was still generated.")
    
except Exception as e:
    print(f"\nError during processing: {str(e)}")
    import traceback
    traceback.print_exc()

if os.path.exists(output_filename):
    print(f"\nOutput saved to: {output_filename}")
    try:
        size = os.path.getsize(output_filename)
        print(f"File size: {size:,} bytes")
    except:
        pass
else:
    print("\nError: No output file was generated")