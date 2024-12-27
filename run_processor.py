"""
FlyPrivate Flight Data Processor Runner
Handles loading and processing flight data from multiple sources.
Performance-optimized version.
"""

from flight_processor import process_flights_data
import pandas as pd
import json
import os
from typing import Dict, List, Tuple
import traceback
from datetime import datetime

# Cache for file metadata
FILE_METADATA_CACHE: Dict[str, Dict] = {}

def get_file_metadata(file_path: str) -> Dict:
    """Get file metadata with caching."""
    if file_path in FILE_METADATA_CACHE:
        return FILE_METADATA_CACHE[file_path]
        
    metadata = {
        'exists': os.path.exists(file_path),
        'size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
        'mtime': os.path.getmtime(file_path) if os.path.exists(file_path) else 0
    }
    
    FILE_METADATA_CACHE[file_path] = metadata
    return metadata

def process_csv_file(file_path: str) -> Tuple[int, List[str]]:
    """Process CSV file with optimized memory usage."""
    try:
        # Read CSV in chunks to reduce memory usage
        chunk_size = 100
        total_rows = 0
        columns = []
        
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            if not columns:
                columns = list(chunk.columns)
            total_rows += len(chunk)
            
        return total_rows, columns
    except Exception as e:
        print(f"  Error reading file: {str(e)}")
        return 0, []

def main():
    print("FlyPrivate Flight Data Processor")
    print("===============================")

    # Define sources and files
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
        metadata = get_file_metadata(file)
        
        if metadata['exists']:
            try:
                total_rows, columns = process_csv_file(file)
                print(f"  Successfully read CSV with {total_rows} rows")
                print(f"  Columns: {', '.join(columns)}")
            except Exception as e:
                print(f"  Error reading file: {str(e)}")
        else:
            print(f"  File not found: {file}")

    # Process all data and get the combined result
    print("\nStarting processing...")
    output_filename = 'all_flights_output.js'
    
    # In run_processor.py, modify the saving section:

    try:
        result = process_flights_data(sources_and_files, 'iata-icao.csv')
        
        # Check if result is empty
        if not result:
            print("Error: No data to write - result is empty")
            exit(1)
            
        print(f"Debug: Length of result string: {len(result)}")
        print(f"Debug: First 100 characters: {result[:100]}")
        
        # Save results with proper encoding and explicit flush
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(result)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
        except Exception as write_error:
            print(f"Error writing output file: {str(write_error)}")
            traceback.print_exc()
        
        # Process and display statistics
        try:
            # Remove JavaScript variable declaration to parse as JSON
            js_content = result.replace('const hotDeals = ', '').strip().rstrip(';')
            flights = json.loads(js_content)
            
            # Gather statistics
            source_counts = {}
            unique_origins = set()
            unique_destinations = set()
            price_ranges = {
                'min_flyprivate': float('inf'),
                'max_flyprivate': 0,
                'min_charter': float('inf'),
                'max_charter': 0
            }
            
            # Process each flight once
            for flight in flights:
                # Count by source
                operator = flight['operatedBy']
                source_counts[operator] = source_counts.get(operator, 0) + 1
                
                # Track unique cities
                unique_origins.add(flight['origin'])
                unique_destinations.add(flight['destination'])
                
                # Track price ranges
                price_ranges['min_flyprivate'] = min(price_ranges['min_flyprivate'], flight['flyPrivatePrice'])
                price_ranges['max_flyprivate'] = max(price_ranges['max_flyprivate'], flight['flyPrivatePrice'])
                price_ranges['min_charter'] = min(price_ranges['min_charter'], flight['charterPrice'])
                price_ranges['max_charter'] = max(price_ranges['max_charter'], flight['charterPrice'])
            
            # Print statistics
            print(f"\nProcessing complete!")
            print(f"Total flights in output: {len(flights)}")
            print(f"Unique origins: {len(unique_origins)}")
            print(f"Unique destinations: {len(unique_destinations)}")
            
            print("\nFlights per source:")
            for source, count in source_counts.items():
                print(f"- {source}: {count} flights")
            
            print("\nPrice Ranges:")
            print(f"FlyPrivate Prices: €{price_ranges['min_flyprivate']:,} to €{price_ranges['max_flyprivate']:,}")
            print(f"Charter Prices: €{price_ranges['min_charter']:,} to €{price_ranges['max_charter']:,}")
            
            # Print sample flights
            print("\nSample flight from each source:")
            processed_sources = set()
            for flight in flights:
                source = flight['operatedBy']
                if source not in processed_sources:
                    print(f"\n{source}:")
                    print(f"  Route: {flight['origin']} -> {flight['destination']}")
                    print(f"  Date: {flight['date']}")
                    print(f"  Price: €{flight['flyPrivatePrice']:,}")
                    processed_sources.add(source)
            
        except json.JSONDecodeError as e:
            print(f"\nWarning: Could not parse output for statistics (error: {str(e)})")
            print("Output file was still generated.")
    
    except Exception as e:
        print(f"\nError during processing: {str(e)}")
        traceback.print_exc()

    # Check output file
    if os.path.exists(output_filename):
        metadata = get_file_metadata(output_filename)
        print(f"\nOutput saved to: {output_filename}")
        print(f"File size: {metadata['size']:,} bytes")
        print(f"Last modified: {datetime.fromtimestamp(metadata['mtime']).strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("\nError: No output file was generated")

if __name__ == "__main__":
    main()