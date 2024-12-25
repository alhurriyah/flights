"""
FlyPrivate Flight Data Processor
===============================
Processes flight data from multiple sources and standardizes it for the FlyPrivate platform.
Handles different data formats, coordinates lookups, price calculations, and data validation.
"""

import pandas as pd
import random
import math
from datetime import datetime
import json
import traceback
import os
import unicodedata
import re

def clean_city_name(name):
    """Standardize city names by removing accents and airport codes."""
    common_replacements = {
        'Zürich': 'Zurich',
        'ZÃ¼rich': 'Zurich',
        'Chambéry': 'Chambery',
        'ChambÃ©ry': 'Chambery',
        'Málaga': 'Malaga',
        'MÃ¡laga': 'Malaga',
        'Düsseldorf': 'Dusseldorf',
        'DÃ¼sseldorf': 'Dusseldorf',
        'Liège': 'Liege',
        'LiÃ¨ge': 'Liege',
        'Genève': 'Geneva',
        'GenÃ¨ve': 'Geneva',
        'Václav': 'Vaclav',
        'VÃ¡clav': 'Vaclav',
        'Nice-Côte': 'Nice',
        'Nice-CÃ´te': 'Nice',
        'Orléans': 'Orleans',
        'OrlÃ©ans': 'Orleans',
        'Hyères': 'Hyeres',
        'HyÃ¨res': 'Hyeres',
        'Mérignac': 'Bordeaux',
        'MÃ©rignac': 'Bordeaux',
        'Paris-Le Bourget': 'Paris',
        'Rotterdam The Hague': 'Rotterdam',
        'Côte': 'Cote',
        'CÃ´te': 'Cote',
        'Wevelgem': 'Bruxelles'
    }

    special_chars = {
    'a€': 'o',  # Fix for Ciampinoa€
    'a¢': 'o',  # Another possible encoding
    '\'': '',   # Remove apostrophes
    '-': ' '    # Convert hyphens to spaces
}
    
    if isinstance(name, str):
        # Decode unicode escapes
        name = bytes(name, 'utf-8').decode('unicode-escape')
        
        # Apply common replacements
        for accented, plain in common_replacements.items():
            name = name.replace(accented, plain)
            
        # Remove airport codes and extra spaces
        name = re.sub(r'\([^)]+\)', '', name).strip()
            
        # Remove any remaining accents
        name = ''.join(c for c in unicodedata.normalize('NFD', name)
                      if unicodedata.category(c) != 'Mn')
    
    return name.strip()

def normalize_string(s):
    """Normalize string for comparison purposes."""
    if isinstance(s, str):
        # Clean the name first
        s = clean_city_name(s)
        # Convert to lowercase and clean up
        s = s.lower().strip()
        # Remove extra whitespace
        s = ' '.join(s.split())
        return s
    return ''

def format_date(date_str):
    """Format date string to be consistent."""
    try:
        # Try different date formats
        for fmt in ['%Y-%m-%d', '%B %d', '%d %b', '%d/%m/%Y', '%d/%m']:
            try:
                date_obj = datetime.strptime(str(date_str).strip(), fmt)
                # Set year appropriate for the month
                month = date_obj.month
                if month == 12:
                    date_obj = date_obj.replace(year=2024)
                else:
                    date_obj = date_obj.replace(year=2025)
                return date_obj.strftime('%B %d')
            except ValueError:
                continue
        
        # If no format matches, return default
        return 'December 25'
    except Exception as e:
        print(f"Error formatting date {date_str}: {str(e)}")
        return 'December 25'

def load_airports_data(airports_csv_path):
    """Load and process airports data from CSV into searchable dictionaries."""
    if not os.path.exists(airports_csv_path):
        raise FileNotFoundError(f"Airport data file not found: {airports_csv_path}")
        
    airports_df = pd.read_csv(airports_csv_path)
    airports_dict = {
        'by_city': {},
        'by_normalized_city': {},
        'by_iata': {},
        'by_icao': {},
        'cities_to_coords': {}  # New index mapping city names to coordinates
    }
    
    for _, row in airports_df.iterrows():
        city = row['region_name'].strip() if not pd.isna(row['region_name']) else None
        iata = row['iata'].strip() if not pd.isna(row['iata']) else None
        icao = row['icao'].strip() if not pd.isna(row['icao']) else None
        
        if city and not pd.isna(row['latitude']) and not pd.isna(row['longitude']):
            city = clean_city_name(city)
            normalized_city = normalize_string(city)
            coords = (float(row['latitude']), float(row['longitude']))
            
            # Store coordinates for each variation of the city name
            airports_dict['cities_to_coords'][normalized_city] = coords
            
            # Store the first airport's coordinates for each city
            if normalized_city not in airports_dict['by_normalized_city']:
                airport_data = {
                    'iata': iata,
                    'icao': icao,
                    'latitude': coords[0],
                    'longitude': coords[1],
                    'city': city,
                    'normalized_city': normalized_city
                }
                airports_dict['by_normalized_city'][normalized_city] = airport_data
                if city:
                    airports_dict['by_city'][city] = airport_data
                if iata:
                    airports_dict['by_iata'][iata] = airport_data
                if icao:
                    airports_dict['by_icao'][icao] = airport_data
    
    return airports_dict

def generate_id():
    """Generate a random 12-digit numeric ID."""
    return int(''.join([str(random.randint(0, 9)) for _ in range(12)]))

def calculate_prices(base_price):
    """Calculate charter and FlyPrivate prices."""
    multiplier = random.choice([2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0])
    charter_price = math.ceil(base_price * multiplier)
    flyprivate_price = math.ceil(base_price * 1.2)
    return charter_price, flyprivate_price

def get_airport_coordinates(location, airports_dict):
    """Get coordinates for any airport in the specified city."""
    try:
        if not location:
            return None, None
            
        original_location = location
        
        # Clean up the location name
        location = clean_city_name(location)
        location = re.sub(r'international|airport|airfield|aerodrome|terminal|\(.*?\)', '', location, flags=re.IGNORECASE)
        location = re.sub(r'-.*$', '', location)  # Remove anything after a dash
        location = location.strip()
        
        # Common city name mappings
        city_mappings = {
            'amsterdam schiphol': 'amsterdam',
            'paris le bourget': 'paris',
            'paris orly': 'paris',
            'paris cdg': 'paris',
            'london heathrow': 'london',
            'london gatwick': 'london',
            'london luton': 'london',
            'london stansted': 'london',
            'london city': 'london',
            'zurich kloten': 'zurich',
            'geneva cointrin': 'geneva',
            'chambery aix': 'chambery',
            'nice cote': 'nice',
            'frankfurt main': 'frankfurt',
            'frankfurt hahn': 'frankfurt',
            'toulon-hyeres': 'toulon',
            'toulon hyeres': 'toulon',
            'ciampino g b pastine': 'rome',
            'ciampinoa g b pastine': 'rome',
            'ciampino': 'rome',
            'vaclav havel': 'prague',
            'adolfo lopez mateos': 'mexico city',
            'faa\'a': 'papeete',
            'abeid amani karume': 'zanzibar',
            'edinburgh airport': 'edinburgh',
            'innsbruck airport': 'innsbruck',
            'speyer airport': 'speyer',
            'billund airport': 'billund',
            'keetmanshoop airport': 'keetmanshoop'
        }

        
        # Apply mappings
        for full_name, base_name in city_mappings.items():
            if full_name in location.lower():
                location = base_name
        
        # Try exact match first
        normalized = location.lower()
        for stored_city, coords in airports_dict['cities_to_coords'].items():
            if normalized == stored_city.lower():
                return coords
                
        # Try prefix match (e.g., "Amsterdam" should match "Amsterdam Schiphol")
        for stored_city, coords in airports_dict['cities_to_coords'].items():
            if normalized in stored_city.lower() or stored_city.lower() in normalized:
                return coords
                
        # Try first word match
        first_word = normalized.split()[0]
        for stored_city, coords in airports_dict['cities_to_coords'].items():
            if stored_city.lower().startswith(first_word):
                return coords
        
        # Try IATA/ICAO code if present in original location
        code_match = re.search(r'\(([A-Z]{3,4})\)', original_location)
        if code_match:
            code = code_match.group(1)
            if code in airports_dict['by_iata']:
                return (airports_dict['by_iata'][code]['latitude'],
                       airports_dict['by_iata'][code]['longitude'])
            if code in airports_dict['by_icao']:
                return (airports_dict['by_icao'][code]['latitude'],
                       airports_dict['by_icao'][code]['longitude'])
        
        # Last resort: try first three letters match
        if len(normalized) >= 3:
            prefix = normalized[:3]
            for stored_city in airports_dict['cities_to_coords']:
                if stored_city.lower().startswith(prefix):
                    return airports_dict['cities_to_coords'][stored_city]
        
        print(f"No coordinates found for {original_location} (normalized: {location})")
        return None, None
        
    except Exception as e:
        print(f"Error finding coordinates for {location}: {str(e)}")
        return None, None

def estimate_duration(lat1, lon1, lat2, lon2):
    """Estimate flight duration based on coordinates."""
    if not all([lat1, lon1, lat2, lon2]):
        return "1h 30m"  # Default duration
        
    # Calculate distance using Haversine formula
    R = 6371  # Earth's radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = R * c
    
    # Calculate duration
    hours = distance / 500  # Assume 500 km/h average speed
    total_minutes = int((hours * 60) + 20)  # Add 20 minutes for takeoff/landing
    
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours}h {minutes:02d}m"

def calculate_arrival_time(departure_time, duration):
    """Calculate arrival time based on departure time and flight duration."""
    try:
        # Parse times
        dep_hour, dep_minute = map(int, departure_time.split(':'))
        dur_hours = int(duration.split('h')[0])
        dur_minutes = int(duration.split('h')[1].strip('m'))
        
        # Calculate arrival time
        total_minutes = dep_minute + dur_minutes
        total_hours = dep_hour + dur_hours + (total_minutes // 60)
        final_minutes = total_minutes % 60
        final_hours = total_hours % 24  # Handle day wraparound
        
        return f"{final_hours:02d}:{final_minutes:02d}"
    except:
        return "11:30"  # Default arrival time

def process_source_data(source, csv_path, airports_dict):
    """Process flight data from a specific source."""
    print(f"\nProcessing {source} data...")
    
    if not os.path.exists(csv_path):
        print(f"Warning: File not found: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV {csv_path}: {str(e)}")
        return []
    
    flights = []
    
    for _, row in df.iterrows():
        try:
            # Get the date early so we can format it
            flight_date = format_date(row.get('date', 'December 25'))
            
            if source.lower() == 'luxaviation':
                try:
                    route_parts = row['route'].split(' Airport ')
                    origin = clean_city_name(route_parts[0].strip())
                    destination = clean_city_name(route_parts[1].strip())
                    aircraft = row['aircraft']
                    
                    # Extract amenities
                    amenities = [
                        "Ground Transportation",
                        "Catering",
                        f"Max Passengers: {row['maxpax']}"
                    ]
                    if str(row['WiFi']).lower() == 'yes':
                        amenities.append("WiFi")
                    if str(row['Pets']).lower() == 'yes':
                        amenities.append("Pet Friendly")
                    elif str(row['Pets']).lower() == 'no':
                        amenities.append("No Pets")
                    if str(row['Beds']).lower() == 'yes':
                        amenities.append("Beds")

                    # Parse price
                    try:
                        price_str = str(row['price'])
                        if 'EUR' in price_str:
                            base_price = float(price_str.split('EUR')[1].strip().replace(',', ''))
                        else:
                            price_str = ''.join(c for c in price_str if c.isdigit() or c == '.')
                            if price_str:
                                base_price = float(price_str)
                            else:
                                base_price = 4000
                    except:
                        print(f"Error parsing price {row['price']}, using default")
                        base_price = 4000

                except Exception as e:
                    print(f"Error processing luxaviation flight: {str(e)}")
                    continue

            elif source.lower() == 'catchajet':
                origin = clean_city_name(row['departure'])
                destination = clean_city_name(row['arrival'])
                aircraft = "Citation CJ2"
                amenities = [
                    "Ground Transportation",
                    "Catering",
                    f"Max Passengers: {row['maxpax']}"
                ]

                try:
                    price_text = row['price'].replace('Book the entire jet for €', '').strip()
                    base_price = float(price_text.replace(',', ''))
                    if base_price < 500:  # If suspiciously low
                        base_price = 4000
                except:
                    base_price = 4000

            elif source.lower() == 'mirai':
                route_parts = row['route'].split(' — ')
                origin = clean_city_name(route_parts[0].strip())
                destination = clean_city_name(route_parts[1].strip())
                aircraft = "Cessna Citation CJ2"
                amenities = [
                    "Ground Transportation",
                    "Catering",
                    f"Max Passengers: {row['maxpax'].split(' ')[-1]}"
                ]

                price_str = ''.join(c for c in row['price'].split(' ')[2] if c.isdigit() or c == '.')
                base_price = float(price_str)

            elif source.lower() == 'sovereign':
                try:
                    parts = [clean_city_name(p.strip()) for p in row['flightinfo'].split('\t') if p.strip()]
                    origin = 'London'  # Default
                    destination = parts[2] if len(parts) > 2 else parts[1]
                    aircraft = parts[-1] if len(parts) > 3 else "Citation Jet"
                    amenities = [
                        "Ground Transportation",
                        "Catering",
                        "Max Passengers: 6"
                    ]

                    try:
                        price_part = next((p for p in parts if '£' in p), '£4000')
                        base_price = float(price_part.replace('£','').replace(',','')) * 1.15  # Convert to EUR
                    except:
                        base_price = 4000

                except Exception as e:
                    print(f"Error processing sovereign flight: {str(e)}")
                    continue

            # Get coordinates
            origin_lat, origin_lon = get_airport_coordinates(origin, airports_dict)
            dest_lat, dest_lon = get_airport_coordinates(destination, airports_dict)
            
            # Calculate prices
            charter_price, flyprivate_price = calculate_prices(base_price)
            
            # Calculate times
            duration = estimate_duration(origin_lat, origin_lon, dest_lat, dest_lon)
            departure_time = "10:00"  # Default departure time
            arrival_time = calculate_arrival_time(departure_time, duration)
            
            # Create flight entry
            flight = {
                'id': generate_id(),
                'thumbnail': row.get('thumbnail', "/api/placeholder/400/320"),
                'origin': origin,
                'destination': destination,
                'originLat': origin_lat,
                'originLon': origin_lon,
                'destLat': dest_lat,
                'destLon': dest_lon,
                'charterPrice': charter_price,
                'flyPrivatePrice': flyprivate_price,
                'date': flight_date,
                'duration': duration,
                'departureTime': departure_time,
                'arrivalTime': arrival_time,
                'aircraft': aircraft,
                'amenities': amenities,
                'operatedBy': source
            }
            
            flights.append(flight)
            print(f"Added {source} flight: {origin} -> {destination}")
            
        except Exception as e:
            print(f"Error processing {source} flight:")
            print(traceback.format_exc())
            continue
    
    print(f"Successfully processed {len(flights)} flights from {source}")
    return flights

def process_flights_data(sources_and_files, airports_csv_path):
    """Main processing function."""
    print(f"\nLoading airports data from {airports_csv_path}")
    airports_dict = load_airports_data(airports_csv_path)
    print(f"Loaded {len(airports_dict['by_city'])} cities and {len(airports_dict['by_iata'])} IATA codes")
    
    all_flights = []
    
    # Process each source
    for source, csv_path in sources_and_files:
        try:
            flights = process_source_data(source, csv_path, airports_dict)
            all_flights.extend(flights)
        except Exception as e:
            print(f"Error processing source {source}:")
            print(traceback.format_exc())
            continue
    
    # Sort flights by date
    for flight in all_flights:
        try:
            date_obj = datetime.strptime(flight['date'], '%B %d')
            # Set appropriate year based on month
            if date_obj.month == 12:
                date_obj = date_obj.replace(year=2024)
            else:
                date_obj = date_obj.replace(year=2025)
            flight['_sort_date'] = date_obj
        except Exception as e:
            print(f"Error processing date for sorting: {flight['date']}: {str(e)}")
            flight['_sort_date'] = datetime(2024, 12, 25)  # Default sort date

    sorted_flights = sorted(all_flights, key=lambda x: x['_sort_date'])
    for flight in sorted_flights:
        del flight['_sort_date']
    
    # Print processing summary
    print(f"\nProcessing Summary:")
    print(f"Total flights processed: {len(sorted_flights)}")
    
    # Show flights per operator
    operator_counts = {}
    for flight in sorted_flights:
        operator = flight['operatedBy']
        operator_counts[operator] = operator_counts.get(operator, 0) + 1
    
    print("\nFlights per operator:")
    for operator, count in operator_counts.items():
        print(f"- {operator}: {count} flights")
    
    # Check for missing coordinates
    missing_coords = sum(1 for flight in sorted_flights if None in [flight['originLat'], flight['originLon'], flight['destLat'], flight['destLon']])
    if missing_coords > 0:
        print(f"\nWarning: {missing_coords} flights have missing coordinates")
        print("Affected routes:")
        for flight in sorted_flights:
            if None in [flight['originLat'], flight['originLon'], flight['destLat'], flight['destLon']]:
                print(f"- {flight['origin']} -> {flight['destination']}")
    
    # Filter out suspiciously low-priced flights
    all_flights = [f for f in all_flights if f['flyPrivatePrice'] >= 100]
    # Calculate price statistics if we have flights
    if sorted_flights:
        price_stats = {
            'min_charter': min(flight['charterPrice'] for flight in sorted_flights),
            'max_charter': max(flight['charterPrice'] for flight in sorted_flights),
            'min_flyprivate': min(flight['flyPrivatePrice'] for flight in sorted_flights),
            'max_flyprivate': max(flight['flyPrivatePrice'] for flight in sorted_flights)
        }
        
        print("\nPrice Ranges:")
        print(f"Charter Prices: €{price_stats['min_charter']:,} to €{price_stats['max_charter']:,}")
        print(f"FlyPrivate Prices: €{price_stats['min_flyprivate']:,} to €{price_stats['max_flyprivate']:,}")
    else:
        print("\nNo flight data available for price ranges")
    
    # Show date range if we have flights
    if sorted_flights:
        dates = []
        for flight in sorted_flights:
            date_obj = datetime.strptime(flight['date'], '%B %d')
            # Set appropriate year based on month
            if date_obj.month == 12:
                date_obj = date_obj.replace(year=2024)
            else:
                date_obj = date_obj.replace(year=2025)
            dates.append(date_obj)
        
        if dates:
            earliest_date = min(dates).strftime('%B %d, %Y')
            latest_date = max(dates).strftime('%B %d, %Y')
            print(f"\nDate Range: {earliest_date} to {latest_date}")
    else:
        print("\nNo flight data available for date range")
    
    # Generate the final JavaScript output
    final_js = f"const hotDeals = {json.dumps(sorted_flights, indent=2, ensure_ascii=False)};"
    return final_js