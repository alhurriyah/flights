"""
FlyPrivate Flight Data Processor
===============================
Processes flight data from multiple sources and standardizes it for the FlyPrivate platform.
Performance-optimized version with caching and memory improvements.
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
from functools import lru_cache
from typing import Dict, Tuple, Optional, Set

# Cache for normalized strings and coordinates
NORMALIZED_STRINGS_CACHE: Dict[str, str] = {}
COORDINATES_CACHE: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
CITY_NAMES_SET: Set[str] = set()

@lru_cache(maxsize=1024)
def decode_unicode(s: str) -> str:
    """Decode unicode escapes and normalize display with caching."""
    try:
        return bytes(s, 'utf-8').decode('unicode-escape')
    except:
        return s

@lru_cache(maxsize=1024)
def normalize_string(s: str) -> str:
    """Normalize string with caching."""
    if not isinstance(s, str):
        return ''
        
    if s in NORMALIZED_STRINGS_CACHE:
        return NORMALIZED_STRINGS_CACHE[s]
        
    try:
        # Decode unicode
        s = decode_unicode(s)
        
        # Normalize accents
        normalized = ''.join(c for c in unicodedata.normalize('NFD', s)
                           if unicodedata.category(c) != 'Mn').lower()
                           
        # Cache and return
        NORMALIZED_STRINGS_CACHE[s] = normalized
        return normalized
    except:
        return s.lower()

@lru_cache(maxsize=1024)
def clean_city_name(name: str) -> str:
    """Clean city name with caching."""
    if not isinstance(name, str):
        return ''
        
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
        'Nice-Côte': 'Nice-Cote',
        'Nice-CÃ´te': 'Nice-Cote',
        'Orléans': 'Orleans',
        'OrlÃ©ans': 'Orleans',
        'Hyères': 'Hyeres',
        'HyÃ¨res': 'Hyeres',
        'Mérignac': 'Merignac',
        'MÃ©rignac': 'Merignac',
        'Paris-Le Bourget': 'Paris',
        'Paris Le Bourget': 'Paris',
        'Paris-Orly': 'Paris',
        'Paris Charles de Gaulle': 'Paris',
        'London Heathrow': 'London',
        'London Gatwick': 'London',
        'London Luton': 'London',
        'London Stansted': 'London',
        'London City': 'London'
    }
    
    try:
        # Decode unicode
        name = decode_unicode(name)
        
        # Apply replacements
        for old, new in common_replacements.items():
            name = name.replace(old, new)
            
        # Remove airport codes and clean up
        name = re.sub(r'\([^)]+\)', '', name)
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'international|airport|airfield|aerodrome', '', name, flags=re.IGNORECASE)
        
        return name.strip()
    except:
        return name.strip()

@lru_cache(maxsize=512)
def format_date(date_str: str) -> str:
    """Format date string with caching."""
    try:
        for fmt in ['%Y-%m-%d', '%B %d', '%d %b', '%d/%m/%Y', '%d/%m']:
            try:
                date_obj = datetime.strptime(str(date_str).strip(), fmt)
                month = date_obj.month
                if month == 12:
                    date_obj = date_obj.replace(year=2024)
                else:
                    date_obj = date_obj.replace(year=2025)
                return date_obj.strftime('%B %d')
            except ValueError:
                continue
        return 'December 25'
    except Exception as e:
        print(f"Error formatting date {date_str}: {str(e)}")
        return 'December 25'

def load_airports_data(airports_csv_path: str) -> Dict:
    """Load and process airports data with optimized storage."""
    if not os.path.exists(airports_csv_path):
        raise FileNotFoundError(f"Airport data file not found: {airports_csv_path}")
        
    airports_df = pd.read_csv(airports_csv_path)
    airports_dict = {
        'by_normalized_city': {},
        'by_iata': {},
        'by_icao': {}
    }
    
    # Pre-process city names
    global CITY_NAMES_SET
    CITY_NAMES_SET = set()
    
    for _, row in airports_df.iterrows():
        if pd.isna(row['latitude']) or pd.isna(row['longitude']):
            continue
            
        city = row['region_name'].strip() if not pd.isna(row['region_name']) else None
        iata = row['iata'].strip() if not pd.isna(row['iata']) else None
        icao = row['icao'].strip() if not pd.isna(row['icao']) else None
        
        if city:
            clean_city = clean_city_name(city)
            normalized_city = normalize_string(clean_city)
            CITY_NAMES_SET.add(normalized_city)
            
            coords = (float(row['latitude']), float(row['longitude']))
            
            if normalized_city not in airports_dict['by_normalized_city']:
                airports_dict['by_normalized_city'][normalized_city] = coords
                
            # Cache the coordinates
            COORDINATES_CACHE[normalized_city] = coords
            if iata:
                COORDINATES_CACHE[iata] = coords
            if icao:
                COORDINATES_CACHE[icao] = coords
    
    return airports_dict

# Remove the @lru_cache decorator from get_airport_coordinates
def get_airport_coordinates(location: str, airports_dict: Dict) -> Tuple[Optional[float], Optional[float]]:
    """Get airport coordinates."""
    try:
        if not location:
            return None, None
            
        # Check cache first
        cache_key = normalize_string(location)
        if cache_key in COORDINATES_CACHE:
            return COORDINATES_CACHE[cache_key]
        
        # Clean and normalize
        clean_loc = clean_city_name(location)
        normalized = normalize_string(clean_loc)
        
        # Try direct lookup
        if normalized in airports_dict['by_normalized_city']:
            coords = airports_dict['by_normalized_city'][normalized]
            COORDINATES_CACHE[cache_key] = coords
            return coords
            
        # Try substring matches with pre-processed city names
        for city in CITY_NAMES_SET:
            if normalized in city or city in normalized:
                coords = airports_dict['by_normalized_city'][city]
                COORDINATES_CACHE[cache_key] = coords
                return coords
        
        print(f"No coordinates found for {location} (normalized: {normalized})")
        return None, None
        
    except Exception as e:
        print(f"Error finding coordinates for {location}: {str(e)}")
        return None, None

# [Previous code remains the same, continuing with remaining functions...]

@lru_cache(maxsize=512)
def generate_id() -> int:
    """Generate a random 12-digit numeric ID with caching."""
    return int(''.join([str(random.randint(0, 9)) for _ in range(12)]))

@lru_cache(maxsize=1024)
def calculate_prices(base_price: float) -> Tuple[int, int]:
    """Calculate charter and FlyPrivate prices with caching."""
    multiplier = random.choice([2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0])
    charter_price = math.ceil(base_price * multiplier)
    flyprivate_price = math.ceil(base_price * 1.2)
    return charter_price, flyprivate_price

@lru_cache(maxsize=1024)
def estimate_duration(lat1: Optional[float], lon1: Optional[float], 
                     lat2: Optional[float], lon2: Optional[float]) -> str:
    """Estimate flight duration with caching."""
    if not all([lat1, lon1, lat2, lon2]):
        return "1h 30m"
        
    R = 6371  # Earth's radius in km
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    distance = R * c
    
    hours = distance / 500  # Assume 500 km/h average speed
    total_minutes = int((hours * 60) + 20)  # Add 20 minutes for takeoff/landing
    
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours}h {minutes:02d}m"

@lru_cache(maxsize=1024)
def calculate_arrival_time(departure_time: str, duration: str) -> str:
    """Calculate arrival time with caching."""
    try:
        dep_hour, dep_minute = map(int, departure_time.split(':'))
        dur_hours = int(duration.split('h')[0])
        dur_minutes = int(duration.split('h')[1].strip('m'))
        
        total_minutes = dep_minute + dur_minutes
        total_hours = dep_hour + dur_hours + (total_minutes // 60)
        final_minutes = total_minutes % 60
        final_hours = total_hours % 24
        
        return f"{final_hours:02d}:{final_minutes:02d}"
    except:
        return "11:30"

def process_source_data(source: str, csv_path: str, airports_dict: Dict) -> list:
    """Process flight data from a specific source."""
    print(f"\nProcessing {source} data...")
    
    if not os.path.exists(csv_path):
        print(f"Warning: File not found: {csv_path}")
        return []
    
    try:
        # Read CSV in chunks to reduce memory usage
        chunks = pd.read_csv(csv_path, chunksize=100)
        flights = []
        
        for chunk in chunks:
            for _, row in chunk.iterrows():
                try:
                    flight_date = format_date(row.get('date', 'December 25'))
                    
                    if source.lower() == 'luxaviation':
                        try:
                            route_parts = row['route'].split(' Airport ')
                            origin = clean_city_name(route_parts[0].strip())
                            destination = clean_city_name(route_parts[1].strip())
                            aircraft = row['aircraft']
                            
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

                            try:
                                price_str = str(row['price'])
                                if 'EUR' in price_str:
                                    base_price = float(price_str.split('EUR')[1].strip().replace(',', ''))
                                else:
                                    price_str = ''.join(c for c in price_str if c.isdigit() or c == '.')
                                    base_price = float(price_str) if price_str else 4000
                            except:
                                base_price = 4000

                        except Exception as e:
                            print(f"Error processing luxaviation flight: {str(e)}")
                            continue

                    elif source.lower() == 'catchajet':
                        try:
                            origin = clean_city_name(row['departure'])
                            destination = clean_city_name(row['arrival'])
                            aircraft = "Citation CJ2"
                            max_pax = str(row['maxpax']).split()[0]  # Extract just the number from "4 Seats"
                            amenities = [
                                "Ground Transportation",
                                "Catering",
                                f"Max Passengers: {max_pax}"
                            ]

                            try:
                                price_text = str(row['price'])
                                # Extract just the number from "Book the entire jet for €990"
                                price_match = re.search(r'€(\d+(?:,\d+)?)', price_text)
                                if price_match:
                                    base_price = float(price_match.group(1).replace(',', ''))
                                else:
                                    base_price = 4000
                            except:
                                print(f"Error parsing price from: {row['price']}, using default")
                                base_price = 4000

                        except Exception as e:
                            print(f"Error processing catchajet flight: {str(e)}")
                            continue

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
                            origin = 'London'
                            destination = parts[2] if len(parts) > 2 else parts[1]
                            aircraft = parts[-1] if len(parts) > 3 else "Citation Jet"
                            amenities = [
                                "Ground Transportation",
                                "Catering",
                                "Max Passengers: 6"
                            ]

                            try:
                                price_part = next((p for p in parts if '£' in p), '£4000')
                                base_price = float(price_part.replace('£','').replace(',','')) * 1.15
                            except:
                                base_price = 4000

                        except Exception as e:
                            print(f"Error processing sovereign flight: {str(e)}")
                            continue

                    # Get coordinates from cache or calculate
                    origin_lat, origin_lon = get_airport_coordinates(origin, airports_dict)
                    dest_lat, dest_lon = get_airport_coordinates(destination, airports_dict)
                    
                    # Calculate prices, duration, and times
                    if base_price >= 100:  # Filter out suspiciously low prices
                        charter_price, flyprivate_price = calculate_prices(base_price)
                        duration = estimate_duration(origin_lat, origin_lon, dest_lat, dest_lon)
                        departure_time = "10:00"
                        arrival_time = calculate_arrival_time(departure_time, duration)
                        
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
        
    except Exception as e:
        print(f"Error processing source {source}: {str(e)}")
        return []

def process_flights_data(sources_and_files: list, airports_csv_path: str) -> str:
    """Main processing function with optimized memory usage."""
    print(f"\nLoading airports data from {airports_csv_path}")
    airports_dict = load_airports_data(airports_csv_path)
    print(f"Loaded {len(airports_dict['by_normalized_city'])} cities")
    
    all_flights = []
    processed_cities = set()  # Track processed cities to avoid duplicates
    
    for source, csv_path in sources_and_files:
        try:
            flights = process_source_data(source, csv_path, airports_dict)
            # Only add flights with coordinates and valid prices
            valid_flights = [f for f in flights if None not in 
                           [f['originLat'], f['originLon'], f['destLat'], f['destLon']] and 
                           f['flyPrivatePrice'] >= 100]
            all_flights.extend(valid_flights)
            
            # Track processed cities
            for flight in valid_flights:
                processed_cities.add(flight['origin'])
                processed_cities.add(flight['destination'])
                
        except Exception as e:
            print(f"Error processing source {source}:")
            print(traceback.format_exc())
            continue
    
    # Sort flights by date
    for flight in all_flights:
        try:
            date_obj = datetime.strptime(flight['date'], '%B %d')
            if date_obj.month == 12:
                date_obj = date_obj.replace(year=2024)
            else:
                date_obj = date_obj.replace(year=2025)
            flight['_sort_date'] = date_obj
        except Exception as e:
            print(f"Error processing date for sorting: {flight['date']}: {str(e)}")
            flight['_sort_date'] = datetime(2024, 12, 25)

    sorted_flights = sorted(all_flights, key=lambda x: x['_sort_date'])
    for flight in sorted_flights:
        del flight['_sort_date']
    
    # Print processing summary
    print(f"\nProcessing Summary:")
    print(f"Total flights processed: {len(sorted_flights)}")
    print(f"Total cities processed: {len(processed_cities)}")
    
    # Show flights per operator
    operator_counts = {}
    for flight in sorted_flights:
        operator = flight['operatedBy']
        operator_counts[operator] = operator_counts.get(operator, 0) + 1
    
    print("\nFlights per operator:")
    for operator, count in operator_counts.items():
        print(f"- {operator}: {count} flights")
    
    print(f"Debug: Number of flights being written: {len(sorted_flights)}")
    final_js = f"const hotDeals = {json.dumps(sorted_flights, indent=2, ensure_ascii=False)};"
    print(f"Debug: Size of final_js: {len(final_js)} bytes")
    
    if not final_js or final_js == "const hotDeals = [];":
        print("Warning: No flights data in output")
    
    return final_js