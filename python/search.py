import json
import boto3
from collections import defaultdict
from itertools import permutations

# ===============================
# AWS Boto3 S3 Client Initialization
# Initializing the client outside the handler allows for connection reuse, 
# improving performance for "warm" Lambda invocations.
# ===============================
s3_client = boto3.client('s3')

# ===================================================================
#
# ORIGINAL HELPER AND CORE LOGIC FUNCTIONS GO HERE
#
# ===================================================================

def endToEnd(carLength, numCars):
    """Return width and length if cars are parked end-to-end."""
    width = carLength * numCars
    length = numCars * 10
    return width, length

def sideBySide(carLength, numCars):
    """Return width and length if cars are parked side-by-side."""
    width = 10 * numCars
    length = carLength
    return width, length

def find_cheapest_spot_optimized(numCars, carLength, available_listings):
    """
    Finds the cheapest single listing by leveraging a pre-sorted list.
    Returns the first compatible listing it finds.
    """
    e2ewidth, e2elength = endToEnd(carLength, numCars)
    sbswidth, sbslength = sideBySide(carLength, numCars)
    
    for listing in available_listings:
        if (listing["length"] >= e2elength and listing["width"] >= e2ewidth) or \
           (listing["length"] >= sbslength and listing["width"] >= sbswidth):
            return listing
            
    return None

def find_partitions(total_cars, num_sets, start=1):
    """
    Generate all integer partitions of total_cars into num_sets parts.
    """
    if num_sets == 1:
        if total_cars >= start:
            yield (total_cars,)
        return
    for i in range(start, total_cars // num_sets + 1):
        for partition in find_partitions(total_cars - i, num_sets - 1, start=i):
            yield (i,) + partition

def find_cheapest_for_location(numCars, carLength, listings_at_location, price_to_beat):
    """
    Find the cheapest parking combination for a single type of vehicle at a single location.
    """
    cheapest_price_for_this_loc = price_to_beat
    best_arrangement_for_this_loc = None

    for num_groups in range(1, numCars + 1):
        for partition in find_partitions(numCars, num_groups):
            for p_ordering in set(permutations(partition)):
                
                temp_listings = listings_at_location.copy()
                used_listings = []
                current_price = 0
                is_possible = True
                
                for group_size in p_ordering:
                    if current_price >= cheapest_price_for_this_loc:
                        is_possible = False
                        break

                    spot = find_cheapest_spot_optimized(group_size, carLength, temp_listings)
                    
                    if spot:
                        current_price += spot['price_in_cents']
                        used_listings.append(spot)
                        temp_listings.remove(spot)
                    else:
                        is_possible = False
                        break
                
                if is_possible and current_price < cheapest_price_for_this_loc:
                    cheapest_price_for_this_loc = current_price
                    best_arrangement_for_this_loc = {
                        "split_of_cars": p_ordering,
                        "listings_used": used_listings,
                        "total_price_in_cents": current_price
                    }
    
    return best_arrangement_for_this_loc

def group_listings_by_location(listings):
    """Group listings by location and sort each group by price."""
    locations = defaultdict(list)
    for listing in listings:
        locations[listing['location_id']].append(listing)
    for loc_id in locations:
        locations[loc_id].sort(key=lambda x: x['price_in_cents'])
    return locations

def find_best_solution_with_grouping(vehicle_request, all_listings):
    """
    Finds the cheapest location by solving the complex grouping problem for each
    type of vehicle required.
    """
    grouped_locations = group_listings_by_location(all_listings)
    overall_results = []
    
    for location_id, listings in grouped_locations.items():
        
        available_listings_at_loc = listings.copy()
        used_listings_for_loc = []
        current_total_price = 0
        location_is_possible = True

        sorted_request = sorted(vehicle_request, key=lambda v: v['quantity'] * v['length'], reverse=True)

        for vehicle_group in sorted_request:
            num_cars = vehicle_group["quantity"]
            car_length = vehicle_group["length"]

            arrangement = find_cheapest_for_location(
                numCars=num_cars,
                carLength=car_length,
                listings_at_location=available_listings_at_loc,
                price_to_beat=float('inf')
            )

            if arrangement:
                current_total_price += arrangement['total_price_in_cents']
                used_listings_for_loc.extend(arrangement['listings_used'])
                for spot in arrangement['listings_used']:
                    if spot in available_listings_at_loc:
                        available_listings_at_loc.remove(spot)
            else:
                location_is_possible = False
                break
        
        if location_is_possible:
            overall_results.append({
                "location_id": location_id,
                "total_price_in_cents": current_total_price,
                "listing_ids": [l["id"] for l in used_listings_for_loc]
            })

    overall_results.sort(key=lambda x: x["total_price_in_cents"])
    return overall_results
# ===============================
# AWS Lambda Handler
# ===============================

def lambda_handler(event, context):
    """
    Main handler for the Lambda function. It processes an API Gateway POST request,
    fetches listings from S3, and returns the cheapest parking solutions.
    """
    try:
        # 1. Get vehicle request from the API Gateway event body
        # The body is now a JSON string representing a list directly.
        try:
            vehicle_request = json.loads(event.get('body', '[]'))
            # Validate that the parsed body is a list
            if not isinstance(vehicle_request, list):
                raise ValueError("Request body must be a JSON array of vehicle objects.")
        except (json.JSONDecodeError, ValueError) as e:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': f"Invalid request format: {str(e)}"})
            }

        # 2. Fetch and load the listings.json from S3
        bucket_name = 'neighbor-assessment-listing'
        file_key = 'listings.json'
        
        try:
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            listings_content = s3_object['Body'].read().decode('utf-8')
            all_listings = json.loads(listings_content)
        except s3_client.exceptions.NoSuchKey:
             return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': f"File '{file_key}' not found in bucket '{bucket_name}'."})
            }

        # 3. Run the core logic from your original script
        results = find_best_solution_with_grouping(vehicle_request, all_listings)

        # 4. Format and return the successful response for API Gateway
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' # Optional: Allows cross-origin requests
            },
            'body': json.dumps(results)
        }

    except Exception as e:
        # A general catch-all for any other unexpected errors
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'An internal server error occurred.'})
        }