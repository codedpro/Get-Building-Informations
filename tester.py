import aiohttp
import asyncio
import ssl
import json
from urllib.parse import urlencode

# SSL Context to handle unverified SSL certificates
ssl_context = ssl.create_default_context()
ssl_context.set_ciphers('DEFAULT')
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Request Headers (Ensure they match your cURL request)
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Referer': 'https://gnaf2.post.ir/',
    'x-api-key': 'YOUR_API_KEY_HERE',  # Replace with your actual API key
    'Cookie': '_ga_64FV2QB3TN=GS1.1.1735368602.14.0.1735368605.0.0.0; _ga=GA1.1.187801861.1730812580; BIGipServerPool_CGZ_82=3003580682.20480.0000',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache'
}

async def fetch_with_new_logic(session, lat, lon):
    """
    Fetch using the new logic with proper encoding and detailed error handling.
    """
    params = {
        '$top': '20',
        '$filter': f'contains(parcel, POINT({lon} {lat}))'
    }
    url = 'https://gnaf2.post.ir/post-services/buildings?' + urlencode(params, safe='(),')

    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                return {"status": response.status, "data": data}
            else:
                return {"status": response.status, "error": await response.text()}
    except Exception as e:
        return {"status": "error", "exception": str(e)}


async def fetch_with_old_logic(session, lat, lon):
    """
    Fetch using the old logic for comparison.
    """
    filter_value = f"contains(parcel, POINT({lon} {lat}))"
    params = {
        '$top': '20',
        '$filter': filter_value
    }
    url = 'https://gnaf2.post.ir/post-services/buildings?' + urlencode(params)

    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                return {"status": response.status, "data": data}
            else:
                return {"status": response.status, "error": await response.text()}
    except Exception as e:
        return {"status": "error", "exception": str(e)}


async def compare_requests(lat, lon):
    """
    Compare results from both old and new request logic.
    """
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        new_logic_result = await fetch_with_new_logic(session, lat, lon)
        old_logic_result = await fetch_with_old_logic(session, lat, lon)

        print("\n--- Comparison Results ---")
        print("New Logic:")
        print(json.dumps(new_logic_result, indent=4, ensure_ascii=False))
        print("\nOld Logic:")
        print(json.dumps(old_logic_result, indent=4, ensure_ascii=False))

        # Determine if the results match
        if new_logic_result == old_logic_result:
            print("\n✅ The results are identical!")
        else:
            print("\n❌ The results differ!")

if __name__ == '__main__':
    # Latitude and Longitude to test
    latitude = 37.94945851
    longitude = 47.51924062

    asyncio.run(compare_requests(latitude, longitude))
