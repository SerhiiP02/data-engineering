import requests
import os
import zipfile
import aiohttp
import asyncio
from aiohttp import ClientSession

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


async def download_and_extract(session, url):
    async with session.get(url) as response:
        filename = os.path.basename(url)
        with open(f"downloads/{filename}", "wb") as f:
            f.write(await response.read())
        
        with zipfile.ZipFile(f"downloads/{filename}", 'r') as zip_ref:
            zip_ref.extractall("downloads")
        
        os.remove(f"downloads/{filename}")
        print(f"Downloaded and extracted: {filename}")

async def main():
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    async with ClientSession() as session:
        tasks = [download_and_extract(session, url) for url in download_uris]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())