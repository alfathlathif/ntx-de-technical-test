import httpx
import asyncio
from bs4 import BeautifulSoup
import polars as pl
from tqdm.asyncio import tqdm_asyncio
import json
import os

# Set max_pages 
max_pages = [20, 20, 20, 20, 20]

# Create datasets directory if not exists
os.makedirs("datasets", exist_ok=True)

# Semaphore to limit the number of concurrent requests
semaphore = asyncio.Semaphore(10)

# Function to scrape one page with retries and better error handling
async def scrape_page(level, page):
    url = f"https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}"
    retries = 3  # Retry up to 3 times in case of failure
    for attempt in range(retries):
        try:
            async with semaphore, httpx.AsyncClient() as client:
                await asyncio.sleep(1)  # Add delay to reduce server load
                response = await client.get(url, timeout=30)
                response.raise_for_status()
                # print(f"Successfully scraped level {level} page {page} on attempt {attempt+1}")
                return response.text
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            print(f"Error scraping level {level} page {page} on attempt {attempt+1}: {exc}")
            if attempt < retries - 1:
                await asyncio.sleep(2)  # Wait before retrying
            else:
                return None

# Function to parse HTML and extract data
def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('div', class_='row')
    data = []
    
    # print(f"Found {len(rows)} rows")
    
    for row in rows:
        # Extract the link from the onclick attribute
        onclick_value = row.get('onclick')
        if onclick_value:
            link = onclick_value.split("'")[1]
            full_link = f"https://www.fortiguard.com{link}"

            # Extract the title from the <div> with the style 'word-break:break-all'
            title_tag = row.find('div', style='word-break:break-all')
            if title_tag:
                # print(f"Found title div: {title_tag}")
                # Inside this div, the title is inside a <b> tag
                title = title_tag.find('b').text.strip()
                # print(f"Found title: {title}")

                # Append the data
                data.append({"title": title, "link": full_link})

    return data


# Main function to scrape a level
async def scrape_level(level, max_page):
    all_data = []
    skipped_pages = []

    for page in tqdm_asyncio(range(1, max_page + 1)):
        html = await scrape_page(level, page)
        if html:
            data = parse_html(html)
            all_data.extend(data)
        else:
            print(f"Page {page} at level {level} was skipped")  
            skipped_pages.append(page)

    # Save the data to a CSV file
    if all_data:
        df = pl.DataFrame(all_data)
        df.write_csv(f"datasets/forti_lists_{level}.csv")
    else:
        print(f"No data collected for level {level}")

    # Return skipped pages
    return skipped_pages

# Function to save skipped pages to JSON
def save_skipped_pages(skipped):
    skipped_path = "datasets/skipped.json"
    if os.path.exists(skipped_path):
        with open(skipped_path, 'r') as f:
            skipped_data = json.load(f)
    else:
        skipped_data = {}

    skipped_data.update(skipped)

    print(f"Saving skipped pages: {skipped_data}")

    with open(skipped_path, 'w') as f:
        json.dump(skipped_data, f, indent=4)

# Main async function to scrape all levels
async def main():
    skipped = {}

    tasks = []
    for level, max_page in enumerate(max_pages, start=1):
        tasks.append(scrape_level(level, max_page))

    results = await asyncio.gather(*tasks)

    # Save skipped pages for each level
    for level, skipped_pages in enumerate(results, start=1):
        if skipped_pages:
            skipped[level] = skipped_pages
            print(f"Skipped pages for level {level}: {skipped_pages}") 

    if skipped:
        save_skipped_pages(skipped)

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
