from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from app.scraper import AsyncProductScraper

# Initialize the FastAPI app
app = FastAPI()

# Define a request model to accept ASIN as input
class ASINRequest(BaseModel):
    asin: str

# Define the POST endpoint for scraping product data
@app.post("/scrape")
async def scrape(asin_request: ASINRequest):
    """Scrape product data from Amazon using the provided ASIN."""
    try:
        scraper = AsyncProductScraper()
        product_data = await scraper.scrape_products([asin_request.asin])
        return product_data[0]  # Return first result since we're only sending one ASIN
    except Exception as e:
        # Return an error if scraping fails
        raise HTTPException(status_code=500, detail=f"Error scraping product data: {str(e)}")

