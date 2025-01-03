from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.scraper import scrape_product_data

# Initialize the FastAPI app
app = FastAPI()

# Define a request model to accept ASIN as input
class ASINRequest(BaseModel):
    asin: str

# Define the POST endpoint for scraping product data
@app.post("/scrape")
async def scrape(asin_request: ASINRequest):
    """Scrape product data from Amazon using the provided ASIN."""
    asin = asin_request.asin  # Get the ASIN from the request

    # Call the scrape function from scraper.py
    try:
        product_data = await scrape_product_data(asin)
        return product_data
    except Exception as e:
        # Return an error if scraping fails
        raise HTTPException(status_code=500, detail=f"Error scraping product data: {str(e)}")

