# utils.py

# Function to construct the Amazon product URL from the ASIN
def get_amazon_url(asin: str) -> str:
    """Generate Amazon product URL from ASIN"""
    return f"https://www.amazon.com/dp/{asin}"
