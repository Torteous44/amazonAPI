# utils.py

# Function to construct the Amazon product URL from the ASIN
def get_amazon_url(asin: str) -> str:
    """
    Construct Amazon product URL from the provided ASIN.

    Args:
        asin (str): The Amazon Standard Identification Number (ASIN) of the product.

    Returns:
        str: The constructed Amazon product URL.
    """
    return f"https://www.amazon.com/dp/{asin}"
