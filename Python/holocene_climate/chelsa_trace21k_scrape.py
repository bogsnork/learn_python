import requests
from bs4 import BeautifulSoup

def get_https_urls_from_envicloud(variable="pr", limit=None):
    """
    Scrape the EnviCloud directory listing for CHELSA TraCE21k GeoTIFFs for the given variable.
    Returns a list of HTTPS URLs.
    """
    base_url = f"https://os.zhdk.cloud.switch.ch/chelsav1/chelsa_TRACE21k/V1.0/{variable}/"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(base_url, headers=headers)
    print("Status code:", response.status_code)
    print("Final URL:", response.url)
    print("First 500 chars of response:")
    print(response.text[:500])
    soup = BeautifulSoup(response.text, "xml")  # Use XML parser
    urls = []
    for key in soup.find_all("Key"):
        href = key.text
        if href.endswith(".tif"):
            urls.append("https://os.zhdk.cloud.switch.ch/chelsav1/" + href)
            if limit and len(urls) >= limit:
                break
    return urls

if __name__ == "__main__":
    VARIABLE = "pr"  # Change to other variables as needed
    LIMIT = 10       # Set to None to get all files

    urls = get_https_urls_from_envicloud(variable=VARIABLE, limit=LIMIT)
    print(f"Found {len(urls)} GeoTIFF URLs for variable '{VARIABLE}':")
    for url in urls:
        print(url)