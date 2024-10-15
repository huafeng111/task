import requests

def detect_file_type(url):
    # Perform initial detection based on URL extension (case insensitive)
    url_lower = url.lower()

    if url_lower.endswith('.pdf'):
        return 'pdf'
    elif url_lower.endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff')):
        return 'image'
    elif url_lower.endswith('.svg'):
        return 'svg'
    elif url_lower.endswith(('.ppt', '.pptx')):
        return 'ppt'
    elif url_lower.endswith(('.doc', '.docx')):
        return 'word'
    elif url_lower.endswith(('.zip', '.tar', '.gz', '.rar')):
        return 'archive'

    # If unable to determine from the extension, request the URL and check the Content-Type
    try:
        # Prefer using a HEAD request to improve efficiency
        response = requests.head(url, allow_redirects=True, timeout=5)

        # If the HEAD request fails, try a GET request and only fetch the headers
        if response.status_code != 200:
            response = requests.get(url, stream=True, timeout=5)

        content_type = response.headers.get('Content-Type', '').lower()

        # Detect the Content-Type main type
        if 'image' in content_type:
            return 'image'
        elif 'pdf' in content_type:
            return 'pdf'
        elif 'html' in content_type:
            return 'html'
        elif 'application/vnd.ms-powerpoint' in content_type or 'application/vnd.openxmlformats-officedocument.presentationml' in content_type:
            return 'ppt'
        elif 'svg' in content_type:
            return 'svg'
        elif 'application/msword' in content_type or 'application/vnd.openxmlformats-officedocument.wordprocessingml' in content_type:
            return 'word'
        elif 'application/zip' in content_type or 'application/x-tar' in content_type:
            return 'archive'
        elif 'json' in content_type:
            return 'json'
        else:
            return 'text'  # Default to text if no other type is matched
    except requests.RequestException as e:
        print(f"Error detecting file type for URL {url}: {e}")
        return 'unknown'