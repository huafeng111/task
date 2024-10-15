import requests

def detect_file_type(url):
    # 根据 URL 扩展名进行初步检测（忽略大小写）
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

    # 如果无法根据扩展名判断，则请求该 URL 并查看 Content-Type
    try:
        # 优先使用 HEAD 请求以提高效率
        response = requests.head(url, allow_redirects=True, timeout=5)

        # 如果 HEAD 请求失败，尝试 GET 请求，只获取头部
        if response.status_code != 200:
            response = requests.get(url, stream=True, timeout=5)

        content_type = response.headers.get('Content-Type', '').lower()

        # 检测 Content-Type 主类型
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
            return 'text'  # 默认情况下视为文本
    except requests.RequestException as e:
        print(f"Error detecting file type for URL {url}: {e}")
        return 'unknown'