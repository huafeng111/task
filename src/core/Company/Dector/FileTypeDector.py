import requests

def detect_file_type(url):
    # ���� URL ��չ�����г�����⣨���Դ�Сд��
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

    # ����޷�������չ���жϣ�������� URL ���鿴 Content-Type
    try:
        # ����ʹ�� HEAD ���������Ч��
        response = requests.head(url, allow_redirects=True, timeout=5)

        # ��� HEAD ����ʧ�ܣ����� GET ����ֻ��ȡͷ��
        if response.status_code != 200:
            response = requests.get(url, stream=True, timeout=5)

        content_type = response.headers.get('Content-Type', '').lower()

        # ��� Content-Type ������
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
            return 'text'  # Ĭ���������Ϊ�ı�
    except requests.RequestException as e:
        print(f"Error detecting file type for URL {url}: {e}")
        return 'unknown'