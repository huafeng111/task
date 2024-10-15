import requests

def detect_file_type(url):
    # ���� URL ��չ�����г������
    if url.endswith('.pdf'):
        return 'pdf'
    elif url.endswith('.png') or url.endswith('.jpg') or url.endswith('.jpeg'):
        return 'image'
    elif url.endswith('.svg'):
        return 'svg'
    elif url.endswith('.ppt') or url.endswith('.pptx'):
        return 'ppt'

    # ����޷�������չ���жϣ�������� URL ���鿴 Content-Type
    try:
        response = requests.head(url, allow_redirects=True)
        content_type = response.headers.get('Content-Type', '')

        if 'image' in content_type:
            return 'image'
        elif 'pdf' in content_type:
            return 'pdf'
        elif 'html' in content_type:
            return 'html'
        elif 'application/vnd.ms-powerpoint' in content_type:
            return 'ppt'
        elif 'svg' in content_type:
            return 'svg'
        else:
            return 'text'  # Ĭ���������Ϊ�ı�
    except requests.RequestException as e:
        print(f"Error detecting file type for URL {url}: {e}")
        return 'unknown'
