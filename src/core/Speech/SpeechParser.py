class SpeechParser:
    """
    This class is responsible for parsing speech pages and extracting relevant information
    such as title, author, date, and PDF links.
    """
    def __init__(self):
        pass

    def parse_speech_links(self, soup):
        """
        从页面的 soup 对象中提取演讲页面链接.
        """
        return [
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm")
        ]

    def parse_speech_page(self, page_url, soup):
        """
        解析单个演讲页面，提取标题、作者、日期和 PDF 链接.
        """
        # 提取页面的文件名（用于匹配 .pdf 文件）
        page_filename = os.path.basename(page_url).split(".htm")[0]

        title_tag = soup.find('h3', class_='title')
        title = title_tag.find('em').get_text(strip=True) if title_tag and title_tag.find('em') else "No Title"

        author_tag = soup.find('p', class_='speaker')
        author = author_tag.get_text(strip=True) if author_tag else "Unknown Author"

        date_tag = soup.find('p', class_='article__time')
        date = date_tag.get_text(strip=True) if date_tag else "Unknown Date"

        pdf_links = [
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].endswith(".pdf") and page_filename in link['href']
        ]

        return {
            'title': title,
            'author': author,
            'date': date,
            'pdf_links': pdf_links
        }
