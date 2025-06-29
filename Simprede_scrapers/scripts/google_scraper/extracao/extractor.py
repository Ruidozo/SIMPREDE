"""
Extrator de conteúdo de artigos do Google News e outras fontes
Otimizado para ambiente Docker e produção
"""

# File: airflow-GOOGLE-NEWS-SCRAPER/scripts/google_scraper/extracao/extractor.py
# Script para extrair conteúdo de artigos do Google News e outras fontes
# Este script é executado como parte do DAG do Airflow e deve ser compatível com o ambiente do Airflow

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from newspaper import Article, Config
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import csv
import logging
import os
import time

# Setup logging
logger = logging.getLogger(__name__)

def get_chrome_options():
    """Configure Chrome options for Docker environment"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--window-size=1920,1080")
    
    # Set the binary location to chromium
    options.binary_location = "/usr/bin/chromium"
    
    return options

def get_webdriver():
    """
    Cria e retorna instância configurada do WebDriver
    Otimizada para ambiente Docker com timeouts adequados
    """
    options = get_chrome_options()
    service = Service("/usr/bin/chromedriver")
    
    try:
        driver = webdriver.Chrome(service=service, options=options)
        # Define timeouts padrão para evitar travamentos
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(10)
        return driver
    except Exception as e:
        logger.error(f"Falha ao criar WebDriver: {e}")
        raise

def resolve_google_news_url(url, driver_path=None, max_wait_time=10):
    """
    Resolve URL do Google News para obter link original
    Implementa retry logic e melhores práticas de timeout
    """
    if not url or not url.startswith("http"):
        logger.warning(f"URL inválido fornecido: {url}")
        return None
    
    driver = None
    try:
        driver = get_webdriver()
        logger.info(f"🌐 A aceder ao link: {url}")
        
        # Acede ao URL com timeout
        driver.get(url)
        wait = WebDriverWait(driver, max_wait_time)

        # Trata página de consentimento se existir
        if "consent.google.com" in driver.current_url:
            logger.info("⚠️ Página de consentimento detectada, a tentar aceitar...")
            try:
                accept_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Accept all" or text()="Aceitar tudo"]]'))
                )
                accept_button.click()
                logger.info("✅ Consentimento aceite")
                
                # Aguarda redirecionamento
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Não foi possível aceitar consentimento: {e}")

        # Aguarda redirecionamento para site original
        try:
            wait.until(lambda d: not d.current_url.startswith("https://news.google.com/")
                                and not d.current_url.startswith("https://consent.google.com/"))
        except Exception:
            logger.warning("Timeout aguardando redirecionamento")

        final_url = driver.current_url
        
        # Valida URL final
        if final_url and final_url != url and not final_url.startswith("https://news.google.com/"):
            logger.info(f"✅ URL resolvido: {final_url}")
            return final_url
        else:
            logger.warning(f"URL não foi redirecionado adequadamente: {final_url}")
            return None

    except Exception as e:
        logger.error(f"❌ Erro ao resolver URL do Google News: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass  # Ignora erros ao fechar driver



def get_original_url_via_requests(google_rss_url):
    """Retrieve the original article URL using Google's internal API."""
    try:
        logger.info("🔄 Tentando obter o URL original via requests...")
        guid = urlparse(google_rss_url).path.replace('/rss/articles/', '')

        param = '["garturlreq",[["en-US","US",["FINANCE_TOP_INDICES","WEB_TEST_1_0_0"],null,null,1,1,"US:en",null,null,null,null,null,null,null,0,5],"en-US","US",true,[2,4,8],1,true,"661099999",0,0,null,0],{guid}]'
        payload = urlencode({
            'f.req': [[['Fbv4je', param.format(guid=guid), 'null', 'generic']]]
        })

        headers = {
            'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

        url = "https://news.google.com/_/DotsSplashUi/data/batchexecute"
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            array_string = json.loads(response.text.replace(")]}'", ""))[0][2]
            article_url = json.loads(array_string)[1]
            logger.info(f"✅ URL original obtido via requests: {article_url}")
            return article_url
        else:
            logger.error(f"❌ Falha ao obter URL via requests. Código de status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"❌ Erro ao usar requests para obter o URL original: {e}")
        return None

def fetch_and_extract_article_text(url: str) -> str:
    """
    Fetches the content of a webpage and extracts the main article text.
    """
    if not url.startswith("http"):
        logger.warning(f"⚠️ URL inválido: {url}")
        return ""

    if "news.google.com" in url:
        logger.warning(f"⚠️ Ignorado URL do Google News (redirecionamento): {url}")
        return ""

    try:
        logger.info(f"🌐 Fetching URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        logger.info(f"✅ Successfully fetched URL: {url}")

        soup = BeautifulSoup(response.text, "html.parser")

        # Attempt to extract the main article text
        containers = [
            soup.find('article'),
            soup.find('main'),
            soup.find('div', class_='article-body'),
            soup.find('div', class_='story'),
            soup.find('div', class_='content'),
            soup.find('body')
        ]
        for container in containers:
            if container:
                paragraphs = container.find_all('p')
                text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if text:
                    logger.info(f"🔍 Extracted text from container: {container.name}")
                    return " ".join(text).strip()

        # Fallback: Try extracting all <p> tags
        paragraphs = soup.find_all('p')
        text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        if text:
            logger.info(f"🔍 Extracted text from fallback <p> tags.")
            return " ".join(text).strip()

        # Final fallback
        visible_text = soup.stripped_strings
        combined_text = " ".join(visible_text)
        if combined_text:
            logger.info(f"🔍 Extracted text from all visible elements.")
            return combined_text.strip()

        logger.warning(f"⚠️ No article or content found for URL: {url}")
        return ""

    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ Error fetching URL {url}: {e}")
        return ""
    except Exception as e:
        logger.warning(f"⚠️ Unexpected error for URL {url}: {e}")
        return ""

def fetch_and_extract_article_text_dynamic(url: str) -> str:
    """
    Fetches the content of a webpage using Selenium and extracts the main article text.
    """
    if "news.google.com" in url:
        logger.warning(f"⚠️ Ignorado URL do Google News (redirecionamento): {url}")
        return ""

    try:
        logger.info(f"🌐 Fetching URL dynamically: {url}")
        driver = get_webdriver()

        driver.get(url)

        # Remove pop-ups and overlays
        driver.execute_script("""
            let modals = document.querySelectorAll('[class*="popup"], [class*="modal"], [id*="popup"]');
            modals.forEach(el => el.remove());
        """)

        # Wait for the main content to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article, .article-body, .content"))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        # Attempt to extract the main article text
        containers = [
            soup.find('article'),
            soup.find('main'),
            soup.find('div', class_='article-body'),
            soup.find('div', class_="story"),
            soup.find('div', class_='content'),
            soup.find('body')
        ]
        for container in containers:
            if container:
                paragraphs = container.find_all('p')
                text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
                if text:
                    logger.info(f"🔍 Extracted text from container: {container.name}")
                    return " ".join(text).strip()

        # Fallback: Try extracting all <p> tags if no container is found
        paragraphs = soup.find_all('p')
        text = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
        if text:
            logger.info(f"🔍 Extracted text from fallback <p> tags.")
            return " ".join(text).strip()

        logger.warning(f"⚠️ No article or content found for URL: {url}")
        return ""

    except Exception as e:
        logger.warning(f"⚠️ Error fetching URL dynamically {url}: {e}")
        return ""

def extract_article_text(soup):
    if soup is None:
        return ""

    containers = [
        soup.find('article'),
        soup.find('main'),
        soup.find('div', class_='article-body'),
        soup.find('div', class_='story'),
        soup.find('body')
    ]
    for container in containers:
        if container:
            paragraphs = container.find_all('p')
            texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
            if texto:
                return " ".join(texto).strip()
    # fallback
    paragraphs = soup.find_all('p')
    texto = [p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)]
    return " ".join(texto).strip()


def get_real_url_with_newspaper(link, driver_path="/usr/bin/chromedriver", max_wait_time=10):
    driver = get_webdriver()

    try:
        logger.info(f"🌐 A aceder ao link: {link}")
        driver.get(link)
        wait = WebDriverWait(driver, max_wait_time)

        # Se for página de consentimento, tenta clicar no botão "Aceitar tudo"
        if "consent.google.com" in driver.current_url:
            logger.warning("⚠️ Página de consentimento detetada. A tentar aceitar...")

            try:
                # ⚠️ Tenta mudar para o iframe se existir
                WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.XPATH, "//iframe[contains(@src, 'consent')]")))
                logger.info("✅ Mudança para iframe de consentimento feita.")

                # Espera e clica no botão "Aceitar tudo"
                aceitar_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//button//*[text()[contains(.,"Aceitar tudo") or contains(.,"Accept all")]]/..'))
                )
                aceitar_btn.click()
                logger.info("✅ Consentimento aceite!")

                # Voltar ao conteúdo principal
                driver.switch_to.default_content()

            except Exception as e:
                logger.error("❌ Não consegui aceitar o consentimento:", e)

        # Espera que o URL final mude e a página de destino carregue
        wait.until(lambda d: not d.current_url.startswith("https://consent.google.com"))
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        final_url = driver.current_url
        logger.info(f"✅ URL final resolvido: {final_url}")
        return final_url

    except Exception as e:
        logger.error(f"❌ Erro ao resolver URL com Selenium: {e}")
        return None
    finally:
        driver.quit()

def extract_article_content(url):
    """Extract article content using newspaper3k."""
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    config = Config()
    config.browser_user_agent = user_agent

    article_data = dict()

    try:
        logger.info(f"📰 Extraindo conteúdo do artigo de {url}...")
        article = Article(url, config=config)
        article.download()
        article.parse()

        article_data["article_title"] = article.title
        article_data["article_text"] = article.text
        logger.info("✅ Conteúdo do artigo extraído com sucesso!")

    except Exception as e:
        logger.error(f"❌ Erro ao extrair conteúdo do artigo de {url}: {e}")

    return article_data

def extrair_conteudo(link, timeout=10, driver_path="/usr/bin/chromedriver"):
    def fetch_content():
        # Tenta resolver via redirecionamento HTTP
        real_url = resolve_google_news_url(link) or link
        if not real_url:
            logger.warning(f"⚠️ Fallback para link original: {link}")
            real_url = link
        return fetch_and_extract_article_text(real_url)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fetch_content)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            logger.warning(f"⚠️ Timeout ao extrair {link}")
            return ""
        except Exception as e:
            logger.error(f"❌ Erro ao extrair: {e}")
            return ""


def load_freguesias_codigos(filepath):
    """
    Load freguesias and their corresponding codes from a CSV file.
    
    Args:
        filepath (str): Path to the CSV file containing freguesias and codes.
        
    Returns:
        dict: Dictionary with freguesia names as keys and their codes as values.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            if 'Freguesia' not in reader.fieldnames or 'Código' not in reader.fieldnames:
                raise ValueError("CSV file must contain 'Freguesia' and 'Código' columns.")
            return {normalize(row['Freguesia']): row['Código'] for row in reader}
    except FileNotFoundError:
        logger.warning(f"⚠️ O arquivo {filepath} não foi encontrado.")
        return {}
    except ValueError as e:
        logger.warning(f"⚠️ Erro no formato do arquivo CSV: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Erro ao carregar os códigos de freguesias: {e}")
        return {}

def get_real_url_and_content(link, driver_path="/usr/bin/chromedriver", max_wait_time=5):
    """
    Retrieves the original URL and the content of the news article.
    """
    driver = get_webdriver()
    page_data = {"source_url": None, "article_content": None}

    try:
        logger.info(f"🌐 Acessando o link: {link}")
        driver.get(link)
        wait = WebDriverWait(driver, max_wait_time)

        # Handle consent page
        current_url = driver.current_url
        if current_url.startswith("https://consent.google.com/"):
            logger.warning("⚠️ Detetado consentimento explícito. A tentar aceitar...")
            try:
                accept_all_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//button[.//span[text()="Accept all"]]'))
                )
                if accept_all_button:
                    accept_all_button.click()
                    logger.info("✅ Consentimento aceite!")
            except Exception:
                logger.error("❌ Não foi possível localizar o botão de consentimento.")

        # Wait for redirection to the source website
        logger.info("🔄 Redirecionando para o site de origem...")
        wait.until(lambda driver: not driver.current_url.startswith("https://news.google.com/")
                                and not driver.current_url.startswith("https://consent.google.com/"))

        # Wait for the article page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Get the final URL
        page_data["source_url"] = driver.current_url
        logger.info(f"✅ URL final obtida: {page_data['source_url']}")

        # Extract article content using newspaper3k
        page_data["article_content"] = extract_article_content(page_data["source_url"])

    except Exception as e:
        logger.error(f"❌ Erro ao obter URL de origem ou conteúdo para {link}: {e}")
        # Fallback to requests-based method
        page_data["source_url"] = get_original_url_via_requests(link)
        if page_data["source_url"]:
            page_data["article_content"] = extract_article_content(page_data["source_url"])

    finally:
        driver.quit()

    return page_data
