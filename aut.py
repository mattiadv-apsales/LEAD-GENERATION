# aut.py
import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urlparse, parse_qs, unquote
import re

# ---------------- CONFIG ----------------
COUNTRY = "IT"
EXCLUDE_DOMAINS = [
    "facebook.com", "fb.com", "fb.me", "fbcdn.net",
    "instagram.com", "instagr.am", "whatsapp.com", "tinyurl.com",
    "bit.ly", "metastatus.com", "static.xx.fbcdn.net"
]

# ---------------- FUNZIONI ----------------
def normalize_url(url: str) -> str:
    """Rimuove trailing slash e query inutili per uniformare gli URL"""
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    return url

async def get_real_landing_urls(query: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        search_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={COUNTRY}&q={query.replace(' ', '%20')}"
        print(f"[DEBUG] Apertura Meta Ads Library: {search_url}")
        await page.goto(search_url)
        await page.wait_for_timeout(5000)  # aspetta caricamento

        # Scroll per caricare più ads
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)

        # Prendi tutti i link
        anchors = await page.query_selector_all("a[href]")
        raw_links = [await a.get_attribute("href") for a in anchors]

        # Filtra link di ads
        ad_links = []
        for link in raw_links:
            if link and ("l.facebook.com/l.php?u=" in link or "fb.me/" in link):
                ad_links.append(link)

        print(f"[DEBUG] Trovati {len(ad_links)} link di potenziali landing page")

        landing_pages = []
        seen_urls = set()
        for link in ad_links:
            real_url = None
            try:
                # l.facebook.com/l.php?u=<url>
                if "l.facebook.com/l.php?u=" in link:
                    parsed = parse_qs(urlparse(link).query)
                    if "u" in parsed:
                        real_url = unquote(parsed["u"][0])
                else:
                    # fb.me shortlink: apri con Playwright per seguire redirect
                    temp_page = await context.new_page()
                    response = await temp_page.goto(link)
                    real_url = response.url
                    await temp_page.close()

                # Filtra solo domini esterni a Meta e social
                if real_url:
                    norm_url = normalize_url(real_url)
                    if not any(d in norm_url for d in EXCLUDE_DOMAINS) and norm_url not in seen_urls:
                        landing_pages.append(norm_url)
                        seen_urls.add(norm_url)
                        print(f"[DEBUG] Landing page trovata: {norm_url}")

            except Exception as e:
                print(f"[DEBUG] Errore su {link}: {e}")

        await browser.close()
        return landing_pages

async def get_real_leads(query: str):
    landing_urls = await get_real_landing_urls(query)
    leads = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        for url in landing_urls:
            lead = {
                "landing_page": url,
                "email": None,
                "telefono": None,
                "copy_valutazione": None
            }
            try:
                page = await context.new_page()
                await page.goto(url, timeout=15000)
                content = await page.content()

                # Email
                emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", content)
                if emails:
                    lead["email"] = emails[0]

                # Telefono
                phones = re.findall(r"\+?\d[\d\s\-]{7,}\d", content)
                if phones:
                    lead["telefono"] = phones[0]

                # Copy valutazione semplice
                if any(word in content.lower() for word in ["gratis", "lezione", "webinar"]):
                    lead["copy_valutazione"] = "Copy interessante"
                else:
                    lead["copy_valutazione"] = "Copy scarso"

                await page.close()
            except Exception as e:
                print(f"[DEBUG] Errore scraping {url}: {e}")

            leads.append(lead)

        await browser.close()
    return leads
