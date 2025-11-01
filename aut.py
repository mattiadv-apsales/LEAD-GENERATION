import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urlparse, parse_qs, unquote
import re
from typing import List, Dict, Set

# ---------------- CONFIG ----------------
COUNTRY = "IT"
EXCLUDE_DOMAINS = [
    "facebook.com", "fb.com", "fb.me", "fbcdn.net",
    "instagram.com", "instagr.am", "whatsapp.com", "tinyurl.com",
    "bit.ly", "metastatus.com", "static.xx.fbcdn.net"
]

# Performance tuning
MAX_CONCURRENT_PAGES = 5
SCROLL_COUNT = 10
SCROLL_WAIT = 1000
PAGE_TIMEOUT = 10000
INITIAL_WAIT = 3000

# ---------------- FUNZIONI ----------------
def normalize_url(url: str) -> str:
    """Rimuove trailing slash e query inutili per uniformare gli URL"""
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    return url

def extract_real_url(link: str) -> str | None:
    """Estrae URL reale da link Facebook (senza aprire pagina)"""
    try:
        if "l.facebook.com/l.php?u=" in link:
            parsed = parse_qs(urlparse(link).query)
            if "u" in parsed:
                return unquote(parsed["u"][0])
    except Exception as e:
        print(f"[DEBUG] Errore estrazione URL: {e}")
    return None

async def resolve_shortlink(context, link: str) -> str | None:
    """Risolve shortlink come fb.me"""
    try:
        temp_page = await context.new_page()
        response = await temp_page.goto(link, timeout=PAGE_TIMEOUT)
        real_url = response.url
        await temp_page.close()
        return real_url
    except Exception as e:
        print(f"[DEBUG] Errore su shortlink {link}: {e}")
        return None

def should_exclude_url(url: str) -> bool:
    """Verifica se URL va escluso"""
    return any(d in url for d in EXCLUDE_DOMAINS)

def validate_email(email: str) -> bool:
    """Valida se l'email è reale e non un placeholder"""
    email = email.lower()
    invalid_patterns = [
        'example.com', 'test.com', 'dummy', 'placeholder',
        'noreply', 'no-reply', 'youremail', 'your-email',
        'email@', '@email', 'info@info', 'admin@admin'
    ]
    if any(pattern in email for pattern in invalid_patterns):
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    
    return True

def validate_phone(phone: str) -> bool:
    """Valida se il telefono è reale"""
    clean = re.sub(r'[\s\-\(\)]', '', phone)
    
    if not re.match(r'^\+?\d{9,15}$', clean):
        return False
    
    if re.match(r'^(\d)\1+$', clean) or clean == '123456789' or clean == '987654321':
        return False
    
    return True

async def get_real_landing_urls(query: str) -> List[str]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        search_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={COUNTRY}&q={query.replace(' ', '%20')}"
        print(f"[DEBUG] Apertura Meta Ads Library: {search_url}")
        
        await page.goto(search_url)
        await page.wait_for_timeout(INITIAL_WAIT)

        for i in range(SCROLL_COUNT):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(SCROLL_WAIT)
            print(f"[DEBUG] Scroll {i+1}/{SCROLL_COUNT}")

        raw_links = await page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll('a[href]'));
                return links.map(a => a.href).filter(href => 
                    href.includes('l.facebook.com/l.php?u=') || href.includes('fb.me/')
                );
            }
        """)

        print(f"[DEBUG] Trovati {len(raw_links)} link potenziali")

        landing_pages = []
        seen_urls: Set[str] = set()
        direct_urls = []
        shortlinks = []
        
        for link in raw_links:
            if "l.facebook.com/l.php?u=" in link:
                real_url = extract_real_url(link)
                if real_url:
                    direct_urls.append(real_url)
            else:
                shortlinks.append(link)
        
        for url in direct_urls:
            norm_url = normalize_url(url)
            if not should_exclude_url(norm_url) and norm_url not in seen_urls:
                landing_pages.append(norm_url)
                seen_urls.add(norm_url)
                print(f"[DEBUG] Landing page trovata (diretta): {norm_url}")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        
        async def resolve_with_limit(link):
            async with semaphore:
                return await resolve_shortlink(context, link)
        
        resolved = await asyncio.gather(*[resolve_with_limit(link) for link in shortlinks])
        
        for url in resolved:
            if url:
                norm_url = normalize_url(url)
                if not should_exclude_url(norm_url) and norm_url not in seen_urls:
                    landing_pages.append(norm_url)
                    seen_urls.add(norm_url)
                    print(f"[DEBUG] Landing page trovata (shortlink): {norm_url}")

        await browser.close()
        return landing_pages

async def scrape_single_lead(context, url: str) -> Dict:
    """Scrape singolo lead - da eseguire in parallelo"""
    lead = {
        "landing_page": url,
        "email": None,
        "telefono": None,
        "copy_valutazione": None
    }
    
    try:
        page = await context.new_page()
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="networkidle")
        await page.wait_for_timeout(1000)
        
        data = await page.evaluate("""
            () => {
                const allText = document.documentElement.innerText || document.body.innerText;
                const allHTML = document.documentElement.innerHTML;
                const allLinks = Array.from(document.querySelectorAll('a[href]'))
                    .map(a => a.href).join(' ');
                
                return {
                    text: allText,
                    html: allHTML,
                    links: allLinks
                };
            }
        """)
        
        full_content = data['html'] + ' ' + data['text'] + ' ' + data['links']
        text_lower = data['text'].lower()
        
        # EMAIL SEARCH
        email_pattern = r'\b[a-zA-Z0-9][a-zA-Z0-9._%+-]*@[a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,}\b'
        found_emails = re.findall(email_pattern, full_content)
        valid_emails = [e for e in found_emails if validate_email(e)]
        if valid_emails:
            lead["email"] = valid_emails[0]
        
        # PHONE SEARCH
        phone_patterns = [
            r'\+39[\s\-]?\d{2,3}[\s\-]?\d{6,7}',
            r'0\d{1,3}[\s\-]?\d{6,8}',
            r'\+\d{1,3}[\s\-]?\d{9,12}',
            r'\(\+39\)[\s\-]?\d{9,10}',
        ]
        
        found_phones = []
        for pattern in phone_patterns:
            found_phones.extend(re.findall(pattern, full_content))
        
        valid_phones = [p for p in found_phones if validate_phone(p)]
        if valid_phones:
            lead["telefono"] = valid_phones[0].strip()
        
        # MAILTO E TEL
        if not lead["email"]:
            mailto = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', full_content)
            valid_mailto = [e for e in mailto if validate_email(e)]
            if valid_mailto:
                lead["email"] = valid_mailto[0]
        
        if not lead["telefono"]:
            tel = re.findall(r'tel:([+\d\s\-\(\)]+)', full_content)
            valid_tel = [t for t in tel if validate_phone(t)]
            if valid_tel:
                lead["telefono"] = valid_tel[0].strip()
        
        # COPY VALUTAZIONE
        keywords_strong = ["gratis", "gratuito", "free", "sconto", "offerta", "promo", "risparmia"]
        keywords_medium = ["lezione", "webinar", "corso", "training", "consulenza"]
        
        strong_count = sum(1 for word in keywords_strong if word in text_lower)
        medium_count = sum(1 for word in keywords_medium if word in text_lower)
        
        if strong_count >= 2 or (strong_count >= 1 and medium_count >= 1):
            lead["copy_valutazione"] = "Copy molto interessante"
        elif strong_count >= 1 or medium_count >= 2:
            lead["copy_valutazione"] = "Copy interessante"
        elif medium_count >= 1:
            lead["copy_valutazione"] = "Copy discreto"
        else:
            lead["copy_valutazione"] = "Copy standard"

        await page.close()
        
        status = "✓" if (lead["email"] or lead["telefono"]) else "○"
        print(f"[DEBUG] {status} {url[:60]}... | Email: {bool(lead['email'])} | Tel: {bool(lead['telefono'])}")
        
    except Exception as e:
        print(f"[DEBUG] ✗ Errore scraping {url}: {e}")
    
    return lead

async def get_real_leads(query: str) -> List[Dict]:
    landing_urls = await get_real_landing_urls(query)
    
    if not landing_urls:
        print("[DEBUG] Nessuna landing page trovata")
        return []
    
    print(f"[DEBUG] Inizio scraping di {len(landing_urls)} landing pages...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        
        async def scrape_with_limit(url):
            async with semaphore:
                return await scrape_single_lead(context, url)
        
        leads = await asyncio.gather(*[scrape_with_limit(url) for url in landing_urls])
        
        await browser.close()
    
    print(f"[DEBUG] Scraping completato: {len(leads)} leads")
    return leads

# ---------------- TEST ----------------
if __name__ == "__main__":
    async def test():
        import time
        start = time.time()
        
        query = "fitness"
        print(f"\n{'='*60}")
        print(f"TEST QUERY: {query}")
        print(f"{'='*60}\n")
        
        leads = await get_real_leads(query)
        
        elapsed = time.time() - start
        print(f"\n{'='*60}")
        print(f"RISULTATI:")
        print(f"{'='*60}")
        print(f"Tempo totale: {elapsed:.2f}s")
        print(f"Leads trovati: {len(leads)}\n")
        
        for i, lead in enumerate(leads, 1):
            print(f"Lead #{i}:")
            print(f"  Landing: {lead['landing_page']}")
            print(f"  Email: {lead['email']}")
            print(f"  Telefono: {lead['telefono']}")
            print(f"  Copy: {lead['copy_valutazione']}\n")
    
    asyncio.run(test())