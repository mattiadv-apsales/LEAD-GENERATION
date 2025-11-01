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
SCROLL_COUNT = 3
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

async def get_real_landing_urls(query: str) -> List[Dict[str, str]]:
    """Ritorna lista di dict con url e ad_link"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        search_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={COUNTRY}&q={query.replace(' ', '%20')}"
        print(f"[DEBUG] Apertura Meta Ads Library: {search_url}")
        
        await page.goto(search_url, wait_until="networkidle")
        await page.wait_for_timeout(INITIAL_WAIT)
        
        # Aspetta che si carichino gli ads
        try:
            await page.wait_for_selector('a[href*="l.facebook.com"]', timeout=5000)
            print("[DEBUG] Ads caricati")
        except:
            print("[DEBUG] Timeout caricamento ads, continuo comunque...")

        for i in range(SCROLL_COUNT):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(SCROLL_WAIT)
            print(f"[DEBUG] Scroll {i+1}/{SCROLL_COUNT}")
        
        # Screenshot per debug (opzionale)
        # await page.screenshot(path="debug_ads_library.png")
        # print("[DEBUG] Screenshot salvato: debug_ads_library.png")

        # Estrai link ads e landing pages insieme
        ads_data = await page.evaluate("""
            () => {
                const results = [];
                
                // Cerca tutti i link che vanno a landing pages
                const allLinks = Array.from(document.querySelectorAll('a[href]'));
                
                allLinks.forEach(link => {
                    const href = link.href;
                    
                    // Se è un link a landing page
                    if (href.includes('l.facebook.com/l.php?u=') || href.includes('fb.me/')) {
                        // Cerca il link dell'ads in vari modi
                        let adUrl = null;
                        let parent = link.parentElement;
                        let levels = 0;
                        
                        // Metodo 1: Cerca link con ?id= nel parent
                        while (parent && levels < 15) {
                            // Cerca tutti i link nel parent
                            const linksInParent = parent.querySelectorAll('a[href*="facebook.com/ads/library"]');
                            for (let adLink of linksInParent) {
                                if (adLink.href.includes('?id=')) {
                                    adUrl = adLink.href;
                                    break;
                                }
                            }
                            
                            if (adUrl) break;
                            
                            // Metodo 2: Cerca data-ad-id o altri attributi
                            if (parent.hasAttribute('data-ad-id')) {
                                const adId = parent.getAttribute('data-ad-id');
                                adUrl = `https://www.facebook.com/ads/library/?id=${adId}`;
                                break;
                            }
                            
                            parent = parent.parentElement;
                            levels++;
                        }
                        
                        // Se non trova niente, cerca il testo "See ad details" o "Vedi dettagli inserzione"
                        if (!adUrl && link.parentElement) {
                            const container = link.closest('div[role="article"]') || link.closest('[data-pagelet]');
                            if (container) {
                                const detailsLinks = container.querySelectorAll('a');
                                for (let dl of detailsLinks) {
                                    const text = dl.textContent.toLowerCase();
                                    if ((text.includes('see') && text.includes('detail')) || 
                                        (text.includes('vedi') && text.includes('dettagli'))) {
                                        adUrl = dl.href;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        results.push({
                            landing: href,
                            ad_url: adUrl
                        });
                    }
                });
                
                return results;
            }
        """)

        print(f"[DEBUG] Trovati {len(ads_data)} link potenziali con ads")
        
        # Debug: conta quanti hanno ad_url
        with_ad = sum(1 for item in ads_data if item['ad_url'])
        without_ad = len(ads_data) - with_ad
        print(f"[DEBUG] Con ad link: {with_ad} | Senza ad link: {without_ad}")
        
        # Debug: mostra primi 3 risultati
        for i, item in enumerate(ads_data[:3]):
            landing_preview = item['landing'][:80] + "..." if len(item['landing']) > 80 else item['landing']
            ad_preview = (item['ad_url'][:80] + "...") if item['ad_url'] and len(item['ad_url']) > 80 else (item['ad_url'] or 'None')
            print(f"[DEBUG] Esempio {i+1}:")
            print(f"  Landing: {landing_preview}")
            print(f"  Ad URL: {ad_preview}")

        landing_pages = []
        seen_urls: Set[str] = set()
        
        for item in ads_data:
            landing_link = item['landing']
            ad_url = item['ad_url']
            
            real_url = None
            
            if "l.facebook.com/l.php?u=" in landing_link:
                real_url = extract_real_url(landing_link)
            else:
                # Shortlink
                real_url = await resolve_shortlink(context, landing_link)
            
            if real_url:
                norm_url = normalize_url(real_url)
                if not should_exclude_url(norm_url) and norm_url not in seen_urls:
                    landing_pages.append({
                        'url': norm_url,
                        'ad_link': ad_url or 'Non disponibile'
                    })
                    seen_urls.add(norm_url)
                    print(f"[DEBUG] Landing page trovata: {norm_url}")
                    if ad_url:
                        print(f"[DEBUG]   └─ Ad link: {ad_url}")

        await browser.close()
        return landing_pages

async def scrape_single_lead(context, url: str, ad_link: str) -> Dict:
    """Scrape singolo lead - da eseguire in parallelo"""
    lead = {
        "landing_page": url,
        "ad_link": ad_link,
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
    landing_data = await get_real_landing_urls(query)
    
    if not landing_data:
        print("[DEBUG] Nessuna landing page trovata")
        return []
    
    print(f"[DEBUG] Inizio scraping di {len(landing_data)} landing pages...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        
        async def scrape_with_limit(data):
            async with semaphore:
                return await scrape_single_lead(context, data['url'], data['ad_link'])
        
        leads = await asyncio.gather(*[scrape_with_limit(data) for data in landing_data])
        
        await browser.close()
    
    print(f"[DEBUG] Scraping completato: {len(leads)} leads")
    return leads