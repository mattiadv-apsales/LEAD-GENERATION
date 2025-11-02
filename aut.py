import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urlparse, parse_qs, unquote
import re
from typing import List, Dict, Set, Optional
import logging

# ---------------- CONFIG ----------------
COUNTRY = "IT"
EXCLUDE_DOMAINS = [
    "facebook.com", "fb.com", "fb.me", "fbcdn.net",
    "instagram.com", "instagr.am", "whatsapp.com", "tinyurl.com",
    "bit.ly", "metastatus.com", "static.xx.fbcdn.net"
]

# Performance tuning - RENDER OPTIMIZED
MAX_CONCURRENT_PAGES = 2
SCROLL_COUNT = 4  # Aumentato per caricare più ads
SCROLL_WAIT = 2000  # 2s tra scroll
PAGE_TIMEOUT = 25000
INITIAL_WAIT = 5000  # 5s iniziali per caricare JS
MAX_RETRIES = 1  # Solo 1 retry per velocità

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------- UTILITY FUNCTIONS ----------------
def normalize_url(url: str) -> str:
    """Rimuove trailing slash e query inutili"""
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    try:
        parsed = urlparse(url)
        if parsed.query:
            essential_params = ['id', 'p', 'page']
            query_dict = parse_qs(parsed.query)
            filtered = {k: v for k, v in query_dict.items() if k in essential_params}
            if filtered:
                from urllib.parse import urlencode
                new_query = urlencode(filtered, doseq=True)
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
            else:
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        pass
    return url

def extract_real_url(link: str) -> Optional[str]:
    """Estrae URL reale da link Facebook"""
    try:
        if "l.facebook.com/l.php?u=" in link:
            parsed = parse_qs(urlparse(link).query)
            if "u" in parsed:
                decoded = unquote(parsed["u"][0])
                decoded = re.sub(r'[?&]fbclid=[^&]*', '', decoded)
                return decoded
    except Exception as e:
        logger.debug(f"Errore estrazione URL: {e}")
    return None

async def resolve_shortlink(context, link: str) -> Optional[str]:
    """Risolve shortlink fb.me"""
    temp_page = None
    try:
        temp_page = await context.new_page()
        response = await temp_page.goto(link, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        real_url = response.url
        await temp_page.close()
        return real_url
    except Exception as e:
        if temp_page:
            await temp_page.close()
        logger.debug(f"Errore risoluzione shortlink {link}: {e}")
        return None

def should_exclude_url(url: str) -> bool:
    """Verifica se URL va escluso"""
    return any(d in url.lower() for d in EXCLUDE_DOMAINS)

def validate_email(email: str) -> bool:
    """Valida email"""
    email = email.lower().strip()
    invalid_patterns = [
        'example.com', 'test.com', 'dummy', 'placeholder',
        'noreply', 'no-reply', 'youremail', 'your-email',
        'email@', '@email', 'info@info', 'admin@admin',
        'sample', 'fake', 'tempmail'
    ]
    if any(pattern in email for pattern in invalid_patterns):
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    if re.search(r'[._%+-]{3,}', email):
        return False
    return True

def validate_phone(phone: str) -> bool:
    """Valida telefono"""
    clean = re.sub(r'[\s\-\(\)\.]', '', phone)
    if not re.match(r'^\+?\d{9,15}$', clean):
        return False
    if re.match(r'^(\d)\1+$', clean) or clean in ['123456789', '987654321', '1234567890']:
        return False
    if re.search(r'(\d{3,})\1', clean):
        return False
    return True

# ---------------- MAIN SCRAPING ----------------
async def get_real_landing_urls(query: str) -> List[Dict[str, str]]:
    """Estrae landing + ad_link con STRATEGIA AGGRESSIVA per Render"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',  # Per Render con poca RAM
                '--no-sandbox',  # Necessario su container
            ]
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            # Simula comportamento umano
            locale='it-IT',
            timezone_id='Europe/Rome',
        )
        page = await context.new_page()

        search_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={COUNTRY}&q={query.replace(' ', '%20')}"
        logger.info(f"🔍 Query: {query}")
        
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=30000)
            
            # ⏳ ATTESA INIZIALE LUNGA - fondamentale su Render
            logger.info("⏳ Attesa caricamento iniziale (5s)...")
            await page.wait_for_timeout(INITIAL_WAIT)
            
            # Verifica caricamento
            try:
                await page.wait_for_selector('div[role="main"]', timeout=10000)
                logger.info("✓ Pagina caricata")
            except:
                logger.warning("⚠️ Timeout pagina principale, continuo...")
            
            # 🔄 SCROLL AGGRESSIVO - carica più ads possibile
            logger.info("📜 Inizio scroll...")
            for i in range(SCROLL_COUNT):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(SCROLL_WAIT)
                logger.info(f"  Scroll {i+1}/{SCROLL_COUNT}")
            
            # Torna su per assicurarsi rendering completo
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1500)
            
            # 📊 DEBUG INFO
            stats = await page.evaluate("""
                () => {
                    return {
                        totalLinks: document.querySelectorAll('a[href]').length,
                        fbLinks: document.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]').length,
                        adIdLinks: document.querySelectorAll('a[href*="ads/library/?id="]').length,
                        bodyLength: document.body.innerHTML.length
                    };
                }
            """)
            logger.info(f"📊 Links: {stats['totalLinks']} | FB: {stats['fbLinks']} | AdID: {stats['adIdLinks']} | HTML: {stats['bodyLength']} chars")

            # 🎯 ESTRAZIONE MULTI-METODO
            ads_data = await page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    
                    // ============ METODO 1: Da link "Vedi dettagli" ============
                    const detailLinks = document.querySelectorAll('a[href*="ads/library/?id="]');
                    detailLinks.forEach(detailLink => {
                        const adIdMatch = detailLink.href.match(/id=(\d+)/);
                        if (!adIdMatch) return;
                        
                        const adUrl = `https://www.facebook.com/ads/library/?id=${adIdMatch[1]}`;
                        
                        // Cerca landing page nei dintorni
                        let container = detailLink.closest('div[class*="x1"]') || detailLink.closest('[role="article"]');
                        if (!container) {
                            // Risali manualmente
                            container = detailLink.parentElement;
                            for (let i = 0; i < 15 && container; i++) {
                                const landingLinks = container.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                                if (landingLinks.length > 0) break;
                                container = container.parentElement;
                            }
                        }
                        
                        if (container) {
                            const landingLinks = container.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                            landingLinks.forEach(link => {
                                if (!seen.has(link.href)) {
                                    seen.add(link.href);
                                    results.push({ landing: link.href, ad_url: adUrl });
                                }
                            });
                        }
                    });
                    
                    // ============ METODO 2: Da landing link → cerca Ad ID ============
                    const allLandings = document.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                    allLandings.forEach(link => {
                        if (seen.has(link.href)) return;
                        
                        let adUrl = null;
                        
                        // Risali nel DOM cercando Ad ID
                        let parent = link.parentElement;
                        for (let level = 0; level < 25 && parent; level++) {
                            // Metodo A: Cerca link con id=
                            const adLink = parent.querySelector('a[href*="ads/library/?id="]');
                            if (adLink) {
                                const match = adLink.href.match(/id=(\d+)/);
                                if (match) {
                                    adUrl = `https://www.facebook.com/ads/library/?id=${match[1]}`;
                                    break;
                                }
                            }
                            
                            // Metodo B: Cerca nell'HTML
                            const html = parent.innerHTML;
                            const htmlMatch = html.match(/ads\/library\/\?id=(\d+)/);
                            if (htmlMatch) {
                                adUrl = `https://www.facebook.com/ads/library/?id=${htmlMatch[1]}`;
                                break;
                            }
                            
                            // Metodo C: Cerca nel testo
                            const text = parent.innerText || parent.textContent || '';
                            const textMatch = text.match(/(?:ID libreria|Library ID|Ad ID)[\s:]+(\d+)/i);
                            if (textMatch) {
                                adUrl = `https://www.facebook.com/ads/library/?id=${textMatch[1]}`;
                                break;
                            }
                            
                            parent = parent.parentElement;
                        }
                        
                        seen.add(link.href);
                        results.push({ landing: link.href, ad_url: adUrl });
                    });
                    
                    // ============ METODO 3: Regex sull'intero HTML (FALLBACK) ============
                    if (results.every(r => !r.ad_url)) {
                        const bodyHtml = document.body.innerHTML;
                        const allAdIds = [...bodyHtml.matchAll(/ads\/library\/\?id=(\d+)/g)];
                        
                        if (allAdIds.length > 0) {
                            // Prendi primo Ad ID trovato e applicalo a tutti
                            const firstAdId = allAdIds[0][1];
                            const fallbackUrl = `https://www.facebook.com/ads/library/?id=${firstAdId}`;
                            
                            results.forEach(r => {
                                if (!r.ad_url) r.ad_url = fallbackUrl;
                            });
                        }
                    }
                    
                    return results;
                }
            """)

            logger.info(f"✅ Estratti {len(ads_data)} link")
            
            with_ad = sum(1 for x in ads_data if x['ad_url'])
            logger.info(f"   📎 Con Ad ID: {with_ad} | ⚠️ Senza: {len(ads_data) - with_ad}")
            
            # Mostra esempi
            for i, item in enumerate(ads_data[:2]):
                ad_status = "✓" if item['ad_url'] else "✗"
                logger.info(f"{ad_status} Esempio {i+1}: {item['landing'][:60]}...")
                if item['ad_url']:
                    logger.info(f"   Ad: {item['ad_url'][:70]}...")

            # Processa e normalizza URL
            landing_pages = []
            seen_urls: Set[str] = set()
            
            for item in ads_data:
                landing_link = item['landing']
                ad_url = item['ad_url']
                
                real_url = None
                if "l.facebook.com/l.php?u=" in landing_link:
                    real_url = extract_real_url(landing_link)
                else:
                    real_url = await resolve_shortlink(context, landing_link)
                
                if real_url:
                    norm_url = normalize_url(real_url)
                    if not should_exclude_url(norm_url) and norm_url not in seen_urls:
                        landing_pages.append({
                            'url': norm_url,
                            'ad_link': ad_url or 'Non disponibile'
                        })
                        seen_urls.add(norm_url)

            logger.info(f"🎯 Landing finali: {len(landing_pages)}")

        except Exception as e:
            logger.error(f"❌ Errore scraping: {e}")
        finally:
            await browser.close()
        
        return landing_pages

async def scrape_single_lead(context, url: str, ad_link: str) -> Dict:
    """Scrape singolo lead - NO RETRY"""
    lead = {
        "landing_page": url,
        "ad_link": ad_link,
        "email": None,
        "telefono": None,
        "copy_valutazione": None,
        "status": "error"
    }
    
    page = None
    try:
        page = await context.new_page()
        await page.route("**/*.{png,jpg,jpeg,gif,svg,webp,css,woff,woff2}", lambda route: route.abort())
        
        await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(800)
        
        data = await page.evaluate("""
            () => ({
                text: document.documentElement.innerText || document.body.innerText,
                html: document.documentElement.innerHTML,
                links: Array.from(document.querySelectorAll('a[href]')).map(a => a.href).join(' ')
            })
        """)
        
        full_content = data['html'] + ' ' + data['text'] + ' ' + data['links']
        text_lower = data['text'].lower()
        
        # EMAIL
        email_pattern = r'\b[a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}@[a-zA-Z0-9][a-zA-Z0-9.-]{0,253}\.[a-zA-Z]{2,}\b'
        found_emails = list(dict.fromkeys(re.findall(email_pattern, full_content)))
        valid_emails = [e for e in found_emails if validate_email(e)]
        if valid_emails:
            lead["email"] = valid_emails[0]
        
        # PHONE
        phone_patterns = [
            r'\+39[\s\-]?\d{2,3}[\s\-]?\d{6,7}',
            r'0\d{1,3}[\s\-]?\d{6,8}',
            r'\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{6,10}',
            r'\(\+39\)[\s\-]?\d{9,10}',
        ]
        found_phones = []
        for pattern in phone_patterns:
            found_phones.extend(re.findall(pattern, full_content))
        found_phones = list(dict.fromkeys(found_phones))
        valid_phones = [p for p in found_phones if validate_phone(p)]
        if valid_phones:
            lead["telefono"] = valid_phones[0].strip()
        
        # MAILTO/TEL fallback
        if not lead["email"]:
            mailto = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', full_content, re.IGNORECASE)
            valid_mailto = [e for e in mailto if validate_email(e)]
            if valid_mailto:
                lead["email"] = valid_mailto[0]
        
        if not lead["telefono"]:
            tel = re.findall(r'tel:([+\d\s\-\(\)]+)', full_content, re.IGNORECASE)
            valid_tel = [t for t in tel if validate_phone(t)]
            if valid_tel:
                lead["telefono"] = valid_tel[0].strip()
        
        # COPY
        keywords_strong = ["gratis", "gratuito", "free", "sconto", "offerta", "promo", "risparmia", "omaggio"]
        keywords_medium = ["lezione", "webinar", "corso", "training", "consulenza", "demo", "prova"]
        keywords_weak = ["scopri", "impara", "migliora", "garantito"]
        
        strong_count = sum(1 for word in keywords_strong if word in text_lower)
        medium_count = sum(1 for word in keywords_medium if word in text_lower)
        weak_count = sum(1 for word in keywords_weak if word in text_lower)
        
        if strong_count >= 3:
            lead["copy_valutazione"] = "Copy molto interessante (alto incentivo)"
        elif strong_count >= 2 or (strong_count >= 1 and medium_count >= 2):
            lead["copy_valutazione"] = "Copy molto interessante"
        elif strong_count >= 1 or medium_count >= 2:
            lead["copy_valutazione"] = "Copy interessante"
        elif medium_count >= 1 or weak_count >= 2:
            lead["copy_valutazione"] = "Copy discreto"
        else:
            lead["copy_valutazione"] = "Copy standard"

        lead["status"] = "success"
        status = "✓" if (lead["email"] or lead["telefono"]) else "○"
        logger.info(f"{status} {url[:40]}... | E:{bool(lead['email'])} T:{bool(lead['telefono'])}")
        
    except Exception as e:
        logger.warning(f"✗ {url[:40]}... (timeout/error)")
        lead["status"] = "timeout"
    finally:
        if page:
            await page.close()
    
    return lead

async def get_real_leads(query: str) -> List[Dict]:
    """Main function"""
    try:
        landing_data = await get_real_landing_urls(query)
        
        if not landing_data:
            logger.warning("❌ Nessuna landing trovata")
            return []
        
        logger.info(f"🚀 Scraping {len(landing_data)} landing pages...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage']
            )
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
            
            async def scrape_with_limit(data):
                async with semaphore:
                    return await scrape_single_lead(context, data['url'], data['ad_link'])
            
            leads = await asyncio.gather(*[scrape_with_limit(data) for data in landing_data])
            await browser.close()
        
        ok = sum(1 for l in leads if l["status"] == "success")
        timeout = sum(1 for l in leads if l["status"] == "timeout")
        logger.info(f"✅ {ok} OK | ⏱️ {timeout} timeout | 📊 {len(leads)} totali")
        
        return leads
        
    except Exception as e:
        logger.error(f"❌ Errore critico: {e}")
        return []