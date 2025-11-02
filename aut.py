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

# Performance tuning
MAX_CONCURRENT_PAGES = 3  # Ridotto per stabilità
SCROLL_COUNT = 3
SCROLL_WAIT = 1000
PAGE_TIMEOUT = 15000  # Aumentato per stabilità
INITIAL_WAIT = 3000
MAX_RETRIES = 2  # Retry per richieste fallite

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------- UTILITY FUNCTIONS ----------------
def normalize_url(url: str) -> str:
    """Rimuove trailing slash e query inutili per uniformare gli URL"""
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    # Rimuove parametri UTM e tracking comuni
    try:
        parsed = urlparse(url)
        if parsed.query:
            # Mantieni solo parametri essenziali
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
    """Estrae URL reale da link Facebook (senza aprire pagina)"""
    try:
        if "l.facebook.com/l.php?u=" in link:
            parsed = parse_qs(urlparse(link).query)
            if "u" in parsed:
                decoded = unquote(parsed["u"][0])
                # Rimuovi eventuali parametri fbclid
                decoded = re.sub(r'[?&]fbclid=[^&]*', '', decoded)
                return decoded
    except Exception as e:
        logger.debug(f"Errore estrazione URL: {e}")
    return None

async def resolve_shortlink(context, link: str) -> Optional[str]:
    """Risolve shortlink come fb.me con retry"""
    for attempt in range(MAX_RETRIES):
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
            if attempt == MAX_RETRIES - 1:
                logger.debug(f"Errore risoluzione shortlink {link}: {e}")
                return None
            await asyncio.sleep(1)
    return None

def should_exclude_url(url: str) -> bool:
    """Verifica se URL va escluso"""
    return any(d in url.lower() for d in EXCLUDE_DOMAINS)

def validate_email(email: str) -> bool:
    """Valida se l'email è reale e non un placeholder"""
    email = email.lower().strip()
    
    # Pattern invalidi
    invalid_patterns = [
        'example.com', 'test.com', 'dummy', 'placeholder',
        'noreply', 'no-reply', 'youremail', 'your-email',
        'email@', '@email', 'info@info', 'admin@admin',
        'sample', 'fake', 'tempmail'
    ]
    
    if any(pattern in email for pattern in invalid_patterns):
        return False
    
    # Verifica formato base
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, email):
        return False
    
    # Evita email con troppi caratteri speciali consecutivi
    if re.search(r'[._%+-]{3,}', email):
        return False
    
    return True

def validate_phone(phone: str) -> bool:
    """Valida se il telefono è reale"""
    clean = re.sub(r'[\s\-\(\)\.]', '', phone)
    
    # Formato base
    if not re.match(r'^\+?\d{9,15}$', clean):
        return False
    
    # Evita pattern ovvi
    if re.match(r'^(\d)\1+$', clean) or clean in ['123456789', '987654321', '1234567890']:
        return False
    
    # Evita sequenze troppo regolari
    if re.search(r'(\d{3,})\1', clean):
        return False
    
    return True

# ---------------- MAIN SCRAPING LOGIC ----------------
async def get_real_landing_urls(query: str) -> List[Dict[str, str]]:
    """Ritorna lista di dict con url e ad_link"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        search_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={COUNTRY}&q={query.replace(' ', '%20')}"
        logger.info(f"Apertura Meta Ads Library per query: {query}")
        
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(INITIAL_WAIT)
            
            # Aspetta che si carichino gli ads con strategia più aggressiva
            try:
                # Prima aspetta il caricamento base
                await page.wait_for_selector('div[role="main"]', timeout=5000)
                logger.info("Pagina principale caricata")
                
                # Poi aspetta i link di landing
                await page.wait_for_selector('a[href*="l.facebook.com"], a[href*="fb.me"]', timeout=10000)
                logger.info("Ads caricati con successo")
            except Exception as e:
                logger.warning(f"Timeout caricamento ads: {e}. Continuo comunque...")
                # Aspetta comunque un po' di più
                await page.wait_for_timeout(3000)

            # Scroll progressivo per caricare più contenuti
            for i in range(SCROLL_COUNT):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(SCROLL_WAIT)
                logger.info(f"Scroll {i+1}/{SCROLL_COUNT} completato")
            
            # Scroll finale verso l'alto per assicurarsi che tutto sia renderizzato
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
            
            # DEBUG: Conta quanti link totali ci sono
            link_count = await page.evaluate("""
                () => {
                    const allLinks = document.querySelectorAll('a[href]');
                    const fbLinks = Array.from(allLinks).filter(a => 
                        a.href.includes('l.facebook.com/l.php?u=') || a.href.includes('fb.me/')
                    );
                    return {
                        total: allLinks.length,
                        fbLinks: fbLinks.length
                    };
                }
            """)
            logger.info(f"Link totali: {link_count['total']}, Link FB: {link_count['fbLinks']}")
            
            # Screenshot per debug (opzionale ma utile)
            try:
                await page.screenshot(path="debug_ads_library.png", full_page=True)
                logger.info("Screenshot debug salvato: debug_ads_library.png")
            except:
                pass

            # Estrai link ads e landing pages - NUOVO approccio basato su struttura reale
            ads_data = await page.evaluate("""
                () => {
                    const results = [];
                    const processedLandings = new Set();
                    
                    // Cerca tutti i container principali degli ads (xh8yej3 è la classe principale)
                    const adContainers = document.querySelectorAll('.xh8yej3');
                    
                    adContainers.forEach(container => {
                        // Cerca "ID libreria: XXXXXXXX" nel testo
                        const text = container.innerText || container.textContent;
                        const idMatch = text.match(/ID libreria:\s*(\d+)/i);
                        
                        if (!idMatch) return; // Se non ha ID libreria, non è un ad
                        
                        const adId = idMatch[1];
                        const adUrl = `https://www.facebook.com/ads/library/?id=${adId}`;
                        
                        // Cerca landing page nel container
                        const landingLinks = container.querySelectorAll('a[href*="l.facebook.com/l.php"], a[href*="fb.me"]');
                        
                        if (landingLinks.length > 0) {
                            landingLinks.forEach(link => {
                                const href = link.href;
                                if (!processedLandings.has(href)) {
                                    processedLandings.add(href);
                                    results.push({
                                        landing: href,
                                        ad_url: adUrl
                                    });
                                }
                            });
                        }
                    });
                    
                    // FALLBACK: Se non trova niente con il metodo sopra
                    if (results.length === 0) {
                        // Cerca tutti i possibili container
                        const allContainers = [
                            ...document.querySelectorAll('[data-pagelet]'),
                            ...document.querySelectorAll('[role="article"]'),
                            ...document.querySelectorAll('div[class*="x1yztbdb"]')
                        ];
                        
                        allContainers.forEach(container => {
                            const text = container.innerText || container.textContent;
                            
                            // Metodo 1: Cerca "ID libreria"
                            let adUrl = null;
                            const idMatch = text.match(/ID libreria:\s*(\d+)/i) || 
                                          text.match(/Library ID:\s*(\d+)/i);
                            
                            if (idMatch) {
                                adUrl = `https://www.facebook.com/ads/library/?id=${idMatch[1]}`;
                            }
                            
                            // Metodo 2: Cerca nel HTML
                            if (!adUrl) {
                                const html = container.innerHTML;
                                const htmlIdMatch = html.match(/ads\/library\/\?id=(\d+)/);
                                if (htmlIdMatch) {
                                    adUrl = `https://www.facebook.com/ads/library/?id=${htmlIdMatch[1]}`;
                                }
                            }
                            
                            // Cerca landing pages
                            const landingLinks = container.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                            
                            landingLinks.forEach(link => {
                                const href = link.href;
                                if (!processedLandings.has(href)) {
                                    processedLandings.add(href);
                                    results.push({
                                        landing: href,
                                        ad_url: adUrl
                                    });
                                }
                            });
                        });
                    }
                    
                    // ULTIMO FALLBACK: Prendi tutte le landing senza ad link
                    if (results.length === 0) {
                        const allLandings = document.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                        allLandings.forEach(link => {
                            const href = link.href;
                            if (!processedLandings.has(href)) {
                                processedLandings.add(href);
                                
                                // Cerca ID risalendo nel DOM
                                let adUrl = null;
                                let parent = link.parentElement;
                                let levels = 0;
                                
                                while (parent && levels < 25) {
                                    const parentText = parent.innerText || parent.textContent;
                                    const idMatch = parentText.match(/ID libreria:\s*(\d+)/i);
                                    if (idMatch) {
                                        adUrl = `https://www.facebook.com/ads/library/?id=${idMatch[1]}`;
                                        break;
                                    }
                                    parent = parent.parentElement;
                                    levels++;
                                }
                                
                                results.push({
                                    landing: href,
                                    ad_url: adUrl
                                });
                            }
                        });
                    }
                    
                    return results;
                }
            """)

            logger.info(f"Trovati {len(ads_data)} link potenziali")
            
            # Debug dettagliato
            with_ad = sum(1 for item in ads_data if item['ad_url'])
            without_ad = len(ads_data) - with_ad
            logger.info(f"Con ad link: {with_ad} | Senza ad link: {without_ad}")
            
            # Mostra primi 3 esempi dettagliati
            for i, item in enumerate(ads_data[:3]):
                logger.info(f"Esempio {i+1}:")
                logger.info(f"  Landing: {item['landing'][:80]}...")
                logger.info(f"  Ad URL: {item['ad_url'][:80] if item['ad_url'] else 'NON TROVATO'}")
            
            # Se non trova nulla, prova un approccio ancora più semplice
            if len(ads_data) == 0:
                logger.warning("Nessun link trovato con metodo principale, provo approccio alternativo...")
                
                # Estrai TUTTI i link e filtra dopo
                all_links = await page.evaluate("""
                    () => {
                        return Array.from(document.querySelectorAll('a[href]'))
                            .map(a => a.href)
                            .filter(href => href.includes('l.facebook.com/l.php?u=') || href.includes('fb.me/'));
                    }
                """)
                
                logger.info(f"Metodo alternativo: trovati {len(all_links)} link")
                
                # Converti in formato compatibile
                ads_data = [{'landing': link, 'ad_url': None} for link in all_links]
            
            with_ad = sum(1 for item in ads_data if item['ad_url'])
            logger.info(f"Con ad link: {with_ad} | Senza ad link: {len(ads_data) - with_ad}")

            landing_pages = []
            seen_urls: Set[str] = set()
            
            # Processa i link con rate limiting
            for i, item in enumerate(ads_data):
                if i > 0 and i % 5 == 0:
                    await asyncio.sleep(0.5)  # Rate limiting
                
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
                        logger.info(f"Landing trovata: {norm_url[:60]}...")

        except Exception as e:
            logger.error(f"Errore durante scraping Meta Ads: {e}")
        finally:
            await browser.close()
        
        return landing_pages

async def scrape_single_lead(context, url: str, ad_link: str) -> Dict:
    """Scrape singolo lead con retry e gestione errori migliorata"""
    lead = {
        "landing_page": url,
        "ad_link": ad_link,
        "email": None,
        "telefono": None,
        "copy_valutazione": None,
        "status": "error"
    }
    
    for attempt in range(MAX_RETRIES):
        page = None
        try:
            page = await context.new_page()
            
            # Blocca risorse non necessarie per velocità
            await page.route("**/*.{png,jpg,jpeg,gif,svg,webp,css,woff,woff2}", lambda route: route.abort())
            
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            await page.wait_for_timeout(1500)
            
            # Scroll per caricare lazy content
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(500)
            
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
            
            # EMAIL SEARCH - pattern migliorato
            email_pattern = r'\b[a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}@[a-zA-Z0-9][a-zA-Z0-9.-]{0,253}\.[a-zA-Z]{2,}\b'
            found_emails = re.findall(email_pattern, full_content)
            
            # Deduplica e valida
            found_emails = list(dict.fromkeys(found_emails))  # Rimuovi duplicati mantenendo ordine
            valid_emails = [e for e in found_emails if validate_email(e)]
            
            if valid_emails:
                lead["email"] = valid_emails[0]
            
            # PHONE SEARCH - pattern estesi
            phone_patterns = [
                r'\+39[\s\-]?\d{2,3}[\s\-]?\d{6,7}',  # IT con prefisso
                r'0\d{1,3}[\s\-]?\d{6,8}',             # IT senza prefisso
                r'\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{6,10}',  # Internazionale
                r'\(\+39\)[\s\-]?\d{9,10}',            # IT con parentesi
            ]
            
            found_phones = []
            for pattern in phone_patterns:
                found_phones.extend(re.findall(pattern, full_content))
            
            # Deduplica e valida
            found_phones = list(dict.fromkeys(found_phones))
            valid_phones = [p for p in found_phones if validate_phone(p)]
            
            if valid_phones:
                lead["telefono"] = valid_phones[0].strip()
            
            # MAILTO E TEL fallback
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
            
            # COPY VALUTAZIONE migliorata
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
            await page.close()
            
            status = "✓" if (lead["email"] or lead["telefono"]) else "○"
            logger.info(f"{status} {url[:50]}... | E: {bool(lead['email'])} | T: {bool(lead['telefono'])}")
            
            return lead
            
        except Exception as e:
            if page:
                await page.close()
            
            if attempt == MAX_RETRIES - 1:
                logger.warning(f"✗ Errore scraping {url[:50]}... dopo {MAX_RETRIES} tentativi: {e}")
                lead["status"] = "failed"
                return lead
            
            await asyncio.sleep(1)
    
    return lead

async def get_real_leads(query: str) -> List[Dict]:
    """Funzione principale - ottimizzata e con migliore gestione errori"""
    try:
        # Step 1: Get landing pages
        landing_data = await get_real_landing_urls(query)
        
        if not landing_data:
            logger.warning("Nessuna landing page trovata")
            return []
        
        logger.info(f"Inizio scraping di {len(landing_data)} landing pages")
        
        # Step 2: Scrape leads in parallelo con semaforo
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
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
        
        # Filtra lead con successo
        successful_leads = [l for l in leads if l["status"] == "success"]
        logger.info(f"Scraping completato: {len(successful_leads)}/{len(leads)} leads con successo")
        
        return leads
        
    except Exception as e:
        logger.error(f"Errore critico in get_real_leads: {e}")
        return []