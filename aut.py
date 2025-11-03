import os
import re
import asyncio
import signal
import logging
from dataclasses import dataclass
from typing import List, Dict, Set, Optional
from urllib.parse import urlparse, parse_qs, unquote, urlencode, quote_plus

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ---------------- CONFIG ----------------
COUNTRY = os.getenv("COUNTRY", "IT")
EXCLUDE_DOMAINS = {
    "facebook.com", "fb.com", "fb.me", "fbcdn.net",
    "instagram.com", "instagr.am", "whatsapp.com", "tinyurl.com",
    "bit.ly", "metastatus.com", "static.xx.fbcdn.net"
}

# Performance tuning - RENDER OPTIMIZED (env-overridable)
MAX_CONCURRENT_PAGES = int(os.getenv("MAX_CONCURRENT_PAGES", "2"))
SCROLL_COUNT = int(os.getenv("SCROLL_COUNT", "4"))
SCROLL_WAIT = int(os.getenv("SCROLL_WAIT_MS", "2000"))
PAGE_TIMEOUT = int(os.getenv("PAGE_TIMEOUT_MS", "25000"))
INITIAL_WAIT = int(os.getenv("INITIAL_WAIT_MS", "5000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))

# Logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("render-playwright-scraper")

# Graceful shutdown flag
_shutdown_event = asyncio.Event()


# ---------------- REGEX PRECOMPILATI ----------------
EMAIL_RE = re.compile(r'\b[a-zA-Z0-9][a-zA-Z0-9._%+-]{0,63}@[a-zA-Z0-9][a-zA-Z0-9.-]{0,253}\.[a-zA-Z]{2,}\b')
MAILTO_RE = re.compile(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', re.IGNORECASE)
TEL_RE = re.compile(r'tel:([+\d\s\-\(\)]+)', re.IGNORECASE)

PHONE_PATTERNS = [
    re.compile(r'\+39[\s\-]?\d{2,3}[\s\-]?\d{6,7}'),
    re.compile(r'0\d{1,3}[\s\-]?\d{6,8}'),
    re.compile(r'\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{6,10}'),
    re.compile(r'\(\+39\)[\s\-]?\d{9,10}'),
]

INVALID_EMAIL_SNIPPETS = [
    'example.com', 'test.com', 'dummy', 'placeholder',
    'noreply', 'no-reply', 'youremail', 'your-email',
    'email@', '@email', 'info@info', 'admin@admin',
    'sample', 'fake', 'tempmail'
]


# ---------------- DATACLASS ----------------
@dataclass
class Lead:
    landing_page: str
    ad_link: str
    email: Optional[str]
    telefono: Optional[str]
    copy_valutazione: Optional[str]
    status: str


# ---------------- SIGNAL HANDLERS ----------------
def _handle_sigterm(signum, frame):
    logger.info("SIGTERM ricevuto: avvio shutdown graceful...")
    _shutdown_event.set()

signal.signal(signal.SIGTERM, _handle_sigterm)


# ---------------- UTILS ----------------
def normalize_url(url: str) -> str:
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
                new_query = urlencode(filtered, doseq=True)
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{new_query}"
            else:
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        pass
    return url


def extract_real_url(link: str) -> Optional[str]:
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


def should_exclude_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(d in host for d in EXCLUDE_DOMAINS)


def validate_email(email: str) -> bool:
    email = email.lower().strip()
    if any(sn in email for sn in INVALID_EMAIL_SNIPPETS):
        return False
    if not EMAIL_RE.match(email):
        return False
    if re.search(r'[._%+-]{3,}', email):
        return False
    return True


def validate_phone(phone: str) -> bool:
    clean = re.sub(r'[\s\-\(\)\.]', '', phone)
    if not re.match(r'^\+?\d{9,15}$', clean):
        return False
    if re.match(r'^(\d)\1+$', clean) or clean in ['123456789', '987654321', '1234567890']:
        return False
    if re.search(r'(\d{3,})\1', clean):
        return False
    return True


async def goto_with_retries(page: Page, url: str, retries: int = MAX_RETRIES) -> None:
    last_err = None
    for attempt in range(retries + 1):
        if _shutdown_event.is_set():
            raise asyncio.CancelledError()
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            return
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.8 * (attempt + 1))
    raise last_err


# ---------------- BROWSER/CONTEXT ----------------
async def launch_browser_and_context():
    pw = await async_playwright().start()

    launch_kwargs = dict(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",    # su container /dev/shm piccolo
            "--no-sandbox",               # richiesto se sandbox non disponibile
            "--no-zygote",
            "--disable-gpu",
            "--disable-extensions",
            "--mute-audio",
        ]
    )
    # NESSUN proxy configurato (nessuna variabile né opzione passata)

    browser: Browser = await pw.chromium.launch(**launch_kwargs)

    context: BrowserContext = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"),
        viewport={'width': 1920, 'height': 1080},
        locale='it-IT',
        timezone_id='Europe/Rome',
        java_script_enabled=True,
        service_workers="block",  # consigliato quando si intercettano richieste
    )
    context.set_default_timeout(PAGE_TIMEOUT)

    # Routing globale: blocca risorse pesanti (immagini, CSS, font, media)
    async def route_blocker(route):
        req = route.request
        rtype = req.resource_type
        if rtype in ("image", "media", "font", "stylesheet"):
            return await route.abort()
        url = req.url
        if any(x in url for x in ("google-analytics.com", "doubleclick.net")):
            return await route.abort()
        return await route.continue_()

    # Applica il routing a tutto il context così vale per nuove pagine e popup
    await context.route("**/*", route_blocker)

    return pw, browser, context


# ---------------- SCRAPING ----------------
async def resolve_shortlink(context: BrowserContext, link: str) -> Optional[str]:
    page = await context.new_page()
    try:
        await goto_with_retries(page, link, retries=MAX_RETRIES)
        if page.response():
            return page.response().url
        return page.url
    except Exception as e:
        logger.debug(f"Errore risoluzione shortlink {link}: {e}")
        return None
    finally:
        await page.close()


async def get_real_landing_urls(context: BrowserContext, query: str) -> List[Dict[str, str]]:
    page = await context.new_page()
    landing_pages: List[Dict[str, str]] = []
    try:
        search_url = (
            "https://www.facebook.com/ads/library/"
            f"?active_status=all&ad_type=all&country={COUNTRY}&q={quote_plus(query)}"
        )
        logger.info(f"🔍 Query: {query}")

        await goto_with_retries(page, search_url, retries=MAX_RETRIES)

        logger.info("⏳ Attesa caricamento iniziale...")
        await page.wait_for_timeout(INITIAL_WAIT)

        try:
            await page.wait_for_selector('div[role="main"]', timeout=10000)
            logger.info("✓ Pagina caricata")
        except Exception:
            logger.warning("⚠️ Timeout pagina principale, continuo...")

        logger.info("📜 Inizio scroll...")
        for i in range(SCROLL_COUNT):
            if _shutdown_event.is_set():
                break
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(SCROLL_WAIT)
            logger.info(f"  Scroll {i+1}/{SCROLL_COUNT}")

        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(1200)

        stats = await page.evaluate("""
            () => ({
                totalLinks: document.querySelectorAll('a[href]').length,
                fbLinks: document.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]').length,
                adIdLinks: document.querySelectorAll('a[href*="ads/library/?id="]').length,
                bodyLength: document.body.innerHTML.length
            })
        """)
        logger.info(f"📊 Links: {stats['totalLinks']} | FB: {stats['fbLinks']} | AdID: {stats['adIdLinks']} | HTML: {stats['bodyLength']} chars")

        ads_data = await page.evaluate("""
            () => {
                const results = [];
                const seen = new Set();

                // METODO 1: da link dettaglio annuncio
                const detailLinks = document.querySelectorAll('a[href*="ads/library/?id="]');
                detailLinks.forEach(detailLink => {
                    const adIdMatch = detailLink.href.match(/id=(\\d+)/);
                    if (!adIdMatch) return;
                    const adUrl = `https://www.facebook.com/ads/library/?id=${adIdMatch[1]}`;

                    let container = detailLink.closest('div[class*="x1"]') || detailLink.closest('[role="article"]');
                    if (!container) {
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

                // METODO 2: per ciascun landing link risali all'Ad ID
                const allLandings = document.querySelectorAll('a[href*="l.facebook.com"], a[href*="fb.me"]');
                allLandings.forEach(link => {
                    if (seen.has(link.href)) return;

                    let adUrl = null;
                    let parent = link.parentElement;
                    for (let level = 0; level < 25 && parent; level++) {
                        const adLink = parent.querySelector('a[href*="ads/library/?id="]');
                        if (adLink) {
                            const match = adLink.href.match(/id=(\\d+)/);
                            if (match) {
                                adUrl = `https://www.facebook.com/ads/library/?id=${match[1]}`;
                                break;
                            }
                        }

                        const html = parent.innerHTML;
                        const htmlMatch = html.match(/ads\\/library\\/\\?id=(\\d+)/);
                        if (htmlMatch) {
                            adUrl = `https://www.facebook.com/ads/library/?id=${htmlMatch[1]}`;
                            break;
                        }

                        const text = parent.innerText || parent.textContent || '';
                        const textMatch = text.match(/(?:ID libreria|Library ID|Ad ID)[\\s:]+(\\d+)/i);
                        if (textMatch) {
                            adUrl = `https://www.facebook.com/ads/library/?id=${textMatch[1]}`;
                            break;
                        }

                        parent = parent.parentElement;
                    }

                    seen.add(link.href);
                    results.push({ landing: link.href, ad_url: adUrl });
                });

                // METODO 3: fallback globale
                if (results.every(r => !r.ad_url)) {
                    const bodyHtml = document.body.innerHTML;
                    const allAdIds = [...bodyHtml.matchAll(/ads\\/library\\/\\?id=(\\d+)/g)];
                    if (allAdIds.length > 0) {
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

        for i, item in enumerate(ads_data[:2]):
            ad_status = "✓" if item['ad_url'] else "✗"
            logger.info(f"{ad_status} Esempio {i+1}: {item['landing'][:60]}...")
            if item['ad_url']:
                logger.info(f"   Ad: {item['ad_url'][:70]}...")

        seen_urls: Set[str] = set()
        for item in ads_data:
            if _shutdown_event.is_set():
                break

            landing_link = item['landing']
            ad_url = item['ad_url']

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
        return landing_pages

    except Exception as e:
        logger.error(f"❌ Errore scraping: {e}")
        return []
    finally:
        await page.close()


async def scrape_single_lead(context: BrowserContext, url: str, ad_link: str) -> Dict:
    lead = Lead(
        landing_page=url,
        ad_link=ad_link,
        email=None,
        telefono=None,
        copy_valutazione=None,
        status="error"
    )

    page: Optional[Page] = None
    try:
        page = await context.new_page()

        await goto_with_retries(page, url, retries=MAX_RETRIES)
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
        found_emails = list(dict.fromkeys(EMAIL_RE.findall(full_content)))
        valid_emails = [e for e in found_emails if validate_email(e)]
        if valid_emails:
            lead.email = valid_emails[0]

        # PHONE
        found_phones = []
        for pattern in PHONE_PATTERNS:
            found_phones.extend(pattern.findall(full_content))
        found_phones = list(dict.fromkeys(found_phones))
        valid_phones = [p for p in found_phones if validate_phone(p)]
        if valid_phones:
            lead.telefono = valid_phones[0].strip()

        # MAILTO/TEL fallback
        if not lead.email:
            mailto = MAILTO_RE.findall(full_content)
            valid_mailto = [e for e in mailto if validate_email(e)]
            if valid_mailto:
                lead.email = valid_mailto[0]

        if not lead.telefono:
            tel = TEL_RE.findall(full_content)
            valid_tel = [t for t in tel if validate_phone(t)]
            if valid_tel:
                lead.telefono = valid_tel[0].strip()

        # COPY scoring
        keywords_strong = ["gratis", "gratuito", "free", "sconto", "offerta", "promo", "risparmia", "omaggio"]
        keywords_medium = ["lezione", "webinar", "corso", "training", "consulenza", "demo", "prova"]
        keywords_weak = ["scopri", "impara", "migliora", "garantito"]

        strong_count = sum(1 for w in keywords_strong if w in text_lower)
        medium_count = sum(1 for w in keywords_medium if w in text_lower)
        weak_count = sum(1 for w in keywords_weak if w in text_lower)

        if strong_count >= 3:
            lead.copy_valutazione = "Copy molto interessante (alto incentivo)"
        elif strong_count >= 2 or (strong_count >= 1 and medium_count >= 2):
            lead.copy_valutazione = "Copy molto interessante"
        elif strong_count >= 1 or medium_count >= 2:
            lead.copy_valutazione = "Copy interessante"
        elif medium_count >= 1 or weak_count >= 2:
            lead.copy_valutazione = "Copy discreto"
        else:
            lead.copy_valutazione = "Copy standard"

        lead.status = "success"
        status = "✓" if (lead.email or lead.telefono) else "○"
        logger.info(f"{status} {url[:40]}... | E:{bool(lead.email)} T:{bool(lead.telefono)}")

    except Exception:
        logger.warning(f"✗ {url[:40]}... (timeout/error)")
        lead.status = "timeout"
    finally:
        if page:
            await page.close()

    return {
        "landing_page": lead.landing_page,
        "ad_link": lead.ad_link,
        "email": lead.email,
        "telefono": lead.telefono,
        "copy_valutazione": lead.copy_valutazione,
        "status": lead.status
    }


async def get_real_leads(query: str) -> List[Dict]:
    if _shutdown_event.is_set():
        return []

    pw = browser = context = None
    try:
        pw, browser, context = await launch_browser_and_context()

        landing_data = await get_real_landing_urls(context, query)
        if not landing_data:
            logger.warning("❌ Nessuna landing trovata")
            return []

        logger.info(f"🚀 Scraping {len(landing_data)} landing pages...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

        async def scrape_with_limit(data):
            async with semaphore:
                if _shutdown_event.is_set():
                    return {
                        "landing_page": data['url'],
                        "ad_link": data['ad_link'],
                        "email": None,
                        "telefono": None,
                        "copy_valutazione": None,
                        "status": "cancelled"
                    }
                return await scrape_single_lead(context, data['url'], data['ad_link'])

        leads = await asyncio.gather(*[scrape_with_limit(d) for d in landing_data])

        ok = sum(1 for l in leads if l["status"] == "success")
        timeout = sum(1 for l in leads if l["status"] == "timeout")
        logger.info(f"✅ {ok} OK | ⏱️ {timeout} timeout | 📊 {len(leads)} totali")

        return leads

    except asyncio.CancelledError:
        logger.warning("Operazione annullata per shutdown.")
        return []
    except Exception as e:
        logger.error(f"❌ Errore critico: {e}")
        return []
    finally:
        try:
            if context:
                await context.close()
            if browser:
                await browser.close()
        finally:
            if pw:
                await pw.stop()


# Esempio di esecuzione:
# asyncio.run(get_real_leads("consulenza fiscale"))
