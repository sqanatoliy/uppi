"""
Головний Scrapy-павук для роботи з AE + SISTER.

Логіка:
- start():
    - чистить state.json та captcha_images
    - читає clients.yml
    - для тих, у кого візура вже є в БД і не FORCE_UPDATE_VISURA — не чіпає SISTER, просто yield UppiItem
    - для решти — додає в self.clients_to_fetch
    - якщо список не порожній — стартує Playwright-логін в AE

- login_and_fetch_visura():
    - на Playwright-сторінці логіниться в AE
    - відкриває SISTER у новій вкладці
    - для кожного клієнта з self.clients_to_fetch:
        - navigate_to_visure_catastali(...)
        - solve_captcha_if_present(...)
        - download_document(...)
        - yield UppiItem з прапорцями успіху/фейлу
    - наприкінці завжди робить logout (через кнопку або URL)
"""

import os
import shutil
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import scrapy
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from uppi.ae.auth import authenticate_user
from uppi.ae.captcha import solve_captcha_if_present
from uppi.ae.download import download_document
from uppi.ae.sister_navigation import open_sister_service, navigate_to_visure_catastali
from uppi.ae.uppi_selectors import UppiSelectors
from uppi.config import load_ae_config, load_clients, load_visura_policy_config
from uppi.domain.models import ClientConfig
from uppi.items import UppiItem
from uppi.services import DatabaseRepository, DbConnectionManager
from uppi.services.storage_minio import StorageService
from uppi.services.visura_fetcher import should_download_visura
from uppi.utils.playwright_helpers import apply_stealth, log_requests, get_webgl_vendor
from uppi.utils.stealth import STEALTH_SCRIPT

AE_CONFIG = load_ae_config()
VISURA_POLICY = load_visura_policy_config()

AE_LOGIN_URL = AE_CONFIG.login_url
AE_URL_SERVIZI = AE_CONFIG.servizi_url
SISTER_LOGOUT_URL = AE_CONFIG.logout_url

TWO_CAPTCHA_API_KEY = AE_CONFIG.two_captcha_api_key
AE_USERNAME = AE_CONFIG.username
AE_PASSWORD = AE_CONFIG.password
AE_PIN = AE_CONFIG.pin


class UppiSpider(scrapy.Spider):
    name = "uppi"
    allowed_domains = ["agenziaentrate.gov.it"]

    # Тут складатимемо клієнтів, для яких треба реально йти в SISTER
    clients_to_fetch: List[ClientConfig]

    async def start(self):
        """
        Стартова точка павука (Scrapy 2.13 async start).

        - видаляє старий state.json + captcha_images
        - завантажує клієнтів
        - вирішує, для кого потрібен SISTER, а для кого ні
        - якщо SISTER потрібен хоча б для одного — стартує Playwright-логін
        """
        self.logger.info("[START] UppiSpider starting...")

        # Чистимо старий state.json
        self.logger.info("[START] Cleaning old state.json if present")
        try:
            if os.path.exists("state.json"):
                os.remove("state.json")
                self.logger.info("[START] Old state.json removed")
        except Exception as e:
            self.logger.warning("[START] Failed to remove state.json: %s", e)

        # Чистимо папку captcha_images
        self.logger.info("[START] Cleaning old captcha_images folder if present")
        try:
            if os.path.exists("captcha_images"):
                shutil.rmtree("captcha_images")
                self.logger.info("[START] Old captcha_images folder removed")
        except Exception as e:
            self.logger.warning("[START] Failed to remove captcha_images folder: %s", e)

        # Завантажуємо клієнтів з clients.yml
        clients = load_clients()
        if not clients:
            self.logger.error("[START] No clients found in clients.yml, aborting spider")
            return

        self.clients_to_fetch = []
        self.logger.info("[START] Loaded %d clients from clients.yml", len(clients))

        repo = DatabaseRepository(ae_username=AE_USERNAME, template_version="")
        storage = StorageService()

        # Вирішуємо, кого потрібно качати з SISTER
        with DbConnectionManager() as conn:
            for client in clients:
                cf = client.locatore_cf

                try:
                    db_state = repo.fetch_visura_metadata(conn, cf)
                except Exception as e:
                    self.logger.exception("[DB] Error checking visura presence for %s: %s", cf, e)
                    db_state = None

                try:
                    storage_exists = storage.object_exists(
                        storage.config.visure_bucket,
                        storage.visura_object_name(cf),
                    ) if db_state else False
                except Exception as e:
                    self.logger.warning("[S3] Error checking visura object for %s: %s", cf, e)
                    storage_exists = False

                decision = should_download_visura(
                    force_update=client.force_update_visura,
                    ttl_days=VISURA_POLICY.ttl_days,
                    db_state=db_state,
                    storage_exists=storage_exists,
                )

                if not decision.should_download:
                    self.logger.info(
                        "[START] Skip SISTER for %s, cache ok (%s)",
                        cf,
                        decision.reason,
                    )
                    mapped = asdict(client)
                    mapped.setdefault("locatore_cf", cf)
                    mapped.setdefault("visura_source", "db_cache")
                    mapped.setdefault("visura_needs_refresh", False)
                    mapped.setdefault("visura_downloaded", False)
                    mapped.setdefault("visura_download_path", None)
                    mapped.setdefault("nav_to_visure_catastali", False)
                    mapped.setdefault("captcha_ok", False)

                    yield UppiItem(**mapped)
                else:
                    self.logger.info(
                        "[START] Will fetch visura from SISTER for %s (reason=%s)",
                        cf,
                        decision.reason,
                    )
                    self.clients_to_fetch.append(client)

        if not self.clients_to_fetch:
            self.logger.info("[START] No clients require SISTER fetch. Spider finished.")
            return

        self.logger.info("[START] %d clients require SISTER fetch", len(self.clients_to_fetch))

        # Стартуємо Playwright-логін у AE
        yield scrapy.Request(
            url=AE_LOGIN_URL,
            callback=self.login_and_fetch_visura,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_context": "default",
            },
            errback=self.errback_close_page,
            dont_filter=True,
        )

    async def login_and_fetch_visura(self, response):
        """
        Playwright-callback:
        - логін в AE
        - відкриття SISTER у новій вкладці
        - цикл по self.clients_to_fetch: навігація, CAPTCHA, download
        - logout у фіналі
        """
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[LOGIN] No Playwright page in response.meta, cannot continue")
            return

        # Pre-navigation setup: stealth, логування запитів, WebGL
        try:
            await apply_stealth(page, STEALTH_SCRIPT)
            await page.route("**", log_requests)
            vendor = await get_webgl_vendor(page)
            self.logger.debug("[LOGIN] WebGL vendor: %s", vendor)
        except Exception as e:
            self.logger.warning("[LOGIN] Pre-navigation setup failed: %s", e)

        # Логін у AE
        login_ok = False
        try:
            login_ok = await authenticate_user(
                page=page,
                ae_username=AE_USERNAME,
                ae_password=AE_PASSWORD,
                ae_pin=AE_PIN,
                logger=self.logger,
            )
        except PlaywrightTimeoutError as err:
            self.logger.error("[LOGIN] Playwright timeout during login: %s", err)
        except Exception as e:
            self.logger.exception("[LOGIN] Unexpected error during login: %s", e)

        if not login_ok:
            self.logger.error("[LOGIN] Login failed, aborting SISTER flow")
            await self.safe_close_page(page, "login_failed")
            return

        # Відкриваємо SISTER у новій вкладці
        sister_page: Optional[Page] = None
        try:
            sister_page = await open_sister_service(
                ae_page=page,
                servizi_url=AE_URL_SERVIZI,
                logger=self.logger,
                safe_close_page=self.safe_close_page,
            )
        except Exception as e:
            self.logger.exception("[SISTER] Error while opening SISTER service: %s", e)

        if not sister_page:
            self.logger.error("[SISTER] Could not obtain SISTER page, aborting")
            # На цей момент AE-сторінка могла вже закритися в open_sister_service,
            # тому на всяк випадок пробуємо її закрити ще раз
            await self.safe_close_page(page, "login_page_after_failed_sister")
            return

        # Основний цикл по клієнтах
        try:
            total = len(self.clients_to_fetch)
            for idx, client in enumerate(self.clients_to_fetch, start=1):
                cf = client.locatore_cf
                comune = client.comune or "PESCARA"
                tipo_catasto = client.tipo_catasto or "F"
                ufficio_label = client.ufficio_label or "PESCARA Territorio"

                self.logger.info(
                    "[CLIENT %d/%d] Processing CF=%s, comune=%s, tipo_catasto=%s, ufficio=%s",
                    idx,
                    total,
                    cf,
                    comune,
                    tipo_catasto,
                    ufficio_label,
                )

                mapped = asdict(client)
                mapped.setdefault("locatore_cf", cf)
                mapped["visura_source"] = "sister"
                mapped["visura_needs_refresh"] = False

                # 1. Навігація до форми і запуск "Visura per soggetto"
                nav_ok = await navigate_to_visure_catastali(
                    sister_page=sister_page,
                    codice_fiscale=cf,
                    comune=comune,
                    tipo_catasto=tipo_catasto,
                    ufficio_label=ufficio_label,
                    logger=self.logger,
                )
                mapped["nav_to_visure_catastali"] = bool(nav_ok)

                if not nav_ok:
                    self.logger.warning(
                        "[CLIENT %d/%d] Navigation to Visure catastali failed for %s",
                        idx,
                        total,
                        cf,
                    )
                    mapped["captcha_ok"] = False
                    mapped["visura_downloaded"] = False
                    mapped["visura_download_path"] = None
                    yield UppiItem(**mapped)
                    continue

                # 2. Обробка CAPTCHA (якщо є)
                captcha_ok = await solve_captcha_if_present(
                    page=sister_page,
                    two_captcha_key=TWO_CAPTCHA_API_KEY,
                    logger=self.logger,
                    codice_fiscale=cf,
                )
                mapped["captcha_ok"] = bool(captcha_ok)

                if not captcha_ok:
                    self.logger.warning(
                        "[CLIENT %d/%d] CAPTCHA solving failed for %s",
                        idx,
                        total,
                        cf,
                    )
                    mapped["visura_downloaded"] = False
                    mapped["visura_download_path"] = None
                    yield UppiItem(**mapped)
                    continue

                # 3. Завантаження PDF-візури
                download_path = await download_document(
                    page=sister_page,
                    codice_fiscale=cf,
                    logger=self.logger,
                )
                mapped["visura_downloaded"] = download_path is not None
                mapped["visura_download_path"] = download_path

                if not download_path:
                    self.logger.error(
                        "[CLIENT %d/%d] Download failed for %s",
                        idx,
                        total,
                        cf,
                    )
                else:
                    self.logger.info(
                        "[CLIENT %d/%d] Downloaded visura for %s -> %s",
                        idx,
                        total,
                        cf,
                        download_path,
                    )

                # Віддаємо item у pipeline
                yield UppiItem(**mapped)

        finally:
            # Гарантований logout з SISTER (через UI або endpoint)
            try:
                if sister_page:
                    await self._logout_in_context(
                        context=sister_page.context,
                        via_ui=True,
                        close_context=True,
                    )
            except Exception as e:
                self.logger.warning("[LOGOUT] Error during logout_in_context: %s", e)

            # На всяк випадок пробуємо закрити сторінку
            await self.safe_close_page(sister_page, "sister_final")

    async def safe_close_page(self, page: Optional[Page], label: str = ""):
        """
        Безпечне закриття Playwright-сторінки з логуванням.
        Не кидає помилки, якщо сторінка вже закрита чи None.
        """
        if not page:
            self.logger.debug("[CLOSE] No page to close (%s)", label)
            return

        try:
            await page.close()
            self.logger.debug("[CLOSE] Page closed (%s)", label)
        except Exception as e:
            self.logger.warning("[CLOSE] Failed to close page (%s): %s", label, e)

    async def _logout_in_context(
        self,
        context,
        via_ui: bool = True,
        close_context: bool = False,
    ) -> bool:
        """
        Універсальний logout в рамках Playwright-контексту.

        - via_ui=True: спробує клікнути по кнопці 'Esci' (якщо є), інакше — перехід на SISTER_LOGOUT_URL.
        - якщо в контексті немає відкритих сторінок — створює тимчасову сторінку, йде на logout URL і закриває її.
        - close_context=True: закриває context в кінці.

        Повертає:
            True  - якщо була зроблена спроба logout (не обов'язково успішна)
            False - якщо сталася неочікувана помилка
        """
        page: Optional[Page] = None
        created_temp = False

        try:
            pages = context.pages

            # Якщо сторінок немає — створимо тимчасову для logout endpoint
            if not pages:
                created_temp = True
                self.logger.warning(
                    "[LOGOUT] No open pages in context, creating temporary page for endpoint logout"
                )
                page = await context.new_page()
                try:
                    await page.goto(SISTER_LOGOUT_URL, wait_until="networkidle", timeout=8_000)
                    self.logger.info("[LOGOUT] Navigated to logout endpoint (temp page)")
                    await page.wait_for_timeout(600)
                except Exception as e:
                    self.logger.debug(
                        "[LOGOUT] goto logout endpoint (temp page) failed or timed out: %s", e
                    )
                return True

            # Беремо останню сторінку в контексті
            page = pages[-1]
            ui_success = False

            # 1) Пробуємо logout через UI, якщо дозволено
            if via_ui:
                try:
                    await page.wait_for_selector(
                        UppiSelectors.ESCI_SISTER_BUTTON,
                        timeout=5_000,
                    )
                    await page.click(UppiSelectors.ESCI_SISTER_BUTTON)
                    self.logger.info("[LOGOUT] Clicked 'Esci' button (UI)")
                    await page.wait_for_timeout(1_000)
                    ui_success = True
                except PlaywrightTimeoutError:
                    self.logger.debug(
                        "[LOGOUT] 'Esci' button not found, falling back to endpoint logout"
                    )
                except Exception as e:
                    self.logger.debug(
                        "[LOGOUT] 'Esci' click failed (%s): %s",
                        type(e).__name__,
                        e,
                    )

            # 2) Якщо через UI не вдалось — йдемо на endpoint
            if not ui_success:
                try:
                    await page.goto(SISTER_LOGOUT_URL, wait_until="networkidle", timeout=8_000)
                    # LOGOUT_BUTTON тут умовний маркер, може не з'явитись — не критично
                    try:
                        await page.wait_for_selector(UppiSelectors.LOGOUT_BUTTON, timeout=8_000)
                        self.logger.info(
                            "[LOGOUT] Navigated to logout endpoint and LOGOUT_BUTTON appeared (fallback)"
                        )
                    except PlaywrightTimeoutError:
                        self.logger.info(
                            "[LOGOUT] Logout endpoint opened but LOGOUT_BUTTON not detected (fallback)"
                        )
                    await page.wait_for_timeout(600)
                except Exception as e:
                    self.logger.debug(
                        "[LOGOUT] goto logout endpoint (fallback) failed or timed out: %s", e
                    )

            return True

        except Exception as e:
            self.logger.exception("[LOGOUT] Unexpected error in logout helper: %s", e)
            return False
        finally:
            # Закриваємо тимчасову сторінку, якщо створювали
            if created_temp and page:
                try:
                    await page.close()
                    self.logger.debug("[LOGOUT] Temporary logout page closed")
                except Exception:
                    pass

            # Закриваємо контекст, якщо просили
            if close_context:
                try:
                    await context.close()
                    self.logger.info("[LOGOUT] Context closed")
                except Exception as e:
                    self.logger.warning("[LOGOUT] Failed to close context: %s", e)

    async def errback_close_page(self, failure):
        """
        Errback для Scrapy-запитів з Playwright:
        - якщо є сторінка — робить logout через її context і закриває сторінку,
        - якщо немає — створює окремий Request на logout URL з Playwright, щоб звільнити серверну сесію.
        """
        page: Optional[Page] = None
        try:
            page = failure.request.meta.get("playwright_page")
        except Exception:
            page = None

        if page:
            try:
                await self._logout_in_context(
                    context=page.context,
                    via_ui=True,
                    close_context=False,
                )
            except Exception as e:
                self.logger.warning("[ERRBACK] Logout via page.context failed: %s", e)
            finally:
                await self.safe_close_page(page, "errback")
                return

        # Якщо сторінки немає — плануємо окремий Request для відкриття нової сторінки з logout URL
        try:
            logout_full_url = SISTER_LOGOUT_URL
            req = scrapy.Request(
                url=logout_full_url,
                callback=self._logout_callback,
                errback=None,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_context": "default",
                },
                dont_filter=True,
            )
            self.crawler.engine.crawl(req, spider=self)
            self.logger.info(
                "[ERRBACK] Scheduled logout request with Playwright to free server session (no page present)"
            )
        except Exception as e:
            self.logger.error("[ERRBACK] Could not schedule logout request: %s", e)

    async def _logout_callback(self, response):
        """
        Callback для logout-request, який автоматично створює Playwright page.

        Тут ми вже на logout URL, але все одно проходимо через _logout_in_context(),
        щоб логіка була однакова.
        """
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[LOGOUT_CB] No playwright_page in logout callback")
            return

        try:
            await self._logout_in_context(
                context=page.context,
                via_ui=False,   # тут UI не потрібен, ми вже на endpoint
                close_context=False,
            )
        finally:
            await self.safe_close_page(page, "logout_callback")
