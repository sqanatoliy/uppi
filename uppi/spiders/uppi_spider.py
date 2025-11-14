import os
import shutil
import csv
import scrapy
from typing import Optional
from decouple import config
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
from uppi.utils.stealth import STEALTH_SCRIPT
from uppi.utils.selectors import UppiSelectors
from uppi.utils.captcha_solver import solve_captcha
from uppi.utils.playwright_helpers import apply_stealth, log_requests, get_webgl_vendor


AE_LOGIN_URL = config("AE_LOGIN_URL")
AE_URL_SERVIZI = config("AE_URL_SERVIZI")
SISTER_VISURE_CATASTALI_URL = config("SISTER_VISURE_CATASTALI_URL")
SISTER_LOGOUT_URL = config("SISTER_LOGOUT_URL")

TWO_CAPTCHA_API_KEY = config("TWO_CAPTCHA_API_KEY")
AE_USERNAME = config("AE_USERNAME")
AE_PASSWORD = config("AE_PASSWORD")
AE_PIN = config("AE_PIN")


class UppiSpider(scrapy.Spider):
    name = "uppi"
    allowed_domains = ["agenziaentrate.gov.it"]

    DEFAULT_COMUNE: str = "PESCARA"
    DEFAULT_TIPO_CATASTO: str = "F"
    DEFAULT_UFFICIO: str = "PESCARA Territorio"

    def load_clients(self, path: str = "clients/clients.csv"):
        """Load clients from CSV file. Returns list of client dicts."""
        clients = []
        if not os.path.exists(path):
            self.logger.error(f"[CLIENTS] File not found: {path}")
            return clients

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                codice = row.get("CODICE_FISCALE", "").strip()
                if not codice:
                    continue
                client = {
                    "CODICE_FISCALE": codice,
                    "COMUNE": row.get("COMUNE", "").strip() or self.DEFAULT_COMUNE,
                    "TIPO_CATASTO": row.get("TIPO_CATASTO", "").strip() or self.DEFAULT_TIPO_CATASTO,
                    "UFFICIO_PROVINCIALE_LABEL": row.get("UFFICIO_PROVINCIALE_LABEL", "").strip() or self.DEFAULT_UFFICIO,
                }
                clients.append(client)
        self.logger.info(f"[CLIENTS] Loaded {len(clients)} clients from {path}")
        return clients

    async def start(self):
        """Entry point: remove stale session file and start login request."""
        self.logger.info("[START] Cleaning old state.json if present")
        try:
            if os.path.exists("state.json"):
                os.remove("state.json")
                self.logger.info("[START] Old state.json removed")
        except Exception as e:
            self.logger.warning("[START] Failed to remove state.json: %s", e)
        self.logger.info("[START] Cleaning old captcha_images folder if present")
        try:
            if os.path.exists("captcha_images"):
                shutil.rmtree("captcha_images")
                self.logger.info("[START] Old captcha_images folder removed")
        except Exception as e:
            self.logger.warning("[START] Failed to remove captcha_images folder: %s", e)

        yield scrapy.Request(
            url=AE_LOGIN_URL,
            callback=self.login,
            meta={
                "playwright": True,
                "playwright_context": "default",
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def login(self, response):
        """
        Login flow.
        - apply stealth
        - fill credentials
        - wait for PROFILE_INFO selector
        - always close original page at the end of this function
        """
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[LOGIN] No Playwright page in response.meta")
            return

        try:
            await apply_stealth(page, STEALTH_SCRIPT)
            await page.route("**", log_requests)

            # Wait and interact with UI
            await page.wait_for_selector(UppiSelectors.FISCOLINE_TAB)
            await page.click(UppiSelectors.FISCOLINE_TAB)

            await page.wait_for_selector(UppiSelectors.USERNAME_FIELD, timeout=10_000)
            await page.wait_for_timeout(1_000)
            await page.fill(UppiSelectors.USERNAME_FIELD, AE_USERNAME)
            await page.fill(UppiSelectors.PASSWORD_FIELD, AE_PASSWORD)
            await page.fill(UppiSelectors.PIN_FIELD, AE_PIN)
            await page.click(UppiSelectors.ACCEDI_BUTON)
            self.logger.info("[LOGIN] Accedi clicked, awaiting profile info")

            try:
                await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
                self.logger.info("[LOGIN] Login successful. profile info found")
            except PlaywrightTimeoutError as err:
                self.logger.error("[LOGIN] Profile not found after login: %s", err)
                # if state.json exists remove it as login failed to produce valid state
                if os.path.exists("state.json"):
                    try:
                        os.remove("state.json")
                        self.logger.info("[LOGIN] Removed leftover state.json after failed login")
                    except Exception as rm_err:
                        self.logger.warning("[LOGIN] Failed remove leftover state.json: %s", rm_err)
            # continue regardless; next request will attempt to use existing session if any
        except PlaywrightTimeoutError as err:
            self.logger.error("[LOGIN] Playwright timeout during login: %s", err)
        except Exception as e:
            self.logger.exception("[LOGIN] Unexpected error during login: %s", e)
        finally:
            try:
                await self.safe_close_page(page, "login")
                self.logger.debug("[LOGIN] Playwright page closed")
            except Exception:
                # closing page might fail if page already closed; ignore
                pass

        # Continue to service parsing
        yield scrapy.Request(
            url=AE_URL_SERVIZI,
            callback=self.preparare_sister_service,
            meta={
                "playwright": True,
                "playwright_context": "default",
                "playwright_include_page": True,
            },
            errback=self.errback_close_page,
        )

    async def preparare_sister_service(self, response):
        """Prepare SISTER service page and process clients."""
        page: Optional[Page] = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[PARSE] No Playwright page provided")
            return

        # Pre-navigation setup - stealth, logging, WebGL info
        try:
            await apply_stealth(page, STEALTH_SCRIPT)
            await page.route("**", log_requests)
            vendor = await get_webgl_vendor(page)
            self.logger.debug("[PARSE] WebGL vendor: %s", vendor)
        except Exception as e:
            self.logger.warning("[PARSE] Pre-navigation setup failed: %s", e)

        # Open sister service and get sister_page
        sister_page = await self._open_sister_service(page)
        if not sister_page:
            self.logger.error("[PARSE] Could not open SISTER page. Aborting parse.")
            return

        clients = self.load_clients("clients/clients.csv")
        if not clients:
            self.logger.error("[PARSE] No clients found, aborting")
            await self._logout_in_context(page.context, via_ui=True)
            return

        try:
            for i, client in enumerate(clients, start=1):
                self.logger.info(f"[CLIENT {i}/{len(clients)}] Processing {client['CODICE_FISCALE']}")
                try:
                    ok = await self._navigate_to_visure_catastali(
                        sister_page,
                        codice_fiscale=client["CODICE_FISCALE"],
                        comune=client["COMUNE"],
                        tipo_catasto=client["TIPO_CATASTO"],
                        ufficio_label=client["UFFICIO_PROVINCIALE_LABEL"],
                    )
                    if not ok:
                        self.logger.warning(f"[CLIENT {i}] Navigation failed, skipping client")
                        continue

                    await self._solve_captcha_if_present(sister_page, client["CODICE_FISCALE"])
                    await self._download_document(sister_page, client["CODICE_FISCALE"])
                except Exception as e:
                    self.logger.exception(f"[CLIENT {i}] Error processing client: {e}")
                    continue  # move to next client

        finally:
            # Always logout at the very end
            self.logger.info("[PARSE] All clients processed. Logging out...")
            await self._logout_in_context(sister_page.context, via_ui=True)
            self.logger.info("[PARSE] Logout completed.")

    async def _open_sister_service(self, page: Page) -> Optional[Page]:
        """Open SISTER in a new page/tab and save state.json. Returns sister_page or None."""
        new_page_ctx = None
        sister_page: Optional[Page] = None
        try:
            await page.wait_for_selector(UppiSelectors.PROFILE_INFO, timeout=10_000)
            await page.wait_for_selector(UppiSelectors.TUOI_PREFERITI_SECTION, timeout=10_000)
            await page.locator(UppiSelectors.TUOI_PREFERITI_SECTION).click()
            # open new page via middle click and capture it
            try:
                async with page.context.expect_page() as ctx:
                    await page.click(UppiSelectors.VAI_AL_SERVIZIO_BUTTON, button="middle")
                new_page_ctx = ctx
                sister_page = await new_page_ctx.value
                await sister_page.bring_to_front()
                await self.safe_close_page(page, "AE page")  # close main AE page
                self.logger.info("[OPEN_SISTER] SISTER page opened and AE main closed")
            except PlaywrightTimeoutError as e:
                self.logger.warning("[OPEN_SISTER] Opening SISTER timed out: %s", e)
                return None
        except PlaywrightTimeoutError as e:
            self.logger.error("[OPEN_SISTER] Required selector not found before opening SISTER: %s", e)
            return None
        except Exception as e:
            self.logger.exception("[OPEN_SISTER] Unexpected error while opening SISTER: %s", e)
            return None

        # Accept confirmation and save storage state
        try:
            await sister_page.wait_for_selector(UppiSelectors.CONFERMA_BUTTON, timeout=10_000)
            await sister_page.wait_for_timeout(1_000)
            await sister_page.click(UppiSelectors.CONFERMA_BUTTON)
            await sister_page.wait_for_timeout(3_000)
            # store auth state to file for reuse
            try:
                await sister_page.context.storage_state(path="state.json")
                self.logger.info("[OPEN_SISTER] state.json saved")
            except Exception as e:
                self.logger.warning("[OPEN_SISTER] Failed to save state.json: %s", e)
        except PlaywrightTimeoutError as e:
            self.logger.warning("[OPEN_SISTER] Conferma button not found: %s", e)
            await self._logout_in_context(page.context, via_ui=True)

            # still return sister_page; maybe flow continues with different UI
        except Exception as e:
            self.logger.exception("[OPEN_SISTER] Error after opening SISTER: %s", e)

        return sister_page

    async def _navigate_to_visure_catastali(self, sister_page: Page,
                                            codice_fiscale: str,
                                            comune: str,
                                            tipo_catasto: str,
                                            ufficio_label: str) -> bool:
        """
        Make the series of clicks/selects inside SISTER to reach property list.
        Returns True on success, False on failure.
        """
        try:
            # await sister_page.click(UppiSelectors.CONSULTAZIONI_CERTIFACAZIONI)
            # await sister_page.wait_for_selector(UppiSelectors.VISURE_CATASTALI, timeout=10_000)
            # await sister_page.wait_for_timeout(1_000)
            # await sister_page.click(UppiSelectors.VISURE_CATASTALI)
            # await sister_page.wait_for_timeout(1_000)
            await sister_page.goto(SISTER_VISURE_CATASTALI_URL)
            # Handle possible "Conferma Lettura"
            try:
                await sister_page.wait_for_selector(UppiSelectors.CONFERMA_LETTURA, timeout=2_000)
                await sister_page.click(UppiSelectors.CONFERMA_LETTURA)
            except PlaywrightTimeoutError:
                self.logger.info("[NAVIGATE TO VISURE CATASTALI] Conferma Lettura not found, possibly already accepted")

            # Select ufficio
            select_ufficio = sister_page.locator(UppiSelectors.SELECT_UFFICIO)
            await select_ufficio.wait_for()
            await select_ufficio.select_option(label=ufficio_label)
            await sister_page.click(UppiSelectors.APLICA_BUTTON)

            # Select catasto
            select_catasto = sister_page.locator(UppiSelectors.SELECT_CATASTO)
            await select_catasto.wait_for()
            await sister_page.wait_for_timeout(1_000)
            await select_catasto.select_option(value=tipo_catasto)

            # Select comune
            select_comune = sister_page.locator(UppiSelectors.SELECT_COMUNE)
            await select_comune.wait_for()
            await sister_page.wait_for_timeout(1_000)
            await select_comune.select_option(label=comune)

            # Fill codice fiscale and search
            await sister_page.click(UppiSelectors.CODICE_FISCALE_RADIO)
            await sister_page.fill(UppiSelectors.CODICE_FISCALE_FIELD, codice_fiscale)
            await sister_page.click(UppiSelectors.RICERCA_BUTTON)

            try:
                # handle omonimi list and select first property
                await sister_page.wait_for_selector(UppiSelectors.SELECT_OMONIMI, timeout=3_000)
                await sister_page.click(UppiSelectors.SELECT_OMONIMI)
            except PlaywrightTimeoutError:
                self.logger.info("[NAVIGATE TO VISURE CATASTALI] Codice fiscale, maybe is invalid or no properties found")
                return False
            except Exception as e:
                self.logger.exception("[NAVIGATE TO VISURE CATASTALI] Error selecting omonimi: %s", e)
                return False

            # If you want to select property by Elenco immobili per diritti e quote instead Visura per soggetto, uncomment below and comment the visura per soggetto line
            # await sister_page.click(UppiSelectors.IMOBILI_BUTTON)
            # await sister_page.wait_for_selector(UppiSelectors.SELECT_IMOBILE, timeout=10_000)
            # await sister_page.wait_for_timeout(1_000)
            # await sister_page.click(UppiSelectors.SELECT_IMOBILE)
            # await sister_page.click(UppiSelectors.VISURA_PER_IMOBILE_BUTTON)

            # Proceed to visura per soggetto
            await sister_page.click(UppiSelectors.VISURA_PER_SOGGECTO_BUTTON)

            self.logger.info(f"[NAVIGATE] Completed for {codice_fiscale}")
            return True
        except PlaywrightTimeoutError as e:
            self.logger.warning("[NAVIGATE TO VISURE CATASTALI] Timeout during navigation: %s", e)
            return False
        except Exception as e:
            self.logger.exception("[NAVIGATE TO VISURE CATASTALI] Unexpected error during navigation: %s", e)
            return False

    async def _solve_captcha_if_present(self, sister_page: Page, codice_fiscale: str = ""):
        """Detect and solve CAPTCHA when present. Logs detailed status."""
        try:
            await sister_page.wait_for_selector(UppiSelectors.IMG_CAPTCHA, timeout=5_000)
        except PlaywrightTimeoutError:
            self.logger.info("[CAPTCHA] No CAPTCHA detected")
            await sister_page.click(UppiSelectors.INOLTRA_BUTTON)
            inoltra_button = sister_page.locator(UppiSelectors.INOLTRA_BUTTON)
            try:
                await inoltra_button.wait_for(state="hidden", timeout=10_000)
            except PlaywrightTimeoutError:
                # If it does not hide, still proceed and log
                self.logger.warning("[CAPTCHA] Inoltra button did not hide after submission")
            return

        # if we reach here, CAPTCHA element exists
        try:
            self.logger.info("[CAPTCHA] Captcha detected, invoking solver")
            await sister_page.click(UppiSelectors.CAPTCHA_FIELD)
            captcha_solution = await solve_captcha(sister_page, TWO_CAPTCHA_API_KEY, codice_fiscale, UppiSelectors.IMG_CAPTCHA)
            if not captcha_solution:
                self.logger.error("[CAPTCHA] Solver returned no solution")
                return

            await sister_page.fill(UppiSelectors.CAPTCHA_FIELD, captcha_solution)
            await sister_page.click(UppiSelectors.INOLTRA_BUTTON)
            inoltra_button = sister_page.locator(UppiSelectors.INOLTRA_BUTTON)
            try:
                await inoltra_button.wait_for(state="hidden", timeout=10_000)
            except PlaywrightTimeoutError:
                # If it does not hide, still proceed and log
                self.logger.warning("[CAPTCHA] Inoltra button did not hide after submission")
            self.logger.info("[CAPTCHA] CAPTCHA submitted")
        except PlaywrightTimeoutError as e:
            self.logger.warning("[CAPTCHA] Timeout while solving captcha: %s", e)
        except Exception as e:
            self.logger.exception("[CAPTCHA] Unexpected error in captcha handling: %s", e)

    async def _download_document(self, sister_page: Page, codice_fiscale: str):
        """Trigger document download and save file to downloads/ folder."""
        downloads_dir = os.path.join(os.getcwd(), f"downloads/{codice_fiscale}")
        os.makedirs(downloads_dir, exist_ok=True)

        download_ctx = None
        download_obj = None

        try:
            # attempt to expect a download and click the 'Apri' button
            async with sister_page.expect_download() as download_ctx:
                await sister_page.wait_for_selector(UppiSelectors.APRI_BUTTON, timeout=60_000)
                await sister_page.click(UppiSelectors.APRI_BUTTON)
                self.logger.info("[DOWNLOAD] Clicked Apri, waiting for download to appear")
            # retrieve download object
            download_obj = await download_ctx.value
        except PlaywrightTimeoutError as e:
            self.logger.warning("[DOWNLOAD] Waiting for download timed out: %s", e)
        except Exception as e:
            self.logger.exception("[DOWNLOAD] Unexpected error when initiating download: %s", e)

        if not download_obj:
            self.logger.error("[DOWNLOAD] No download object obtained. Aborting save.")
            return

        try:
            suggested = download_obj.suggested_filename
            download_path = os.path.join(downloads_dir, suggested)
            await download_obj.save_as(download_path)
            self.logger.info("[DOWNLOAD] File saved: %s", download_path)
        except Exception as e:
            self.logger.exception("[DOWNLOAD] Failed to save download: %s", e)

    async def safe_close_page(self, page: Optional[Page], label: str = ""):
        """Safely close a Playwright page with logging."""
        if not page:
            self.logger.debug(f"[CLOSE] No page to close {label}")
            return
        try:
            await page.close()
            self.logger.debug(f"[CLOSE] Page closed {label}")
        except Exception as e:
            self.logger.warning(f"[CLOSE] Failed to close page {label}: {e}")

    async def _logout_in_context(self, context, via_ui: bool = True, close_context: bool = False):
        """
        Universal logout within the given context.
        - via_ui=True: try clicking the 'Esci' button (if present), otherwise fallback -> goto endpoint.
        - close_context=True: closes the context at the end.
        Returns True if a logout attempt was made (at least some).
        """
        page = None
        created_temp = False
        try:
            pages = context.pages

            # If the context is empty, create a temporary page and simply go to the endpoint
            if not pages:
                created_temp = True
                self.logger.warning("[LOGOUT] No open pages in context, creating temporary one for endpoint logout")
                page = await context.new_page()
                try:
                    await page.goto(SISTER_LOGOUT_URL)
                    await page.wait_for_selector(UppiSelectors.LOGOUT_BUTTON, timeout=8_000)
                    self.logger.info("[LOGOUT] Navigated to logout endpoint")
                    await page.wait_for_timeout(600)
                except Exception as e:
                    self.logger.debug(f"[LOGOUT] goto logout endpoint failed or timed out: {e}")
                return True

            # If the page exists, use the last one
            page = pages[-1]
            ui_success = False

            # --- 1) Try via UI if allowed ---
            if via_ui:
                try:
                    await page.wait_for_selector(UppiSelectors.ESCI_SISTER_BUTTON, timeout=5_000)
                    await page.click(UppiSelectors.ESCI_SISTER_BUTTON)
                    self.logger.info("[LOGOUT] Clicked Esci button (UI)")
                    await page.wait_for_timeout(1_000)
                    ui_success = True
                except PlaywrightTimeoutError:
                    self.logger.debug("[LOGOUT] Esci button not found, falling back to endpoint")
                except Exception as e:
                    self.logger.debug(f"[LOGOUT] Esci click failed ({type(e).__name__}): {e}")

            # --- 2) If the UI didn't work, try the endpoint ---
            if not ui_success:
                try:
                    await page.goto(SISTER_LOGOUT_URL)
                    await page.wait_for_selector(UppiSelectors.LOGOUT_BUTTON, timeout=8_000)
                    self.logger.info("[LOGOUT] Navigated to logout endpoint (fallback)")
                    await page.wait_for_timeout(600)
                except Exception as e:
                    self.logger.debug(f"[LOGOUT] goto logout endpoint failed or timed out: {e}")

            return True

        except Exception as e:
            self.logger.exception("[LOGOUT] Unexpected error in logout helper: %s", e)
            return False
        finally:
            # Close the temporary page if created one
            if created_temp and page:
                try:
                    await page.close()
                except Exception:
                    pass

            # Close the context if requested.
            if close_context:
                try:
                    await context.close()
                    self.logger.info("[LOGOUT] Context closed")
                except Exception as e:
                    self.logger.warning(f"[LOGOUT] Failed to close context: {e}")

    async def errback_close_page(self, failure):
        """Errback: ensure Playwright page/context is cleaned and closed on request failure."""
        page: Optional[Page] = None
        try:
            page = failure.request.meta.get("playwright_page")
        except Exception:
            pass

        if page:
            try:
                # We are trying to make UI logout, but if button is missing - helper will fall back to endpoint
                await self._logout_in_context(page.context, via_ui=True, close_context=False)
            except Exception as e:
                self.logger.warning("[ERRBACK] logout via page.context failed: %s", e)
            finally:
                await self.safe_close_page(page, "errback")
                return

        # Якщо page немає — плануємо окремий Request щоб відкрити новий playwright page і виконати logout там
        try:
            logout_full_url = SISTER_LOGOUT_URL
            req = scrapy.Request(
                url=logout_full_url,
                callback=self._logout_callback,
                errback=None,
                meta={"playwright": True, "playwright_include_page": True, "playwright_context": "default"},
                dont_filter=True,
            )
            # We send the request through the Scrapy engine
            self.crawler.engine.crawl(req, spider=self)
            self.logger.info("[ERRBACK] Scheduled logout request to free server session (no page present)")
        except Exception as e:
            self.logger.error("[ERRBACK] Could not schedule logout request: %s", e)

    async def _logout_callback(self, response):
        """Callback для logout-request, який створює playwright page автоматично."""
        page = response.meta.get("playwright_page")
        if not page:
            self.logger.error("[LOGOUT_CB] No playwright_page in logout callback")
            return
        try:
            await self._logout_in_context(page.context, via_ui=False, close_context=False)
        finally:
            await self.safe_close_page(page, "logout_callback")
