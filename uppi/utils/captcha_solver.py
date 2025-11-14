import base64
from twocaptcha import TwoCaptcha
from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Page

async def solve_captcha(playwright_page: Page, solver_key: str, codice_fiscale: str, img_captcha_selector="#imgCaptcha"):
    """
    Solves a CAPTCHA using 2Captcha service.
    Args:
        playwright_page: The Playwright page object.
        solver_key (str): The API key for 2Captcha service.
        img_captcha_selector (str): The CSS selector for the CAPTCHA image element.
    Returns:
        str: The solved CAPTCHA code, or None if solving failed.
    """
    try:
        # 1. Check if CAPTCHA element is visible
        try:
            captcha_element = playwright_page.locator(img_captcha_selector)
            if not await captcha_element.is_visible():
                print("[CAPTCHA] ⚠️ Елемент не знайдено або невидимий.")
                return None
        except PlaywrightTimeoutError:
            print("[CAPTCHA] ⚠️ Таймаут при пошуку CAPTCHA.")
            return None

        # 2. Makes screenshot of the CAPTCHA element
        await playwright_page.wait_for_timeout(3_000)
        captcha_bytes = await captcha_element.screenshot(path="captcha_images/codice_fiscale/captcha.png", type="png")
        if not captcha_bytes:
            print("[CAPTCHA] ⚠️ Не вдалося отримати скріншот CAPTCHA.")
            return None

        # 3. Convert to base64
        captcha_base64 = base64.b64encode(captcha_bytes).decode("utf-8")
        # await playwright_page.pause()

        # 4. Send to 2Captcha for solving
        solver = TwoCaptcha(solver_key)
        try:
            result = solver.normal(captcha_base64)
        except Exception as e:
            print(f"[CAPTCHA] ❌ Помилка при зверненні до 2Captcha: {e}")
            return None

        # 5. Check the result
        if not result or "code" not in result:
            print("[CAPTCHA] ⚠️ Відповідь 2Captcha порожня або некоректна.")
            return None

        code = result["code"].strip()
        if not code:
            print("[CAPTCHA] ⚠️ Отримано порожній код.")
            return None

        print(f"[CAPTCHA] ✅ Розпізнано: {code}")
        return code

    except Exception as e:
        print(f"[CAPTCHA] ❌ Неочікувана помилка: {e}")
        return None
