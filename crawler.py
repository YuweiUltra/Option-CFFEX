import re
from playwright.sync_api import Playwright, sync_playwright, expect
import os
import zipfile

download_path = "downloads"


def run(playwright: Playwright) -> None:
    # Specify your download directory here
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(
        accept_downloads=True  # Enable download handling
    )
    page = context.new_page()
    page.goto("http://www.cffex.com.cn/lssjxz/")

    page.locator("#actualDateStart").fill("2020-01")
    page.get_by_role("button", name="查询").click()

    # Wait for the table to load after clicking the search button
    page.wait_for_selector("table")

    # Locate all the download links in the table (assuming they have a consistent selector)
    download_links = page.locator("table a")  # Adjust this selector if needed

    # Iterate through each link to trigger the downloads
    count = download_links.count()
    for i in range(count-1): # the last month data is lost
        with page.expect_download() as download_info:
            download_links.nth(i).click()
        download = download_info.value
        zip_file_path = os.path.join(download_path, download.suggested_filename)
        download.save_as(zip_file_path)

        # Unzip the downloaded file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)

        # Optionally, remove the zip file after extraction
        os.remove(zip_file_path)

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)

for filename in os.listdir(download_path):
    new_filename = filename.replace("_1", "")
    os.rename(os.path.join(download_path, filename), os.path.join(download_path, new_filename))
