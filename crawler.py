import logging
import sys

import requests

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Please input result path, e.g. python3 crawler.py tmp/source/lvr_landcsv.zip")
        sys.exit()
    result_path = sys.argv[1]
    SEASON = "108S2"
    download_url = "https://plvr.land.moi.gov.tw/DownloadSeason?season=108S2&type=zip&fileName=lvr_landcsv.zip"

    logging.info(f"Start downloading {SEASON} lvr_land data to {result_path}")
    r = requests.get(download_url)
    if r.status_code == 200:
        with open(result_path, "wb") as f:
            f.write(r.content)
        logging.info("Download completed")
    else:
        logging.error(f"Fail to download: {r.content}")
