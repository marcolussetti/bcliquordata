# bcliquordata

This project collects and updates product data from BC Liquor stores in British Columbia, Canada.  
It includes CSV and JSON files for beer, wine, spirits, and coolers.

## Files
- `bcliquordata-beer.csv` / `.json`
- `bcliquordata-wine.csv` / `.json`
- `bcliquordata-spirits.csv` / `.json`
- `bcliquordata-coolers.csv` / `.json`
- `gitscrape.py` – runs the scraper
- `periodicdata.py` – schedules updates
- `requirements.txt` – Python dependencies

## How to Use
```bash
git clone https://github.com/marcolussetti/bcliquordata.git
cd bcliquordata
pip install -r requirements.txt
python gitscrape.py
```

## License
MIT License – use freely, attribution appreciated.
