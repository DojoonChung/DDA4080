from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

RAW_OD_GLOB = "sim_od_*.csv"
STATION_INFO_XLSX = DATA_DIR / "stationinfo.xlsx"
MAP_SEQ_XLSX = DATA_DIR / "地图站点序号.xlsx"
CLEANED_GEO_CSV = DATA_DIR / "cleaned_subway_data.csv"
STATION_GEO_TXT = DATA_DIR / "station_geo.txt"

CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

TIME_BIN_MINUTES = 10
MAX_TRAVEL_MINUTES = 240
MIN_TRAVEL_MINUTES = 1

WIND_EXPOSED_LINES = {
    "13号线", "八通线", "S1线", "燕房线", "首都机场线",
    "昌平线", "房山线", "亦庄线", "15号线", "5号线",
    "8号线", "14号线", "4号线/大兴线"
}

LINE_COLORS = {
    "1号线": "#C23A30",  
    "2号线": "#006098",  
    "4号线/大兴线": "#008C95",  
    "5号线": "#AA0061",  
    "6号线": "#B58500",  
    "7号线": "#FFC56E",  
    "8号线": "#009B77",  
    "9号线": "#97D700",  
    "10号线": "#0092BC", 
    "13号线": "#F4DA40", 
    "14号线": "#CA9A8E", 
    "15号线": "#653279", 
    "16号线": "#6BA539", 
    "S1线": "#A45A2A",   
    "亦庄线": "#E40077", 
    "八通线": "#C23A30", 
    "大兴机场线": "#0049A5", 
    "房山线": "#D86018", 
    "昌平线": "#D986BA", 
    "燕房线": "#D86018", 
    "首都机场线": "#A192B2", 
}