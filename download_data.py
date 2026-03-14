from pathlib import Path
import shutil
import kagglehub

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

dataset_path = Path(
    kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
)

csv_files = list(dataset_path.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError("Nenhum arquivo CSV foi encontrado no dataset baixado.")

source_csv = csv_files[0]
target_csv = DATA_DIR / "IOT-temp.csv"

shutil.copy2(source_csv, target_csv)

print(f"Arquivo original: {source_csv}")
print(f"Arquivo final em: {target_csv.resolve()}")