import argparse
from spark.common.session import build_spark, load_config, DEFAULT_CONFIG

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--config", default=DEFAULT_CONFIG)
    a = p.parse_args()

    cfg = load_config(a.config)
    spark = build_spark(f"verify-{a.date}", cfg)
    
    canonical_root = cfg["paths"]["canonical"]
    
    print("\n--- Day 1 Verification ---")
    for src in ["internal", "processor", "bank"]:
        try:
            df = spark.read.format(cfg["storage"]["format"]).load(f"{canonical_root}/{src}")
            count = df.filter(df.delivery_date == a.date).count()
            print(f"{src}: {count} canonical records found for {a.date}")
        except Exception as e:
            print(f"{src}: FAILED to read - {e}")
            
if __name__ == "__main__":
    main()
