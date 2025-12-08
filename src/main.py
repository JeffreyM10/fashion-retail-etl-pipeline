from src.config import load_sources_config
from src.reader import read_csv
from src.validate import *
from src.load import *
from src.clean import *

def run():
    cfg = load_sources_config()

    for source in cfg["sources"]:
        if source["type"] != "csv":
            continue

        print(f"\n📥 Reading source: {source['name']}")
        df = read_csv(source["path"])
        print(f"   Loaded {len(df)} rows")

        ##Validation
        schema = source["schema"]
        missing = check_missing_columns(df, schema)
        print("   Missing columns:", missing)

        # type casting reject if anything wrong 
        # Apply type casting
        valid_df, reject_df = apply_schema_casts(df, schema)
        print(f"   After casting: {len(valid_df)} valid rows, {len(reject_df)} rejected rows")
        
        if len(reject_df) > 0:
            load_rejects(reject_df, source_name=source["name"], reason="type_cast_failed")
            
            
        # Business rule validation on the cast-valid rows
        rule_valid_df, rule_reject_df = apply_business_rules(valid_df)
        print(f"   After rules:   {len(rule_valid_df)} valid, {len(rule_reject_df)} rejected")
        
        if len(rule_reject_df) > 0:
            load_rejects(rule_reject_df, source_name=source["name"], reason="business_rule_failed")

        # 3.5) CLEANING step (new)
        clean_df = clean_fashion_sales(rule_valid_df)
        print(f"   After cleaning: {len(clean_df)} rows ready for load")
        
        # Load Valid rows into PostGresSQl
        table_name = source["target_table"]  # "stg_fashion_sales"
        load_fashion_sales(rule_valid_df, table_name)
        
        
if __name__ == "__main__":
    run()
