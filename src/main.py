from src.config import *
from src.reader import *
from src.validate import *
from src.load import *
from src.clean import *
from src.logs.logging_config import *
import time

logger = get_logger(__name__)     #logger store

def run():
    start_time = time.time()  # START TIMER
    cfg = load_sources_config()

    for source in cfg["sources"]:
        if source["type"] != "csv":
            continue
        
        summary = {  # COLLECT STATS FOR SUMMARY
            "source": source["name"],
            "loaded_raw": 0,
            "valid_after_cast": 0,
            "rejected_rows": 0,
            "valid_after_rules": 0,
            "loaded_to_db": 0,
        }
        
        logger.debug(f"📥 Reading source: {source['name']}")
        df = read_csv(source["path"])
        summary["loaded_raw"] = len(df)
        logger.debug(f"   Loaded {len(df)} rows")

        ##Validation
        schema = source["schema"]
        missing = check_missing_columns(df, schema)
        logger.debug(f"   Missing columns: {missing}")

        # type casting reject if anything wrong 
        # Apply type casting
        valid_df, reject_df = apply_schema_casts(df, schema)
        summary["valid_after_cast"] = len(valid_df)
        summary["rejected_rows"] += len(reject_df)
        logger.debug(f"   After casting: {len(valid_df)} valid rows, {len(reject_df)} rejected rows")
        
        if len(reject_df) > 0:
            load_rejects(reject_df, source_name=source["name"], reason="type_cast_failed")
            
        # Business rule validation on the cast-valid rows
        rule_valid_df, rule_reject_df = apply_business_rules(valid_df)
        summary["valid_after_rules"] = len(rule_valid_df)
        summary["rejected_rows"] += len(rule_reject_df)
        logger.debug(f"   After rules:   {len(rule_valid_df)} valid, {len(rule_reject_df)} rejected")
        
        if len(rule_reject_df) > 0:
            load_rejects(rule_reject_df, source_name=source["name"], reason="business_rule_failed")

        # CLEANING step (new)
        clean_df = clean_fashion_sales(rule_valid_df)
        logger.debug(f"   After cleaning: {len(clean_df)} rows ready for load")
        table_name = source["target_table"]  # "stg_fashion_sales"
        
        # Use dataset-specific loader with UPSERT
        load_fashion_sales_upsert(clean_df, table_name)
        summary["loaded_to_db"] = len(clean_df)

        
        # --- RUN SUMMARY BLOCK ---
        logger.info("\n--- RUN SUMMARY ---")
        logger.info(f"Source: {summary['source']}")
        logger.info(f"Loaded (raw): {summary['loaded_raw']}")
        logger.info(f"Valid after cast: {summary['valid_after_cast']}")
        logger.info(f"Rejected rows: {summary['rejected_rows']}")
        logger.info(f"Valid after rules: {summary['valid_after_rules']}")
        logger.info(f"Loaded into DB: {summary['loaded_to_db']}")
        logger.info(f"Runtime: {round(time.time() - start_time, 2)} seconds")
        #logger.info("----------------------\n")        
        
        
if __name__ == "__main__":
    run()
