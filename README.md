# Enterprise Data Pipeline with Databricks Asset Bundles (DAB), CI/CD & Automated Validation

The project demonstrates end-to-end expertise in ETL pipeline development, data quality validation and CI/CD automation which, deployed using **Databricks Asset Bundles (DAB)** for modern CI/CD practices.

### **Business Requirement:**
Engineered a scalable data pipeline processing **Contact Information** and **Real Estate** datasets through a multi-layer architecture, ensuring data quality, compliance, and production readiness for enterprise analytics.

---

## 🚀 Quick Start

### **Project Highlights**
- **Pipeline Architecture**: Bronze → Silver → Gold
- **Automated CI/CD pipeline**: using GithHub Action
- **QA Framework**: Smoke and Regression testing
- **Modern Deployment**: using Databricks Asset Bundles (DAB)
- **Data Quality validation**: performed at every stage of task run

### **CI/CD Deployment**
The GitHub Actions workflow automatically:
1. ✅ Installs correct Databricks CLI
2. ✅ Validates bundle configuration
3. ✅ Deploys to Databricks workspace
4. ✅ Runs validation jobs

---

## 📋 Repository File Structure

```
data-pipeline/
├── .github/workflows/
│   └── data_etl.yml            # CI/CD workflow (DAB-based)
├── data_pipeline/              # Core application code
│   ├── contact_info/
│   ├── core/
│   ├── data_generation/
│   ├── real_estate/
│   └── validation/
├── databricks.yml              # DAB configuration (MAIN)
└── setup.py                    # Python package setup
```

---

## 🏗️ End-to-End Pipeline Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                      DATA ENGINEERING PIPELINE                     │
└────────────────────────────────────────────────────────────────────┘

📁 Data Sources                  🔄 Processing Layers              📊 Analytics
     │                                   │                             │
     ├─► Synthetic Data ────────► 🟦 Raw Zone ──────────────────────► │
     │   Generator                  │ (Parquet Files)                  │
     │   (Faker)                    │ - No transformation              │
     │                              │ - Batch tracking                 │
     │                              │                                  │
     │                              ▼                                  │
     │                         🟧 Bronze Zone ──────────────────────► │
     │                              │ - Data cleansing                 │
     │                              │ - Null filtering                 │
     │                              │ - Special char removal           │
     │                              │ - Phone standardization          │
     │                              │ - Name normalization             │
     │                              │                                  │
     │                              ▼                                  │
     │                         🟨 Silver Zone ──────────────────────► │
     │                              │ - Delta Lake tables              │
     │                              │ - Schema evolution               │
     │                              │ - Ready for analytics            │
     │                              │                                  │
     │                              │                                  │
     │                              ▼                                  │
     │                         🟩 Gold Zone (Future Planned) ───────► │
     │                                - Aggregations                   │
     │                                - Business metrics               │
     │                                - Feature engineering            │
     │                                                                 │
     └─────────────────────────────────────────────────────────────────┘

                    ✅ Validation Layer (Parallel)
                         │
                         ├─► Smoke Tests (Fast)
                         │   - Business rule validation (YAML-based queries)
                         │   
                         └─► Regression Tests (Comprehensive)
                             - Schema validation
                             - Row count checks
                             - Data comparison
```
                             
---

## 🔧 Key End-to-End Pipeline

### **Data Generation Module**
Built synthetic test data for development and testing:
```python
# File: data_pipeline/data_generation/task/generate_data_task.py
def etl_process(**options):
    """Generate realistic synthetic data using Faker"""
    fake = Faker()
    
    # Intelligent batch ID management
    batch_id = batch_ids_processing(path)  # Auto-increments from last batch
    
    # Generate records with realistic patterns
    for i in range(num_rows):
        data.append({
            "profile_id": fake.uuid4(),
            "first_name": random_cases(fake.first_name()),
            "phone_personal": fake.phone_number(),
            # ... 20+ fields with realistic data
        })
```
Key features:
- ✅ Automatic batch versioning
- ✅ Realistic data patterns with intentional errors
- ✅ Data quality issues for testing (null, special chars, etc.)
- ✅ Separating datasets for Contact Info and Real Estate
- ✅ Store all datasets into a parquet files

### **Raw Layer Extractions**
```python
# File: data_pipeline\real_estate\task\raw\re_extract_data_task.py
def etl_process(**options):
    # Fetch all the files in the folder
    based_path = "/Volumes/data_lake_dev/feature_raw_data/real_estate_parquet/"
    files = dbutils.fs.ls(based_path)

    # Table existence check and append the table
    spark.sql(f"CREATE TABLE IF NOT EXISTS {re_raw_loc} USING DELTA")
    re_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(re_raw_loc)
```
Reasons raw layer is lean:
- ✅ Faster downstream iteration (as Bronze handle heavy processes)
- ✅ Simplifies reprocessing (rerun Bronze/Silver without re-extract)

### **Bronze Layer Transformation**
```python
# File: data_pipeline\contact_info\task\bronze\ci_transform_data_task.py
def etl_process(**options):
    """Data Quality Transformations"""
    
    # Filter NULL values in critical fields
    filter_null_df = ci_raw.filter(
        ~F.expr("first_name IS NULL AND last_name IS NULL")
    )
    
    # Remove special characters using regex
    spec_char_rmv_df = filter_null_df.withColumn(
        "first_name", 
        F.regexp_replace("first_name", r"(?i)[^a-z0-9_-]", "")
    )
    
    # Standardize phone numbers to US format
    format_us_phone_udf = F.udf(us_format_phone, StringType())
    std_phone_df = spec_char_rmv_df.withColumn(
        "std_phone", format_us_phone_udf(F.col("phone"))
    )
    
    # Name normalization (lowercase, concatenation)
    std_name = name_df.withColumn(
        "std_full_name", 
        F.lower(F.concat_ws(" ", "first_name", "middle_name", "last_name"))
    )
```
Transformations applied:
- ✅ NULL handling
- ✅ Regex-based special characters removal
- ✅ Phone number standardization
- ✅ Name standardization

### **Silver Layer Loading**
```python
# File: data_pipeline\contact_info\task\silver\ci_load_data_task.py
def etl_process(**options):
    """Load to Silver with Delta Lake features"""
    
    # Enable schema evolution for flexibility
    ci_bronze.write.format("delta")\
        .mode("append")\
        .option("mergeSchema", "true")\
        .saveAsTable(ci_silver_loc)
```
Delta lake benefits:
- ✅ Data versioning and time travel capabilities
- ✅ Data reliability and consistency (prevent data corruption & allow rollbacks)

### **QA Validation Framework**
Configuration-Driven Testing:
```python
# File: data_pipeline\core\validation\config\smoke\qa_config_cip_smoke.csv
environment,space,object_type,zone,job_type,test_type,check_type,assert_type
dev,synthetic,ci,raw,cip,smoke,software,hard_stop
dev,synthetic,ci,raw,cip,smoke,data_quality,soft_stop
dev,synthetic,ci,silver,cip,smoke,software,hard_stop
```

Validation Framework Architecture:
```python
# File: data_pipeline\validation\task\rep_val.py
def etl_process(**options):
    # Smoke Test run 
    if test_type == 'smoke': 
        print("Running smoke tests...")
        smoke_run_validation(use_case_id, config, bucket, catalog, env, space, job_type, smoke_object_type, smoke_zone, test_type, check_types, batch_id, flow, property_schema) 

    # Regression Test run 
    elif test_type == 'regression':   
        print("Running regression tests...")
```
Key features:
- ✅ Streamlined CSV-driven test configuration
- ✅ Hard/soft assertion modes
- ✅ Automated test report generation

---
