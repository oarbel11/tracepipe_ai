# 2026-01-10

## 🔍 New Features: Data Quality & Business Logic Validation

The MCP server now includes powerful tools to detect data quality issues and validate business logic.

### New Tools Available

#### 1. **Detect Duplicates** 🔍
Find duplicate rows and understand why they exist:
```
"Check for duplicates in silver.fact_jobs"
"Are there duplicate employees in dim_employees?"
```

**What it does:**
- Finds exact duplicate rows (across all columns or specific columns)
- Explains possible causes (missing DISTINCT, JOIN issues, etc.)
- Suggests fixes (add constraints, review SQL, use ROW_NUMBER, etc.)
- Provides duplicate group details with counts

**MCP Tool:** `detect_duplicates(table, columns)`

#### 2. **Validate Business Rules** ✅
Check if data follows business logic:
```
"Validate that all salaries are positive in fact_jobs"
"Check that end_date is after start_date for all jobs"
```

**What it does:**
- Auto-detects common rules based on column names and types:
  - Positive numbers (salary, price, amount, cost, revenue)
  - Valid date ranges (end_date >= start_date)
  - Required fields (NOT NULL for ID columns)
- Validates custom rules you provide as SQL WHERE clauses
- Shows violation counts and sample violating rows
- Explains why violations might occur

**MCP Tool:** `validate_business_rules(table, rules)`

#### 3. **Analyze Data Quality** 📊
Comprehensive data quality analysis:
```
"Analyze data quality of conformed.company_stats"
"Check for missing values and outliers in fact_jobs"
```

**What it does:**
- Null value analysis (counts and percentages per column)
- Outlier detection in numeric columns (min, max, avg, distinct counts)
- Data type validation (e.g., dates stored as VARCHAR)
- Automatic duplicate detection integration
- Overall quality score (EXCELLENT/GOOD/FAIR/POOR) with detailed metrics
- Actionable recommendations for each issue found

**MCP Tool:** `analyze_data_quality(table)`

### Example Usage in Cursor

**Detecting Issues:**
> "Why are there duplicates in the sales table?"

The MCP will:
1. Run duplicate detection on the table
2. Show which rows are duplicated and how many times
3. Explain possible causes (missing DISTINCT, JOIN issues, data entry errors)
4. Suggest SQL fixes (add DISTINCT, review JOINs, add UNIQUE constraints)

**Validating Logic:**
> "Check if the risk_level calculation is correct for employee 103"

The MCP will:
1. Trace how risk_level is calculated using `explain_column`
2. Validate business rules (e.g., total_jobs >= 3 → HIGH risk)
3. Show actual data values using `inspect_row`
4. Explain if the result matches expectations

**Quality Analysis:**
> "What data quality issues exist in silver.fact_jobs?"

The MCP will:
1. Analyze all columns for nulls, outliers, and type issues
2. Check for duplicates automatically
3. Calculate overall quality score
4. Provide specific recommendations for each issue

### Implementation Details

**Files Modified:**
- `scripts/debug_engine.py` - Added three new methods:
  - `detect_duplicates()` - Duplicate detection with explanations
  - `validate_business_rules()` - Business rule validation
  - `analyze_data_quality()` - Comprehensive quality analysis
  - Helper methods for auto-detection and scoring

- `mcp_server.py` - Added three new MCP tools:
  - `detect_duplicates` - Exposed as MCP tool
  - `validate_business_rules` - Exposed as MCP tool
  - `analyze_data_quality` - Exposed as MCP tool

**Features:**
- Auto-detection of common business rules based on column naming patterns
- Intelligent explanations that help understand why issues exist
- Actionable recommendations for fixing problems
- Quality scoring system for quick assessment
- Integration with existing lineage tools for comprehensive debugging

### Backwards Compatibility

✅ All existing tools continue to work as before  
✅ New tools are optional additions  
✅ No breaking changes to existing APIs  
✅ Same security validations apply  

### Testing

The new features have been tested and verified:
- ✅ Import successful
- ✅ Duplicate detection working
- ✅ Business rule validation working
- ✅ Data quality analysis working
- ✅ No linting errors
- ✅ Backwards compatible

---

## Previous Updates

_This section will contain updates from previous dates as new features are added._


