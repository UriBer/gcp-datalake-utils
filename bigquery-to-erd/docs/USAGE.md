# BigQuery to ERD Tool - Usage Guide

## Quick Start

1. **Install the tool**:
   ```bash
   pip install -e .
   ```

2. **Set up your environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your BigQuery credentials and settings
   ```

3. **Generate your first ERD**:
   ```bash
   bigquery-to-erd
   ```

## Configuration

### Environment Variables

The tool uses environment variables for configuration. Copy `.env.example` to `.env` and modify the values:

```env
# Required
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
PROJECT_ID=your-gcp-project-id
DATASET_ID=your-dataset-name

# Optional
OUTPUT_FORMAT=drawio
OUTPUT_FILE=erd_output.drawio
INCLUDE_VIEWS=false
TABLE_LAYOUT=auto
```

### Command Line Options

You can override any environment variable using command line options:

```bash
bigquery-to-erd --dataset-id my_dataset --output-file my_erd.drawio --format mermaid
```

## Output Formats

### Draw.io Format (.drawio)
- **Best for**: Interactive editing, presentations
- **Features**: Full visual control, themes, custom styling
- **Usage**: Import into app.diagrams.net

### Mermaid Format (.mmd)
- **Best for**: Documentation, version control
- **Features**: Text-based, Git-friendly
- **Usage**: Render in GitHub, GitLab, or Mermaid Live Editor

### PlantUML Format (.puml)
- **Best for**: Technical documentation
- **Features**: UML standard, extensive customization
- **Usage**: Render with PlantUML tools

## Relationship Detection

The tool automatically detects relationships using multiple methods:

### 1. Foreign Key Detection
- Analyzes column names ending in `_id`, `_fk`, `_key`
- Matches data types between tables
- Confidence: High (0.8)

### 2. Naming Convention Detection
- Matches patterns like `user_id` → `users.id`
- Handles plural/singular conversions
- Confidence: Medium (0.6)

### 3. Data Type Matching
- Finds columns with compatible types
- Lower confidence but catches edge cases
- Confidence: Low (0.4)

### 4. Custom Rules
- Define explicit relationships in JSON
- Override automatic detection
- Confidence: Configurable

## Custom Relationship Rules

Create a JSON file to define custom relationships:

```json
{
  "relationships": [
    {
      "source_table": "orders",
      "source_column": "customer_id",
      "target_table": "customers",
      "target_column": "id",
      "relationship_type": "many_to_one",
      "confidence": 0.9
    }
  ],
  "naming_patterns": [
    {
      "pattern": ".*_id$",
      "target_suffix": "s",
      "confidence": 0.8
    }
  ]
}
```

## Layout Algorithms

### Auto (Default)
- Chooses best algorithm based on table count and relationships
- Small datasets (< 5 tables): Grid layout
- Many relationships: Force-directed layout
- Otherwise: Hierarchical layout

### Grid Layout
- Simple grid arrangement
- Good for small datasets
- Predictable positioning

### Hierarchical Layout
- Top-down arrangement
- Shows data flow clearly
- Good for normalized schemas

### Force-Directed Layout
- Physics-based positioning
- Minimizes edge crossings
- Good for complex schemas

### Horizontal/Vertical Layout
- Linear arrangements
- Good for simple schemas
- Space-efficient

## Examples

### Basic Usage
```bash
# Generate ERD for default dataset
bigquery-to-erd

# Specify different dataset
bigquery-to-erd --dataset-id analytics_data

# Include views
bigquery-to-erd --include-views
```

### Advanced Usage
```bash
# Generate Mermaid format with custom layout
bigquery-to-erd --format mermaid --table-layout hierarchical --output-file schema.mmd

# Use custom relationship rules
bigquery-to-erd --custom-rules my_rules.json

# Verbose output for debugging
bigquery-to-erd --verbose
```

### Dry Run
```bash
# See what would be generated without executing
bigquery-to-erd --dry-run
```

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Error: Failed to connect to BigQuery
   ```
   - Check `GOOGLE_APPLICATION_CREDENTIALS` path
   - Verify service account has BigQuery access
   - Test with `gcloud auth application-default login`

2. **Dataset Not Found**
   ```
   Error: Dataset my_dataset not found
   ```
   - Verify dataset ID is correct
   - Check project ID
   - Ensure dataset exists in specified location

3. **No Tables Found**
   ```
   No tables found in dataset
   ```
   - Check if dataset has tables
   - Verify `INCLUDE_VIEWS` and `INCLUDE_EXTERNAL_TABLES` settings
   - Check `MAX_RESULTS` limit

4. **Permission Denied**
   ```
   Error: Permission denied
   ```
   - Service account needs `bigquery.tables.get` permission
   - Service account needs `bigquery.datasets.get` permission

### Debug Mode

Enable verbose logging to see detailed information:

```bash
bigquery-to-erd --verbose
```

This will show:
- Connection details
- Table discovery process
- Relationship detection results
- Layout algorithm choices

### Log Files

Logs are written to console by default. To save to file:

```bash
# Set in .env
LOG_FILE=bigquery_to_erd.log

# Or use command line
bigquery-to-erd --log-file debug.log --verbose
```

## Performance Tips

### Large Datasets
- Use `MAX_RESULTS` to limit table count
- Consider excluding views and external tables
- Use hierarchical layout for better performance

### Memory Usage
- The tool processes all tables in memory
- For very large schemas, consider splitting into multiple datasets
- Monitor memory usage with `--verbose` logging

### Network Optimization
- Use regional BigQuery locations when possible
- Consider running from same region as BigQuery
- Use service account authentication for better performance

## Integration

### CI/CD Pipelines
```yaml
# GitHub Actions example
- name: Generate ERD
  run: |
    pip install bigquery-to-erd
    bigquery-to-erd --format mermaid --output-file schema.mmd
```

### Documentation Generation
```bash
# Generate ERD for documentation
bigquery-to-erd --format mermaid --output-file docs/schema.mmd
```

### Schema Monitoring
```bash
# Generate ERD and compare with previous version
bigquery-to-erd --output-file current_schema.drawio
diff previous_schema.drawio current_schema.drawio
```
