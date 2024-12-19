# Data Acquisition Project

## Overview

This project aims to acquire and process doctor data from various sources, including the e_sante API and the RPPS dataset. The pipeline involves multiple steps to fetch, merge, preprocess, and store the data into a PostgreSQL database.

## Prerequisites

Before running the pipeline, ensure you have the following:

1. **Python Environment**: Make sure you have Python 3.x installed.
2. **Dependencies**: Install the required Python packages. You can do this by running:
   ```bash
   pip install -r requirements.txt
   ```
3. **API Key**: Obtain the API key for the e_sante API.
4. **RPPS Data**: Download the `rpps_data.csv` file.

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/data_acq.git
   cd data_acq
   ```

2. **Create a Virtual Environment**:
   ```bash
   python -m venv data_acq_env
   source data_acq_env/bin/activate  # On Windows use `data_acq_env\Scripts\activate`
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set Environment Variables**:
   Create a `.env` file in the root directory and add your API key:
   ```env
   API_KEY=your_e_sante_api_key
   ```

5. **Place the RPPS Data**:
   Ensure the `rpps_data.csv` file is placed in the `data` directory.

## Pipeline Execution

The pipeline consists of several scripts that need to be run in a specific order. Follow these steps to execute the pipeline:

1. **Run `ameli_api.py`**:
   ```bash
   python ameli_api.py
   ```

2. **Run `merge_api.py`**:
   ```bash
   python merge_api.py
   ```

3. **Run `map_to_code.py`**:
   ```bash
   python map_to_code.py
   ```

4. **Run `getDatPract.py`**:
   ```bash
   python getDatPract.py
   ```

5. **Run `preprocess_ameli.py`**:
   ```bash
   python preprocess_ameli.py
   ```

6. **Run `mapping_profession.py`**:
   ```bash
   python mapping_profession.py
   ```

7. **Run `merge_all_datasets.py`**:
   ```bash
   python merge_all_datasets.py
   ```

8. **Run `store_data.py`**:
   ```bash
   python store_data.py
   ```

## Additional Scripts

- **`oms_api.py` and `spain_data_api.py`**:
  These scripts are exploratory and were used to add more data to the dataset. They are not part of the main pipeline and can be ignored for now.

## Database Configuration

Ensure your PostgreSQL database is configured correctly. Update the connection details in `store_data.py` if necessary:

```python
conn = psycopg2.connect(
    dbname="doctor_database", user="postgres", password="yourpassword", host="localhost", port="5432"
)
```

## Troubleshooting

- **API Rate Limits**: Be mindful of the number of requests you make to the e_sante API to avoid hitting rate limits.
- **Data Issues**: Ensure the `rpps_data.csv` file is correctly formatted and placed in the `data` directory.

