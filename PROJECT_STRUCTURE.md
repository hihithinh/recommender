# Project Structure - Distributed Recommender System

```
recommender/
├── raw_data/                           # Raw BookCrossing dataset
│   ├── BX-Book-Ratings.csv
│   ├── BX-Users.csv
│   └── BX_Books.csv
│
├── data/                               # Processed data storage
│   ├── processed/                      # Cleaned and preprocessed data
│   │   ├── ratings.parquet
│   │   ├── users.parquet
│   │   └── books.parquet
│   ├── embeddings/                     # Model embeddings
│   │   ├── als_user_embeddings.parquet
│   │   ├── als_item_embeddings.parquet
│   │   └── combined_features.parquet
│   └── models/                         # Saved models
│       ├── als_model/
│       └── lightgbm_model/
│
├── src/                                # Source code
│   ├── __init__.py
│   ├── config/                         # Configuration files
│   │   ├── __init__.py
│   │   ├── spark_config.py            # Spark cluster configuration
│   │   └── model_config.py            # Model hyperparameters
│   │
│   ├── data_processing/                # Data preprocessing
│   │   ├── __init__.py
│   │   ├── data_loader.py             # Load raw data
│   │   ├── data_cleaner.py            # Clean and validate data
│   │   └── feature_engineering.py     # Feature extraction
│   │
│   ├── models/                         # Model implementations
│   │   ├── __init__.py
│   │   ├── als_recommender.py         # Spark ALS implementation
│   │   ├── lightgbm_recommender.py    # LightGBM implementation
│   │   └── hybrid_recommender.py      # Combine ALS + LightGBM
│   │
│   ├── training/                       # Training pipelines
│   │   ├── __init__.py
│   │   ├── train_als.py               # Train ALS model
│   │   ├── train_lightgbm.py          # Train LightGBM model
│   │   └── train_hybrid.py            # Train hybrid model
│   │
│   ├── evaluation/                     # Model evaluation
│   │   ├── __init__.py
│   │   ├── metrics.py                 # Evaluation metrics (RMSE, MAP@K, etc.)
│   │   └── evaluator.py               # Model evaluation pipeline
│   │
│   └── utils/                          # Utility functions
│       ├── __init__.py
│       ├── spark_utils.py             # Spark helper functions
│       └── logger.py                  # Logging configuration
│
├── scripts/                            # Execution scripts
│   ├── run_preprocessing.py           # Run data preprocessing
│   ├── run_training.py                # Run model training
│   ├── run_evaluation.py              # Run model evaluation
│   └── run_inference.py               # Run batch inference
│
├── notebooks/                          # Jupyter notebooks for analysis
│   ├── 01_data_exploration.ipynb
│   ├── 02_als_experiments.ipynb
│   └── 03_lightgbm_experiments.ipynb
│
├── tests/                              # Unit tests
│   ├── __init__.py
│   ├── test_data_processing.py
│   ├── test_models.py
│   └── test_training.py
│
├── docker/                             # Docker configurations
│   ├── spark-master/
│   │   └── Dockerfile
│   ├── spark-worker/
│   │   └── Dockerfile
│   └── jupyter/
│       └── Dockerfile
│
├── logs/                               # Application logs
│   ├── spark/
│   └── training/
│
├── docker-compose.yml                  # Docker Compose configuration
├── docker-compose.distributed.yml      # Multi-host deployment config
├── requirements.txt                    # Python dependencies
├── setup.py                            # Package setup
├── README.md                           # Project documentation
├── .env.example                        # Environment variables template
└── .gitignore                          # Git ignore file
```

## Architecture Overview

### Components:
1. **Spark Master**: Coordinates distributed computation
2. **Spark Workers**: Execute distributed tasks (scalable to multiple machines)
3. **Jupyter Notebook**: Interactive development and analysis
4. **Shared Storage**: Distributed file system for data and models

### Data Flow:
1. Raw data → Data Processing (distributed) → Processed Parquet files
2. Processed data → ALS Training (distributed) → User/Item embeddings
3. Embeddings + Features → LightGBM Training (distributed) → Final model
4. Models → Evaluation → Metrics and reports

### Distributed Processing:
- All data processing uses Spark DataFrames for distributed computation
- ALS model training distributed across Spark cluster
- LightGBM training using Spark LightGBM for distributed gradient boosting
- Embeddings stored in Parquet format for efficient distributed access
