class ALSConfig:
    """
    ALS Hyperparameters optimized based on als_deep_dive.ipynb findings
    
    Reference notebook results:
    - Best RMSE achieved with rank=20, regParam=0.1
    - RMSE: ~0.96, MAE: ~0.75, R2: ~0.27
    - Ranking metrics @K=10: Precision ~0.044, Recall ~0.014, NDCG ~0.037
    """
    
    RANK = 20
    MAX_ITER = 15
    REG_PARAM = 0.1
    ALPHA = 1.0
    IMPLICIT_PREFS = False
    COLD_START_STRATEGY = "drop"
    NUM_USER_BLOCKS = 10
    NUM_ITEM_BLOCKS = 10
    CHECKPOINT_INTERVAL = 10


class LightGBMConfig:
    """
    LightGBM Hyperparameters optimized based on mmlspark_lightgbm_criteo.ipynb
    
    Reference notebook results:
    - Best AUC: 0.659 with numLeaves=32, numIterations=50
    - learningRate=0.1, featureFraction=0.8
    - Uses GBDT boosting with unbalanced data handling
    """
    
    NUM_LEAVES = 32
    MAX_DEPTH = -1
    LEARNING_RATE = 0.1
    NUM_ITERATIONS = 50
    OBJECTIVE = "regression"
    METRIC = "rmse"
    NUM_THREADS = 4
    FEATURE_FRACTION = 0.8
    BAGGING_FRACTION = 0.8
    BAGGING_FREQ = 5
    MIN_DATA_IN_LEAF = 20
    BOOSTING_TYPE = "gbdt"


class DataConfig:
    
    TRAIN_RATIO = 0.8
    VALIDATION_RATIO = 0.1
    TEST_RATIO = 0.1
    RANDOM_SEED = 42
    MIN_RATINGS_PER_USER = 5
    MIN_RATINGS_PER_BOOK = 5
