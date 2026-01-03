from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from pyspark.sql import SparkSession
from recommendation_service import RecommendationService
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, 
            template_folder='/opt/spark-apps/templates',
            static_folder='/opt/spark-apps/static')
CORS(app)
app.config['DEBUG'] = True

spark = None
rec_service = None


def init_spark():
    global spark, rec_service
    
    spark_master = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
    data_path = os.getenv('DATA_PATH', '/opt/spark-data')
    model_path = os.getenv('MODEL_PATH', '/opt/spark-data/models')
    
    logger.info(f"Initializing Spark with master: {spark_master}")
    
    spark = SparkSession.builder \
        .appName("BookRecommendationAPI") \
        .master(spark_master) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    rec_service = RecommendationService(spark, data_path, model_path)
    
    logger.info("Spark and RecommendationService initialized")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/new-user')
def new_user():
    return render_template('new_user.html')


@app.route('/api/recommend', methods=['POST'])
def recommend():
    try:
        logger.info("Received recommendation request")
        data = request.json
        logger.info(f"Request data: {data}")
        
        user_id = data.get('user_id')
        top_n = data.get('top_n', 10)
        use_lgbm = data.get('use_lgbm', True)
        
        if not user_id:
            logger.warning("user_id not provided")
            return jsonify({'error': 'user_id is required'}), 400
        
        logger.info(f"Getting recommendations for user {user_id}, top_n={top_n}, use_lgbm={use_lgbm}")
        recommendations = rec_service.get_user_recommendations(
            user_id=str(user_id),
            top_n=int(top_n),
            use_lgbm=bool(use_lgbm)
        )
        logger.info(f"Got {len(recommendations)} recommendations")
        
        logger.info(f"Getting user history for {user_id}")
        user_history = rec_service.get_user_history(str(user_id), limit=5)
        logger.info(f"Got {len(user_history)} history items")
        
        return jsonify({
            'user_id': user_id,
            'recommendations': recommendations,
            'user_history': user_history,
            'count': len(recommendations)
        })
        
    except Exception as e:
        logger.error(f"Error in recommend endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500


@app.route('/api/search', methods=['GET'])
def search():
    try:
        logger.info("Received search request")
        query = request.args.get('q', '')
        limit = request.args.get('limit', 20)
        logger.info(f"Search query: {query}, limit: {limit}")
        
        if not query:
            return jsonify({'error': 'query parameter q is required'}), 400
        
        results = rec_service.search_books(query, int(limit))
        logger.info(f"Found {len(results)} results")
        
        return jsonify({
            'query': query,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        logger.error(f"Error in search endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500


@app.route('/api/popular', methods=['GET'])
def popular():
    try:
        logger.info("Received popular books request")
        top_n = request.args.get('top_n', 20)
        logger.info(f"Getting top {top_n} popular books")
        
        results = rec_service._get_popular_books(int(top_n))
        logger.info(f"Found {len(results)} popular books")
        
        return jsonify({
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        logger.error(f"Error in popular endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500


@app.route('/api/recommend/new-user', methods=['POST'])
def recommend_new_user():
    try:
        logger.info("Received new user recommendation request")
        data = request.json
        logger.info(f"Request data: {data}")
        
        age = data.get('age')
        location = data.get('location')
        favorite_authors = data.get('favorite_authors', [])
        favorite_genres = data.get('favorite_genres', [])
        top_n = data.get('top_n', 10)
        
        logger.info(f"Getting recommendations for new user: age={age}, location={location}, authors={favorite_authors}")
        recommendations = rec_service.get_recommendations_for_new_user(
            age=int(age) if age else None,
            location=location,
            favorite_authors=favorite_authors,
            favorite_genres=favorite_genres,
            top_n=int(top_n)
        )
        logger.info(f"Got {len(recommendations)} recommendations")
        
        return jsonify({
            'recommendations': recommendations,
            'count': len(recommendations),
            'profile': {
                'age': age,
                'location': location,
                'favorite_authors': favorite_authors,
                'favorite_genres': favorite_genres
            }
        })
        
    except Exception as e:
        logger.error(f"Error in new user recommend endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'spark_active': spark is not None,
        'service_ready': rec_service is not None
    })


if __name__ == '__main__':
    init_spark()
    app.run(host='0.0.0.0', port=5000, debug=False)
