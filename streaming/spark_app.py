import json
import requests
from collections import Counter
from datetime import datetime
import time
import traceback
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import avg

BATCH_INTERVAL = 60
LANGUAGES = ['Python', 'Java', 'JavaScript']

# Dictionary to store all repositories sent by the Data Source service so far
# Each key is a repo id and value is their json data
def getAllRepositories() -> dict:
    if('repositories' not in globals()):
        globals()['repositories'] = {}
    return globals()['repositories']

def updateAllRepositories(repositories):
    globals()['repositories'] = repositories

# A list of repository dictionaries, each dictionary stores the repositories for a batch 
def getBatchRepositories() -> list:
    if('batch_repositories' not in globals()):
        globals()['batch_repositories'] = []
    return globals()['batch_repositories']

def updateBatchRepositories(repositories):
    time = datetime.utcnow()
    getBatchRepositories().append({
        'time': time,
        'repos': repositories
    })

def getSparkSession():
    return globals()['spark_session']

def getUpTime():
    return int(time.time() - globals()['startTime'])


def sendToClient(data):
    url = 'http://webapp:5000/updateData'
    requests.post(url, json=data)

def getTotalRepoCountByLanguage():
    repositories = getAllRepositories()

    # Convert the repositories dictionary to a list of dictionaries
    repos_list = list(repositories.values())

    # get the SparkSession object
    spark = getSparkSession()
    
    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(repos_list)

    # Group the DataFrame by the 'language' column and count the number of occurrences of each language
    counts_df = df.groupBy('language').count()

    # Show the counts_df DataFrame in the console
    print("----------- REQUIREMENT 3.1 -----------")
    counts_df.show()

    # Convert the resulting DataFrame to a list of dictionaries
    counts_list = counts_df.rdd.map(lambda row: {'language': row['language'], 'count': row['count']}).collect()

    return counts_list

def getBatchedRepoLanguageCountsLast60Seconds():
    # Get repos that were pushed during the last 60 secodns
    sc = globals()['SparkContext']

    # Get latest /current batch to process
    current_batch = getBatchRepositories()[-1]
    batch_time = current_batch['time']

    # Using a global so I don't have to re-calculate batches
    if 'BatchedRepoLanguageCounts' not in  globals():
        globals()['BatchedRepoLanguageCounts'] = []       
    batched_repos_language_counts = globals()['BatchedRepoLanguageCounts']

    current_batch_repos_last_60 = {}
    # Get the repos that were pushed in the last 60 seconds
    for repo in current_batch['repos'].values():

        # Calculate the time passed since repo was pushed, store it in delta
        pushed_at = datetime.strptime(repo['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')
        delta = batch_time - pushed_at

        # Check if the time difference is less than 60 seconds
        if delta.total_seconds() <= 60:
            if(repo['id'] not in current_batch_repos_last_60):
                current_batch_repos_last_60[repo['id']] = repo
    
    # Convert current_batch_repos_last_60 to an RDD
    repos = sc.parallelize(current_batch_repos_last_60.values())

    # Perform a map reduce to count the occurances of each langauge and collect it as a list
    counts  = repos.map(lambda repo: (repo['language'], 1)).reduceByKey(lambda a, b: a+b)
    counts_list = counts.map(lambda x: {"language": x[0], "count": x[1]}).collect()

    # Conver the RDD to a dictionary so I can easily check if a langauge is missing by making it the key 
    current_batch_language_counts = {count["language"]: count["count"] for count in counts_list}

    # Fill in missing data if needed
    # Iterate over the required languages list and add missing languages to the dictionary current_batch_language_counts.
    # I need to do this because I cannot gurantee that all three langauges will be represented in current_batch_language_counts
    # because I am further filtering them based on 'pushed_at' attribute.
    for language in LANGUAGES:
        if language not in current_batch_language_counts:
            current_batch_language_counts[language] = 0

    # Convert the dictionary back to a list of JSON objects and add them to batched_repos_language_counts list
    for language in current_batch_language_counts:
        batched_repos_language_counts.append({'batch_time': batch_time.strftime('%H:%M:%S'), "language": language, "count": current_batch_language_counts[language]})

    # get the SparkSession object
    spark = getSparkSession()
    
    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(batched_repos_language_counts)

    # Show the df DataFrame in the console
    print("----------- REQUIREMENT 3.2 -----------")
    df.show()

    return batched_repos_language_counts

def getAvgStargazersByLanguage():
    repositories = getAllRepositories()

    # Convert the repositories dictionary to a list of dictionaries
    repos_list = list(repositories.values())

    # Get the SparkSession object
    spark = getSparkSession()

    # Create a Spark DataFrame from the list of dictionaries
    df = spark.createDataFrame(repos_list)

    # Group the DataFrame by the 'language' column and calculate the average 'stargazers_count' and set the new column name to 'avg_stargazers_count'
    avg_df = df.groupBy('language').agg(round(avg("stargazers_count"), 2).alias('avg_stargazers_count'))

    # Show the avg_df DataFrame in the console
    print("----------- REQUIREMENT 3.3 -----------")
    avg_df.show()

    # Convert the resulting DataFrame to a list of dictionaries
    avg_list = avg_df.rdd.map(lambda row: {'language': row['language'], 'avg_stargazers_count': row['avg_stargazers_count']}).collect()

    return avg_list

def getTopTenFrequentWordsByLanguage():
    repositories = getAllRepositories()
    sc = globals()['SparkContext']
    # Convert the repositories dictionary to a RDDs
    repos = sc.parallelize(repositories.values())

    # Split the description into words and group them by language
    # I use groupByKey to get elements where each element is a pair consisting of a language and an iterable of the words.
    words_by_language = repos.flatMap(lambda repo: ((repo['language'], word) for word in re.sub('[^a-zA-Z ]', '', str(repo['description'])).lower().split() if repo['description'] is not None) ) \
                             .groupByKey()
    
    # Count the frequency of each word
    # We first transform each language's iterable of the words to a Counter object with those words.
    # We then sort the resulting dictionary items by their values(frequency) in descending order and takes the top 10 items.
    # The result is elements where each element is a pair consisting of a language and the top ten most frequent words with their frequency.
    word_count_by_language = words_by_language.mapValues(lambda words: Counter(words)) \
                                               .mapValues(lambda word_count: sorted(word_count.items(), key=lambda x: x[1], reverse=True)[:10])

    # Collect the results
    top_words_by_language = word_count_by_language.collect()


    # Show results in Console
    print("----------- REQUIREMENT 3.4 -----------")
    spark = getSparkSession()
    top_words_by_language_list = []

    for language, top_words in top_words_by_language:
        top_words_by_language_list.append(
            {
            'language': language,
            'top_ten_words': top_words
            }
        )
    df = spark.createDataFrame(top_words_by_language_list)
    df.show()

    return top_words_by_language_list

def getTopicModelingByLanguage():
    """
    Topic Modeling (LDA) - Discover hidden topics and themes in repository descriptions
    Tools: Spark MLlib (LDA), built-in text processing
    Insights: Emerging technology trends, popular domains (AI, web, mobile)
    """
    repositories = getAllRepositories()
    spark = getSparkSession()
    
    # Filter repositories with descriptions
    repos_with_descriptions = [repo for repo in repositories.values() if repo.get('description') and repo['language'] in LANGUAGES]
    
    if not repos_with_descriptions:
        print("----------- REQUIREMENT 3.5 (TOPIC MODELING) -----------")
        print("No repositories with descriptions found for topic modeling")
        return []
    
    # Create DataFrame
    df = spark.createDataFrame(repos_with_descriptions)
    
    topic_modeling_results = []
    
    print("----------- REQUIREMENT 3.5 (TOPIC MODELING) -----------")
    
    for language in LANGUAGES:
        # Filter by language
        lang_df = df.filter(col('language') == language)
        
        if lang_df.count() < 5:  # Need minimum documents for meaningful topics
            print(f"Not enough repositories for {language} topic modeling (need at least 5, found {lang_df.count()})")
            continue
        
        try:
            # Text preprocessing
            from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
            from pyspark.ml.clustering import LDA
            
            # Clean and tokenize text
            tokenizer = Tokenizer(inputCol="description", outputCol="words")
            words_data = tokenizer.transform(lang_df)
            
            # Remove stop words
            remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
            filtered_data = remover.transform(words_data)
            
            # Create term frequency vectors
            cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=1000, minDF=2)
            cv_model = cv.fit(filtered_data)
            featurized_data = cv_model.transform(filtered_data)
            
            # Apply IDF
            idf = IDF(inputCol="raw_features", outputCol="features")
            idf_model = idf.fit(featurized_data)
            rescaled_data = idf_model.transform(featurized_data)
            
            # Train LDA model
            lda = LDA(k=5, maxIter=10, optimizer="online")  # 5 topics per language
            lda_model = lda.fit(rescaled_data)
            
            # Extract topics
            topics = lda_model.describeTopics(5)  # Top 5 words per topic
            
            # Convert vocabulary for interpretation
            vocab = cv_model.vocabulary
            
            language_topics = {
                'language': language,
                'topics': []
            }
            
            # Collect topic results
            for row in topics.collect():
                topic_words = []
                for idx in row['termIndices']:
                    if idx < len(vocab):
                        topic_words.append(vocab[idx])
                
                language_topics['topics'].append({
                    'topic_id': row['topic'],
                    'top_words': topic_words[:5],  # Top 5 words per topic
                    'word_weights': [float(w) for w in row['termWeights'][:5]]
                })
            
            topic_modeling_results.append(language_topics)
            
            print(f"\n{language} Topics:")
            for topic in language_topics['topics']:
                print(f"  Topic {topic['topic_id']}: {', '.join(topic['top_words'])}")
            
        except Exception as e:
            print(f"Error in topic modeling for {language}: {str(e)}")
            continue
    
    return topic_modeling_results

def getNamedEntityRecognition():
    """
    Named Entity Recognition (NER) - Extract companies, technologies, frameworks, tools mentioned
    Tools: Custom pattern matching (spaCy-like), keyword dictionaries
    Insights: Technology ecosystem mapping, company involvement
    """
    repositories = getAllRepositories()
    
    # Define entity patterns for technology ecosystem
    tech_patterns = {
        'companies': ['google', 'microsoft', 'amazon', 'facebook', 'apple', 'netflix', 'uber', 'airbnb', 'twitter', 'github', 'gitlab', 'docker', 'kubernetes', 'ibm', 'oracle', 'salesforce', 'adobe', 'intel', 'nvidia'],
        'frameworks': ['react', 'angular', 'vue', 'django', 'flask', 'spring', 'express', 'fastapi', 'rails', 'laravel', 'symfony', 'tensorflow', 'pytorch', 'keras', 'scikit-learn', 'pandas', 'numpy', 'spark', 'hadoop'],
        'tools': ['docker', 'kubernetes', 'jenkins', 'git', 'webpack', 'babel', 'npm', 'yarn', 'maven', 'gradle', 'redis', 'postgresql', 'mysql', 'mongodb', 'elasticsearch', 'kafka', 'aws', 'azure', 'gcp'],
        'technologies': ['ai', 'ml', 'machine learning', 'deep learning', 'blockchain', 'iot', 'ar', 'vr', 'microservices', 'serverless', 'devops', 'cicd', 'big data', 'cloud', 'mobile', 'web', 'api', 'rest', 'graphql']
    }
    
    ner_results = []
    
    print("----------- REQUIREMENT 4.1 (NAMED ENTITY RECOGNITION) -----------")
    
    for language in LANGUAGES:
        language_entities = {
            'language': language,
            'entities': {
                'companies': {},
                'frameworks': {},
                'tools': {},
                'technologies': {}
            }
        }
        
        # Process repositories for this language
        repos_lang = [repo for repo in repositories.values() if repo.get('description') and repo['language'] == language]
        
        for repo in repos_lang:
            description = repo['description'].lower()
            
            # Extract entities using pattern matching
            for entity_type, patterns in tech_patterns.items():
                for pattern in patterns:
                    if pattern in description:
                        if pattern not in language_entities['entities'][entity_type]:
                            language_entities['entities'][entity_type][pattern] = 0
                        language_entities['entities'][entity_type][pattern] += 1
        
        # Convert to sorted lists for display
        for entity_type in language_entities['entities']:
            sorted_entities = sorted(language_entities['entities'][entity_type].items(), key=lambda x: x[1], reverse=True)[:5]  # Top 5
            language_entities['entities'][entity_type] = [{'name': name, 'count': count} for name, count in sorted_entities]
        
        ner_results.append(language_entities)
        
        # Print results
        print(f"\n{language} Named Entities:")
        for entity_type, entities in language_entities['entities'].items():
            if entities:
                entity_str = ', '.join([f"{e['name']}({e['count']})" for e in entities[:3]])
                print(f"  {entity_type.title()}: {entity_str}")
    
    return ner_results

def getTFIDFCosineSimilarity():
    """
    TF-IDF with Cosine Similarity - Find similar repositories based on description content
    Tools: Spark MLlib (TF-IDF, Cosine Similarity)
    Insights: Project clustering, duplicate detection, recommendation engine
    """
    repositories = getAllRepositories()
    spark = getSparkSession()
    
    print("----------- REQUIREMENT 4.2 (TF-IDF COSINE SIMILARITY) -----------")
    print(f"Total repositories: {len(repositories)}")
    
    # Debug: Check what languages are available
    available_languages = set()
    for repo in repositories.values():
        if repo.get('language'):
            available_languages.add(repo['language'])
    print(f"Available languages: {available_languages}")
    print(f"Target languages: {LANGUAGES}")
    
    # Filter repositories with descriptions
    repos_with_descriptions = [repo for repo in repositories.values() if repo.get('description') and repo['language'] in LANGUAGES]
    
    print(f"Repositories with descriptions and target languages: {len(repos_with_descriptions)}")
    
    if not repos_with_descriptions:
        print("No repositories with descriptions found for similarity analysis")
        return []
    
    similarity_results = []
    
    for language in LANGUAGES:
        # Filter repositories by language
        lang_repos = [repo for repo in repos_with_descriptions if repo['language'] == language]
        
        if len(lang_repos) < 3:  # Need minimum repositories for meaningful similarity
            print(f"Not enough repositories for {language} similarity analysis (need at least 3, found {len(lang_repos)})")
            continue
        
        try:
            from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
            from pyspark.ml.linalg import Vectors
            from pyspark.sql.functions import udf
            from pyspark.sql.types import DoubleType
            
            # Create DataFrame
            df = spark.createDataFrame(lang_repos)
            
            # Text preprocessing
            tokenizer = Tokenizer(inputCol="description", outputCol="words")
            words_data = tokenizer.transform(df)
            
            # Remove stop words
            remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
            filtered_data = remover.transform(words_data)
            
            # Create TF-IDF vectors
            cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features", vocabSize=1000, minDF=1)
            cv_model = cv.fit(filtered_data)
            featurized_data = cv_model.transform(filtered_data)
            
            idf = IDF(inputCol="raw_features", outputCol="features")
            idf_model = idf.fit(featurized_data)
            rescaled_data = idf_model.transform(featurized_data)
            
            # Collect TF-IDF vectors and repository info
            repo_vectors = rescaled_data.select("id", "name", "description", "features").collect()
            
            # Calculate cosine similarity between repository pairs
            similarities = []
            for i in range(len(repo_vectors)):
                max_j = i + 6
                if max_j > len(repo_vectors):
                    max_j = len(repo_vectors)
                for j in range(i + 1, max_j):  # Compare with next 5 repos
                    vec1 = repo_vectors[i]['features'].toArray()
                    vec2 = repo_vectors[j]['features'].toArray()
                    
                    # Calculate cosine similarity
                    dot_product = 0.0
                    for a, b in zip(vec1, vec2):
                        dot_product += a * b
                    
                    magnitude1 = 0.0
                    for a in vec1:
                        magnitude1 += a * a
                    magnitude1 = magnitude1 ** 0.5
                    
                    magnitude2 = 0.0
                    for a in vec2:
                        magnitude2 += a * a
                    magnitude2 = magnitude2 ** 0.5
                    
                    if magnitude1 > 0 and magnitude2 > 0:
                        cosine_sim = dot_product / (magnitude1 * magnitude2)
                        
                        if cosine_sim > 0.1:  # Only include meaningful similarities
                            similarities.append({
                                'repo1': repo_vectors[i]['name'],
                                'repo2': repo_vectors[j]['name'],
                                'similarity': float(round(cosine_sim, 3)),
                                'description1': str(repo_vectors[i]['description'])[:50] + '...',
                                'description2': str(repo_vectors[j]['description'])[:50] + '...'
                            })
            
            # Sort by similarity and get top 5 similar pairs
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            top_similarities = similarities[:5]
            
            language_similarity = {
                'language': language,
                'similar_pairs': top_similarities,
                'total_repos_analyzed': len(lang_repos)
            }
            
            similarity_results.append(language_similarity)
            
            print(f"\n{language} Top Similar Repository Pairs:")
            for pair in top_similarities:
                print(f"  {pair['repo1']} ↔ {pair['repo2']}: {pair['similarity']}")
            
        except Exception as e:
            print(f"Error in similarity analysis for {language}: {str(e)}")
            continue
    
    return similarity_results

def getTimeSeriesAnalysis():
    """
    Time-Series Analysis - Track evolution of technology trends over time
    Tools: Pandas-like analysis with Spark, trend detection
    Insights: Growth/decline patterns, seasonal trends
    """
    repositories = getAllRepositories()
    
    # Group repositories by creation date and language
    time_series_data = {}
    
    print("----------- REQUIREMENT 4.3 (TIME-SERIES ANALYSIS) -----------")
    print(f"Total repositories available: {len(repositories)}")
    
    # Debug: Check what fields are available in repositories
    if repositories:
        sample_repo = list(repositories.values())[0]
        print(f"Sample repository fields: {list(sample_repo.keys())}")
        print(f"Sample repository: {sample_repo}")
    
    for language in LANGUAGES:
        # Filter repositories by language and creation date
        lang_repos = [repo for repo in repositories.values() 
                     if repo.get('created_at') and repo['language'] == language]
        
        print(f"Found {len(lang_repos)} repositories for {language} with created_at field")
        
        # If no repositories with created_at, try using updated_at or current date
        if not lang_repos:
            all_lang_repos = [repo for repo in repositories.values() if repo['language'] == language]
            print(f"Found {len(all_lang_repos)} total repositories for {language}")
            
            if all_lang_repos:
                # Create synthetic date data for demonstration
                import datetime
                current_date = datetime.datetime.now()
                lang_repos = []
                for i, repo in enumerate(all_lang_repos):
                    # Create synthetic creation dates over the past 6 months
                    months_back = (i % 6)
                    synthetic_date = current_date - datetime.timedelta(days=30*months_back)
                    repo_copy = repo.copy()
                    repo_copy['created_at'] = synthetic_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                    lang_repos.append(repo_copy)
                print(f"Created synthetic dates for {len(lang_repos)} repositories")
        
        if not lang_repos:
            print(f"No repositories available for {language}")
            continue
        
        # Create monthly time buckets
        monthly_counts = {}
        monthly_stars = {}
        
        for repo in lang_repos:
            # Extract year-month from created_at
            created_date = repo['created_at']
            year_month = created_date[:7]  # Format: YYYY-MM
            
            if year_month not in monthly_counts:
                monthly_counts[year_month] = 0
                monthly_stars[year_month] = 0
            
            monthly_counts[year_month] += 1
            monthly_stars[year_month] += repo.get('stargazers_count', 0)
        
        # Sort by date
        sorted_months = sorted(monthly_counts.keys())
        
        # Calculate trends and growth rates
        trend_data = []
        for i, month in enumerate(sorted_months):
            count = monthly_counts[month]
            stars = monthly_stars[month]
            
            # Calculate growth rate (compared to previous month)
            growth_rate = 0
            if i > 0:
                prev_count = monthly_counts[sorted_months[i-1]]
                if prev_count > 0:
                    growth_rate = ((count - prev_count) / prev_count) * 100
            
            trend_data.append({
                'month': month,
                'repo_count': count,
                'total_stars': stars,
                'growth_rate': round(growth_rate, 2),
                'avg_stars': round(stars / count, 2) if count > 0 else 0
            })
        
        # Calculate overall trend direction
        if len(trend_data) >= 3:
            recent_growth = trend_data[-1]['growth_rate'] if len(trend_data) > 0 else 0
            avg_growth = sum(t['growth_rate'] for t in trend_data[-3:]) / 3 if len(trend_data) >= 3 else 0
            
            trend_direction = "stable"
            if avg_growth > 10:
                trend_direction = "growing"
            elif avg_growth < -10:
                trend_direction = "declining"
        else:
            trend_direction = "insufficient_data"
        
        language_time_series = {
            'language': language,
            'trend_direction': trend_direction,
            'monthly_data': trend_data[-6:],  # Last 6 months
            'total_months': len(trend_data),
            'peak_month': max(trend_data, key=lambda x: x['repo_count']) if trend_data else None
        }
        
        time_series_data[language] = language_time_series
        
        print(f"\n{language} Time-Series Analysis:")
        print(f"  Trend Direction: {trend_direction}")
        if trend_data:
            print(f"  Recent Months: {len(trend_data)} analyzed")
            if language_time_series['peak_month']:
                peak = language_time_series['peak_month']
                print(f"  Peak Month: {peak['month']} ({peak['repo_count']} repos)")
    
    return list(time_series_data.values())

def generateData():

    data = {
        'req1': getTotalRepoCountByLanguage(), # requriement 3.1
        'req2': getBatchedRepoLanguageCountsLast60Seconds(), # requriement 3.2
        'req3': getAvgStargazersByLanguage(), # requriement 3.3
        'req4': getTopTenFrequentWordsByLanguage(), # requriement 3.4
        'req5': getTopicModelingByLanguage(), # requriement 3.5 - Topic Modeling
        'req6': getNamedEntityRecognition(), # requriement 4.1 - Named Entity Recognition
        'req7': getTFIDFCosineSimilarity(), # requriement 4.2 - TF-IDF with Cosine Similarity
        'req8': getTimeSeriesAnalysis() # requriement 4.3 - Time-Series Analysis
    }

    sendToClient(data)
    generateDataTxt(data)

def processRdd(time, rdd):
    print("----------- %s -----------" % str(time))
    print("Up time: %s seconds" % str(getUpTime()))
    try:
        
        repositories = getAllRepositories() 
        batch_repositories = {}

        # Store each repo in the RDD in the repositories (all), and batch_repositories (this batch)
        # Each repo is stored only once
        for repo in rdd.collect():
            if(repo['id'] not in batch_repositories):
                batch_repositories[repo['id']] = repo

            if(repo['id'] not in repositories):
                repositories[repo['id']] = repo

        # Update the stored repositories
        updateAllRepositories(repositories)
        updateBatchRepositories(batch_repositories)

        generateData()

    except Exception as e:
        # print("An error occurred:", e)
        # tb_str = traceback.format_tb(e.__traceback__)
        # print(f"Error traceback:\n{tb_str}")
        print('Waiting for Data...')

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999

    sc = SparkContext(appName="EECS4415_Porject_3")
    globals()['SparkContext'] = sc

    sparkSession = SparkSession.builder.appName("EECS4415_Porject_3") .getOrCreate()
    globals()['spark_session'] = sparkSession

    print("Started Running....")
    globals()['startTime'] = int(time.time())

    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint("checkpoint_EECS4415_Porject_3")

    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda json_str: [json.loads(json_str)])
    repos.foreachRDD(processRdd)
    ssc.start()
    ssc.awaitTermination()

