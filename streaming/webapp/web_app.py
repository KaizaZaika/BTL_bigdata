from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

# Creates the plot for requriement 3.2 
def createReq2Plot(data):
    plt.cla()
    
    # A list stores all the batch times 
    batch_times = []

    # a dictionary of each langauge counts
    # Each element in the dictionary has the following format
    # {
    #   'language': x, --> The programming language
    #   'count': [],   --> List of the count for this programming langauge for each batch, i.e count[i] is the count for this langauge for the ith batch 
    # }
    counts_by_language = {}

    for repo in data:
        batch_time = repo['batch_time']
        language = repo['language']
        count = repo['count']

        if batch_time not in batch_times:
            batch_times.append(batch_time)

        if language not in counts_by_language:
            counts_by_language[language] = []

        counts_by_language[language].append(count)
    
    for language, counts in counts_by_language.items():
        plt.plot(batch_times, counts, label=language, marker='o')

    plt.xlabel('Time')
    plt.ylabel('#Repositories')
    plt.legend()
    plt.savefig('/streaming/webapp/static/chart_req2.png')

    return '/static/chart_req2.png'

# creates the var graph for requriement 3.3 
def createReq3Plot(data):
    plt.cla()
    languages = []
    avg_stars = []

    for d in data:
        languages.append(d['language'])
        avg_stars.append(d['avg_stargazers_count'])

    plt.bar(languages, avg_stars)

    # Add labels and title
    plt.xlabel('Programming Language')
    plt.ylabel('Average number of stars')
    plt.savefig('/streaming/webapp/static/bar_req3.png')    

    return '/static/bar_req3.png'

# Creates topic modeling visualization for requirement 3.5
def createTopicModelingDisplay(data):
    """
    Process topic modeling data for display
    Returns formatted topic data for frontend
    """
    if not data:
        return []
    
    formatted_topics = []
    
    for lang_data in data:
        language = lang_data['language']
        topics = lang_data['topics']
        
        language_topics = {
            'language': language,
            'topics': []
        }
        
        for topic in topics:
            topic_display = {
                'topic_id': topic['topic_id'],
                'top_words': topic['top_words'],
                'formatted_words': ', '.join(topic['top_words'][:3])  # Show top 3 words
            }
            language_topics['topics'].append(topic_display)
        
        formatted_topics.append(language_topics)
    
    return formatted_topics 

def createNERDisplay(data):
    """
    Process Named Entity Recognition data for display
    Returns formatted NER data for frontend
    """
    if not data:
        return []
    
    formatted_ner = []
    
    for lang_data in data:
        language = lang_data['language']
        entities = lang_data['entities']
        
        language_ner = {
            'language': language,
            'entities': {}
        }
        
        for entity_type, entity_list in entities.items():
            if entity_list:  # Only include entity types that have results
                language_ner['entities'][entity_type] = entity_list
        
        formatted_ner.append(language_ner)
    
    return formatted_ner

def createSimilarityDisplay(data):
    """
    Process TF-IDF Cosine Similarity data for display
    Returns formatted similarity data for frontend
    """
    if not data:
        return []
    
    formatted_similarity = []
    
    for lang_data in data:
        language = lang_data['language']
        similar_pairs = lang_data['similar_pairs']
        total_repos = lang_data['total_repos_analyzed']
        
        language_similarity = {
            'language': language,
            'similar_pairs': similar_pairs,
            'total_repos_analyzed': total_repos
        }
        
        formatted_similarity.append(language_similarity)
    
    return formatted_similarity

def createTimeSeriesDisplay(data):
    """
    Process Time-Series Analysis data for display
    Returns formatted time series data for frontend
    """
    if not data:
        return []
    
    formatted_time_series = []
    
    for lang_data in data:
        language = lang_data['language']
        trend_direction = lang_data['trend_direction']
        monthly_data = lang_data['monthly_data']
        peak_month = lang_data['peak_month']
        
        language_time_series = {
            'language': language,
            'trend_direction': trend_direction,
            'trend_color': 'green' if trend_direction == 'growing' else 'red' if trend_direction == 'declining' else 'orange',
            'monthly_data': monthly_data,
            'peak_month': peak_month
        }
        
        formatted_time_series.append(language_time_series)
    
    return formatted_time_series 

# This endpoint is for Spark to be able to update the data
@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

# This endpoint is for the webapp client to fetch the data
@app.route('/getData', methods=['GET'])
def getData():
    r = Redis(host='redis', port=6379)
    data = r.get('data')

    try:
        data = json.loads(data)
    except TypeError:
        return jsonify({'msg': 'no data'})
    
    
    result = {
        'req1': data['req1'],
        'req2': createReq2Plot(data['req2']),
        'req3': createReq3Plot(data['req3']),
        'req4': data['req4'],
        'req5': createTopicModelingDisplay(data.get('req5', [])),
        'req6': createNERDisplay(data.get('req6', [])),
        'req7': createSimilarityDisplay(data.get('req7', [])),
        'req8': createTimeSeriesDisplay(data.get('req8', []))
    }

    return jsonify(result)

# This endpoint is initally render the web page 
@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')