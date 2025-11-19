**Title:** Real-Time Streaming Analytics with Apache Spark and Python   

## Objective
This project involves designing and implementing a big data system that performs real-time streaming analytics for public repositories hosted on GitHub. The system runs a stream processing pipeline, where the live data stream to be analyzed is coming from GitHub API. An Apache Spark cluster processes the data stream. A web application receives the output from Spark and visualizes the analysis result. This project follows the requirements to write codes (Python, Bash, YAML scripts, Dockerfiles) that implement such a streaming process pipeline, which is a multi-container system based on Docker and Docker Compose.

## Implementation
* All scripts are written using Python >= 3.7.0.

## Objective
The goal of this project is to develop a robust big data system that can perform real-time streaming analytics on GitHub repositories. The system leverages Apache Spark and Python to process live data from the GitHub API, providing insights into the world of open-source software.

## Key Features
- **Data Collection**: The system collects data from GitHub using the GitHub API, focusing on repositories written in three programming languages (Python, Java, and Javascript).
- **Real-time Processing**: Apache Spark is used to process the incoming data in real-time, enabling rapid analysis and insights.
- **Visualization**: A web application presents the analysis results in real-time through intuitive visualizations.

## Details
1. Three programming languages are selected as a focus: Python, Java, and Javascript.

2. A data source service using Python, which collects information about the most recently-pushed repositories that use any of the three programming languages as the primary coding language through GitHub API. The Python scripts collects and pushes the new data to Spark at an interval of around 15 seconds, which means that every 15 seconds, the scripts feed Spark with the latest data.

3. A Python script (streaming application) for Spark streaming (`spark_app.py` in the `streaming` folder). The application receives the streaming data, divides it into batches at an interval of 60 seconds (batch duration is 60 seconds), and performs the following four analysis tasks.
   1. Computes the total number of the collected repositories since the start of the streaming application for each of the three programming languages. Each repository is counted only once.
   2. Computes the number of the collected repositories with changes pushed during the last 60 seconds. Each repository is counted only once during a batch interval (60 seconds).
   3. Computes the average number of stars of all the collected repositories since the start of the streaming application for each of the three programming languages. Each repository counts towards the result only once.
   4. Finds the top 10 most frequent words in the description of all the collected repositories since the start of the streaming application for each of the three programming languages. Each repository counts towards the result only once.

4. A web service listening on port 5000, which receives the analysis results from Spark and visualizes them in real-time. The web service runs a dashboard web application that includes:
    1. Three numbers that tell the total number of the collected repositories since the start of the streaming application for each of the three programming languages in real-time (requirement 3(i)). The numbers are updated every 60 seconds.
    2. A real-time line chart that shows the number of the recently-pushed repositories during each batch interval (60 seconds) for each of the three programming languages (requirement 3(ii)).
    3. A real-time bar plot that shows the average number of stars of all the collected repositories since the start of the streaming application for each of the three programming languages (requirement 3(iii)). 
    4. Three lists that contain the top 10 most frequent words in the description of all the collected repositories since the start of the streaming application and the number of occurrences of each word, sorted from the most frequent to the least, for each of the three programming languages in real-time (requirement 3(iv)). The lists are updated every 60 seconds.

5. All components of the data streaming pipeline (i.e., data source service, Spark cluster, and web service) are containerized with Docker and Docker Compose. The system can be up and running using the following commands:
    ```
    $ git clone https://github.com/KaizaZaika/BTL_bigdata.git
    $ cd BTL_bigdata
    $ cd streaming
    $ docker-compose build -d
    $ docker exec streaming-spark-1 /opt/spark/bin/spark-submit /streaming/spark_app.py
    ```
    The web application with the real-time charts is on port 5000 of the Docker host.


