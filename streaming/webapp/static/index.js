var UPDATE_INTERVAL = 60

var timeLeft = UPDATE_INTERVAL; // Set initial time

window.onload = function () {
    loadData();
    startUpdateCycle();
}

function fetchUpdatedImage(src, id) {
    var img = document.getElementById(id);
    img.setAttribute("src", src);
}

function loadData() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', '/getData', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4 && xhr.status === 200) {
            var responseData = JSON.parse(xhr.responseText);
            console.log(responseData)

            if (xhr.responseText["msg"] == "no data") {
                return;
            }

            let req1 = responseData['req1'];
            // Loop through the JSON list for requirement 3.1
            for (var i = 0; i < req1.length; i++) {
                let data = req1[i];
                let id = "req1-language" + (i + 1)
                document.getElementById(id).innerHTML = `${data['language']}: ${data['count']}`
            }

            // display plot for requirement 3.2
            fetchUpdatedImage(responseData['req2'] + "?t=" + new Date().getTime(), "req2-plot");

            // display plot for requirement 3.3
            fetchUpdatedImage(responseData['req3'] + "?t=" + new Date().getTime(), "req3-plot");

            let req4 = responseData['req4'];
            // Loop through the JSON list for requirement 3.4
            for (var i = 0; i < req4.length; i++) {
                let data = req4[i];
                let id = "req4-language" + (i + 1)
                let result = `<h4>${data['language']}</h4>`

                for (var j = 0; j < data['top_ten_words'].length; j++) {
                    let word_count = data['top_ten_words'][j]
                    result += `${word_count[0]}, ${word_count[1]} <br>`
                }

                document.getElementById(id).innerHTML = result
            }

            // Display topic modeling results for requirement 3.5
            let req5 = responseData['req5'];
            if (req5 && req5.length > 0) {
                let topicHtml = '';
                for (var i = 0; i < req5.length; i++) {
                    let langData = req5[i];
                    topicHtml += `<div style="margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">`;
                    topicHtml += `<h3 style="color: #333; margin-bottom: 15px;">${langData['language']} - Emerging Topics</h3>`;
                    
                    for (var j = 0; j < langData['topics'].length; j++) {
                        let topic = langData['topics'][j];
                        topicHtml += `<div style="margin-bottom: 10px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">`;
                        topicHtml += `<strong>Topic ${topic['topic_id'] + 1}:</strong> `;
                        topicHtml += `<span style="color: #007bff;">${topic['formatted_words']}</span>`;
                        topicHtml += `</div>`;
                    }
                    
                    topicHtml += `</div>`;
                }
                document.getElementById('req5-topics').innerHTML = topicHtml;
            } else {
                document.getElementById('req5-topics').innerHTML = '<p style="color: #666;">No topic modeling data available yet. Processing more repositories...</p>';
            }

            // Display Named Entity Recognition results for requirement 4.1
            let req6 = responseData['req6'];
            if (req6 && req6.length > 0) {
                let nerHtml = '';
                for (var i = 0; i < req6.length; i++) {
                    let langData = req6[i];
                    nerHtml += `<div style="margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">`;
                    nerHtml += `<h3 style="color: #333; margin-bottom: 15px;">${langData['language']} - Technology Ecosystem</h3>`;
                    
                    for (var entity_type in langData['entities']) {
                        if (langData['entities'][entity_type] && langData['entities'][entity_type].length > 0) {
                            nerHtml += `<div style="margin-bottom: 15px;">`;
                            nerHtml += `<strong style="color: #495057; text-transform: capitalize;">${entity_type}:</strong><br>`;
                            
                            let entities = langData['entities'][entity_type];
                            for (var j = 0; j < entities.length; j++) {
                                let entity = entities[j];
                                nerHtml += `<span style="display: inline-block; margin: 3px; padding: 5px 10px; background-color: #e3f2fd; border-radius: 15px; font-size: 0.9em;">`;
                                nerHtml += `${entity['name']} <span style="color: #666;">(${entity['count']})</span>`;
                                nerHtml += `</span>`;
                            }
                            nerHtml += `</div>`;
                        }
                    }
                    nerHtml += `</div>`;
                }
                document.getElementById('req6-ner').innerHTML = nerHtml;
            } else {
                document.getElementById('req6-ner').innerHTML = '<p style="color: #666;">No NER data available yet. Processing more repositories...</p>';
            }

            // Display TF-IDF Similarity results for requirement 4.2
            let req7 = responseData['req7'];
            if (req7 && req7.length > 0) {
                let similarityHtml = '';
                for (var i = 0; i < req7.length; i++) {
                    let langData = req7[i];
                    similarityHtml += `<div style="margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">`;
                    similarityHtml += `<h3 style="color: #333; margin-bottom: 15px;">${langData['language']} - Similar Repository Pairs</h3>`;
                    similarityHtml += `<p style="color: #666; margin-bottom: 15px;">Analyzed ${langData['total_repos_analyzed']} repositories</p>`;
                    
                    if (langData['similar_pairs'] && langData['similar_pairs'].length > 0) {
                        for (var j = 0; j < langData['similar_pairs'].length; j++) {
                            let pair = langData['similar_pairs'][j];
                            similarityHtml += `<div style="margin-bottom: 15px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; border-left: 4px solid #007bff;">`;
                            similarityHtml += `<div style="font-weight: bold; color: #333; margin-bottom: 8px;">Similarity: ${pair['similarity']}</div>`;
                            similarityHtml += `<div style="margin-bottom: 5px;"><strong>${pair['repo1']}</strong></div>`;
                            similarityHtml += `<div style="font-size: 0.9em; color: #666; margin-bottom: 10px;">${pair['description1']}</div>`;
                            similarityHtml += `<div style="text-align: center; color: #007bff;">↔</div>`;
                            similarityHtml += `<div style="margin-bottom: 5px;"><strong>${pair['repo2']}</strong></div>`;
                            similarityHtml += `<div style="font-size: 0.9em; color: #666;">${pair['description2']}</div>`;
                            similarityHtml += `</div>`;
                        }
                    } else {
                        similarityHtml += `<p style="color: #666;">No similar repository pairs found</p>`;
                    }
                    similarityHtml += `</div>`;
                }
                document.getElementById('req7-similarity').innerHTML = similarityHtml;
            } else {
                document.getElementById('req7-similarity').innerHTML = '<p style="color: #666;">No similarity data available yet. Processing more repositories...</p>';
            }

            // Display Time-Series Analysis results for requirement 4.3
            let req8 = responseData['req8'];
            if (req8 && req8.length > 0) {
                let timeSeriesHtml = '';
                for (var i = 0; i < req8.length; i++) {
                    let langData = req8[i];
                    timeSeriesHtml += `<div style="margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">`;
                    timeSeriesHtml += `<h3 style="color: #333; margin-bottom: 15px;">${langData['language']} - Technology Trends</h3>`;
                    timeSeriesHtml += `<div style="margin-bottom: 15px;">`;
                    timeSeriesHtml += `<span style="display: inline-block; padding: 5px 15px; background-color: ${langData['trend_color']}; color: white; border-radius: 20px; font-weight: bold;">`;
                    timeSeriesHtml += `Trend: ${langData['trend_direction'].toUpperCase()}`;
                    timeSeriesHtml += `</span>`;
                    timeSeriesHtml += `</div>`;
                    
                    if (langData['monthly_data'] && langData['monthly_data'].length > 0) {
                        timeSeriesHtml += `<div style="margin-bottom: 15px;">`;
                        timeSeriesHtml += `<h4 style="color: #495057; margin-bottom: 10px;">Recent Monthly Data:</h4>`;
                        for (var j = 0; j < langData['monthly_data'].length; j++) {
                            let month = langData['monthly_data'][j];
                            timeSeriesHtml += `<div style="margin-bottom: 8px; padding: 8px; background-color: #f8f9fa; border-radius: 5px;">`;
                            timeSeriesHtml += `<strong>${month['month']}:</strong> ${month['repo_count']} repos, `;
                            timeSeriesHtml += `${month['total_stars']} stars `;
                            if (month['growth_rate'] > 0) {
                                timeSeriesHtml += `<span style="color: green;">↑${month['growth_rate']}%</span>`;
                            } else if (month['growth_rate'] < 0) {
                                timeSeriesHtml += `<span style="color: red;">↓${Math.abs(month['growth_rate'])}%</span>`;
                            } else {
                                timeSeriesHtml += `<span style="color: #666;">→0%</span>`;
                            }
                            timeSeriesHtml += `</div>`;
                        }
                        timeSeriesHtml += `</div>`;
                    }
                    
                    if (langData['peak_month']) {
                        let peak = langData['peak_month'];
                        timeSeriesHtml += `<div style="padding: 10px; background-color: #fff3cd; border-radius: 5px; border-left: 4px solid #ffc107;">`;
                        timeSeriesHtml += `<strong>Peak Month:</strong> ${peak['month']} with ${peak['repo_count']} repositories`;
                        timeSeriesHtml += `</div>`;
                    }
                    
                    timeSeriesHtml += `</div>`;
                }
                document.getElementById('req8-time-series').innerHTML = timeSeriesHtml;
            } else {
                document.getElementById('req8-time-series').innerHTML = '<p style="color: #666;">No time-series data available yet. Processing more repositories...</p>';
            }

        }
    };
    xhr.send();
}

// this function handles updating the data on a 60 second interval 
// It reduces the timeLeft by one every 1 second untill timeLeft reaches 0 
// Which then the loadData() is called and TimeLeft is reset to 60
function startUpdateCycle() {
    countdownTimer = setInterval(function () {
        timeLeft--;
        document.getElementById("countdown").innerHTML = "Updating Data in: " + timeLeft + " seconds";
        if (timeLeft <= 0) {
            clearInterval(countdownTimer);
            timeLeft = UPDATE_INTERVAL;
            loadData();
            setTimeout(startUpdateCycle, 1000);
        }
    }, 1000);  //1000 milliseconds = 1 second
}