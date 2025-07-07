from flask import Flask, request, jsonify
import praw
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Any
import logging
import time
import re
import random

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration from environment variables
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', 'your_client_id')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', 'your_client_secret')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'RedditAnalyzer/1.0')

class RedditAnalyzer:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            ratelimit_seconds=300
        )
    
    def calculate_effectiveness(self, avg_posts_per_day: float, avg_score_per_post: float,
                              avg_comments_per_post: float, subscribers: int) -> float:
        """Calculate effectiveness score with the custom formula"""
        # Post frequency score (inverse relationship)
        if avg_posts_per_day <= 0:
            post_freq_score = 0
        elif avg_posts_per_day <= 10:
            post_freq_score = 100 * (1 - (avg_posts_per_day - 1) / 9)
        else:
            post_freq_score = max(0, 100 - (avg_posts_per_day - 10) * 5)
        
        # Score normalization
        score_normalized = min(100, (avg_score_per_post / 1000) * 100)
        
        # Comments normalization
        comments_normalized = min(100, (avg_comments_per_post / 100) * 100)
        
        # Subscriber modifier
        if subscribers < 10000:
            subscriber_modifier = 1.2
        elif subscribers < 100000:
            subscriber_modifier = 1.0
        elif subscribers < 1000000:
            subscriber_modifier = 0.9
        else:
            subscriber_modifier = 0.8
        
        effectiveness = (
            post_freq_score * 0.3 +
            score_normalized * 0.35 +
            comments_normalized * 0.35
        ) * subscriber_modifier
        
        return min(100, max(0, effectiveness))
    
    def analyze_subreddit(self, subreddit_name: str, days: int = 30) -> Dict[str, Any]:
        """Analyze a single subreddit with better error handling"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            subscribers = subreddit.subscribers
            date_threshold = datetime.utcnow() - timedelta(days=days)
            
            post_count = 0
            total_score = 0
            total_comments = 0
            
            # Reduced limit to avoid timeouts
            for post in subreddit.new(limit=500):
                post_date = datetime.utcfromtimestamp(post.created_utc)
                if post_date < date_threshold:
                    break
                
                post_count += 1
                total_score += post.score
                total_comments += post.num_comments
                
                # Add small delay to respect rate limits
                if post_count % 50 == 0:
                    time.sleep(1)
            
            avg_posts_per_day = post_count / days
            avg_score_per_post = total_score / post_count if post_count > 0 else 0
            avg_comments_per_post = total_comments / post_count if post_count > 0 else 0
            
            effectiveness = self.calculate_effectiveness(
                avg_posts_per_day, avg_score_per_post, 
                avg_comments_per_post, subscribers
            )
            
            return {
                'success': True,
                'subreddit': subreddit_name,
                'subscribers': subscribers,
                'avg_posts_per_day': round(avg_posts_per_day, 2),
                'avg_score_per_post': round(avg_score_per_post, 2),
                'avg_comments_per_post': round(avg_comments_per_post, 2),
                'effectiveness_score': round(effectiveness, 2)
            }
        except Exception as e:
            logging.error(f"Error analyzing subreddit {subreddit_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def find_subreddits(self, query: str, limit: int = 50, search_type: str = "search") -> List[Dict[str, Any]]:
        """Search for subreddits with different strategies based on search type"""
        try:
            subreddits = []
            
            if search_type == "niche":
                # For niche: Focus on smaller, more targeted communities
                for sub in self.reddit.subreddits.search(query, limit=limit*2):
                    try:
                        # Filter for smaller, more engaged communities
                        if sub.subscribers and 1000 <= sub.subscribers <= 500000:
                            subreddits.append({
                                'name': sub.display_name,
                                'subscribers': sub.subscribers or 0,
                                'description': sub.public_description[:100] if sub.public_description else "",
                                'type': 'niche'
                            })
                        time.sleep(0.1)
                    except Exception as e:
                        continue
                
                # Sort by engagement potential (smaller but active communities)
                subreddits.sort(key=lambda x: x['subscribers'], reverse=False)
                
            else:
                # For search: Focus on popular, established communities
                for sub in self.reddit.subreddits.search(query, limit=limit):
                    try:
                        subreddits.append({
                            'name': sub.display_name,
                            'subscribers': sub.subscribers or 0,
                            'description': sub.public_description[:100] if sub.public_description else "",
                            'type': 'popular'
                        })
                        time.sleep(0.1)
                    except Exception as e:
                        continue
                
                # Sort by popularity (largest communities first)
                subreddits.sort(key=lambda x: x['subscribers'], reverse=True)
            
            return subreddits[:limit]
            
        except Exception as e:
            logging.error(f"Error searching subreddits: {e}")
            return []

    def get_random_subreddits(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get randomized subreddit results"""
        try:
            # Get more results than needed
            all_subreddits = []
            for sub in self.reddit.subreddits.search(query, limit=limit*3):
                try:
                    all_subreddits.append({
                        'name': sub.display_name,
                        'subscribers': sub.subscribers or 0,
                        'description': sub.public_description[:100] if sub.public_description else ""
                    })
                    time.sleep(0.1)
                except Exception as e:
                    continue
            
            # Randomize and return subset
            random.shuffle(all_subreddits)
            return all_subreddits[:limit]
            
        except Exception as e:
            logging.error(f"Error getting random subreddits: {e}")
            return []

    def parse_compare_input(self, input_text: str) -> List[str]:
        """Parse flexible compare input formats"""
        # Remove extra spaces and split by various delimiters
        cleaned = re.sub(r'\s+', ' ', input_text.strip())
        
        # Split by comma, space, or other common delimiters
        subreddits = re.split(r'[,\s]+', cleaned)
        
        # Clean up each subreddit name
        return [sub.strip() for sub in subreddits if sub.strip()]

# Initialize analyzer
analyzer = RedditAnalyzer()

# API Endpoints
@app.route('/analyze', methods=['POST'])
def analyze_endpoint():
    """Endpoint to analyze a single subreddit"""
    data = request.json
    subreddit = data.get('subreddit')
    days = data.get('days', 7)
    
    if not subreddit:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    result = analyzer.analyze_subreddit(subreddit, days)
    return jsonify(result)

@app.route('/analyze-multiple', methods=['POST'])
def analyze_multiple_endpoint():
    """Endpoint to analyze multiple subreddits"""
    data = request.json
    subreddits_input = data.get('subreddits', [])
    days = data.get('days', 7)
    
    # Handle flexible input format
    if isinstance(subreddits_input, str):
        subreddits = analyzer.parse_compare_input(subreddits_input)
    else:
        subreddits = subreddits_input
    
    if not subreddits:
        return jsonify({'success': False, 'error': 'No subreddits provided'}), 400
    
    results = []
    for i, sub in enumerate(subreddits):
        if i > 0:
            time.sleep(2)
        
        result = analyzer.analyze_subreddit(sub, days)
        if result['success']:
            results.append(result)
    
    # Sort by effectiveness
    results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
    
    return jsonify({
        'success': True,
        'count': len(results),
        'results': results
    })

@app.route('/search', methods=['POST'])
def search_endpoint():
    """Search for popular subreddits - returns biggest ones first"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 50), 100)
    randomize = data.get('randomize', False)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    if randomize:
        subreddits_data = analyzer.get_random_subreddits(query, limit)
    else:
        subreddits_data = analyzer.find_subreddits(query, limit, "search")
    
    # Return just the names for compatibility
    subreddit_names = [sub['name'] for sub in subreddits_data]
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(subreddit_names),
        'subreddits': subreddit_names,
        'type': 'randomized' if randomize else 'popular'
    })

@app.route('/search-and-analyze', methods=['POST'])
def search_and_analyze_endpoint():
    """Niche analysis - focuses on smaller, more targeted communities"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 20), 20)
    days = data.get('days', 7)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # Use niche search strategy
    subreddits_data = analyzer.find_subreddits(query, limit*2, "niche")
    
    # Analyze top niche subreddits
    results = []
    for i, sub_data in enumerate(subreddits_data[:10]):
        if i > 0:
            time.sleep(2)
            
        result = analyzer.analyze_subreddit(sub_data['name'], days)
        if result['success']:
            results.append(result)
    
    # Sort by effectiveness
    results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(results),
        'results': results,
        'type': 'niche_analysis'
    })

# Health check endpoints
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({'status': 'healthy', 'service': 'reddit-analyzer'})

@app.route('/ping', methods=['GET'])
def ping():
    """Simple ping endpoint to keep service warm"""
    return jsonify({'message': 'pong', 'timestamp': datetime.utcnow().isoformat()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
