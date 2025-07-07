from flask import Flask, request, jsonify
import praw
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Any
import logging
import time

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
            user_agent=REDDIT_USER_AGENT
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
        """Analyze a single subreddit"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            subscribers = subreddit.subscribers
            date_threshold = datetime.utcnow() - timedelta(days=days)
            
            post_count = 0
            total_score = 0
            total_comments = 0
            
            for post in subreddit.new(limit=1000):
                post_date = datetime.utcfromtimestamp(post.created_utc)
                if post_date < date_threshold:
                    break
                
                post_count += 1
                total_score += post.score
                total_comments += post.num_comments
            
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
            return {'success': False, 'error': str(e)}
    
    def find_subreddits(self, query: str, limit: int = 10) -> List[str]:
        """Search for subreddits by niche"""
        try:
            return [sub.display_name for sub in self.reddit.subreddits.search(query, limit=limit)]
        except Exception as e:
            logging.error(f"Error searching subreddits: {e}")
            return []

# Initialize analyzer
analyzer = RedditAnalyzer()

# API Endpoints for n8n
@app.route('/analyze', methods=['POST'])
def analyze_endpoint():
    """
    Endpoint to analyze a single subreddit
    Expected JSON: {"subreddit": "name", "days": 30}
    """
    data = request.json
    subreddit = data.get('subreddit')
    days = data.get('days', 30)
    
    if not subreddit:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    result = analyzer.analyze_subreddit(subreddit, days)
    return jsonify(result)

@app.route('/analyze-multiple', methods=['POST'])
def analyze_multiple_endpoint():
    """
    Endpoint to analyze multiple subreddits
    Expected JSON: {"subreddits": ["sub1", "sub2"], "days": 30}
    """
    data = request.json
    subreddits = data.get('subreddits', [])
    days = data.get('days', 30)
    
    if not subreddits:
        return jsonify({'success': False, 'error': 'No subreddits provided'}), 400
    
    results = []
    for sub in subreddits:
        time.sleep(1)  # Rate limiting
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
    """
    Search for subreddits by niche
    Expected JSON: {"query": "machine learning", "limit": 10}
    """
    data = request.json
    query = data.get('query')
    limit = data.get('limit', 10)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    subreddits = analyzer.find_subreddits(query, limit)
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(subreddits),
        'subreddits': subreddits
    })

@app.route('/search-and-analyze', methods=['POST'])
def search_and_analyze_endpoint():
    """
    Search for subreddits and analyze them in one go
    Expected JSON: {"query": "cryptocurrency", "limit": 10, "days": 30}
    """
    data = request.json
    query = data.get('query')
    limit = data.get('limit', 10)
    days = data.get('days', 30)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # Search for subreddits
    subreddits = analyzer.find_subreddits(query, limit)
    
    # Analyze each one
    results = []
    for sub in subreddits:
        time.sleep(1)  # Rate limiting
        result = analyzer.analyze_subreddit(sub, days)
        if result['success']:
            results.append(result)
    
    # Sort by effectiveness
    results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
    
    # Format for Telegram
    telegram_message = format_for_telegram(query, results)
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(results),
        'results': results,
        'telegram_message': telegram_message
    })

def format_for_telegram(query: str, results: List[Dict]) -> str:
    """Format results for Telegram message"""
    if not results:
        return f"❌ No subreddits found for '{query}'"
    
    message = f"📊 *Reddit Analysis for '{query}'*\n"
    message += f"_Analyzed {len(results)} subreddits_\n\n"
    
    for i, r in enumerate(results[:5], 1):  # Top 5
        emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📌"
        message += f"{emoji} *r/{r['subreddit']}*\n"
        message += f"   • Effectiveness: *{r['effectiveness_score']}*\n"
        message += f"   • Subscribers: {r['subscribers']:,}\n"
        message += f"   • Avg Score: {r['avg_score_per_post']}\n"
        message += f"   • Avg Comments: {r['avg_comments_per_post']}\n"
        message += f"   • Posts/Day: {r['avg_posts_per_day']}\n\n"
    
    return message

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring"""
    return jsonify({'status': 'healthy', 'service': 'reddit-analyzer'})

if __name__ == '__main__':
    # Run on port 5000 for local development
    # In production, use gunicorn: gunicorn reddit_api:app
    app.run(host='0.0.0.0', port=5000, debug=True)
