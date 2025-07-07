from flask import Flask, request, jsonify
import praw
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Any, Set
import logging
import time
import re
from collections import defaultdict
import requests

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
        
        # Blacklist of generic subreddits to filter out
        self.generic_subreddits = {
            'funny', 'pics', 'gifs', 'videos', 'askreddit', 'todayilearned',
            'worldnews', 'news', 'aww', 'gaming', 'movies', 'music',
            'television', 'books', 'art', 'food', 'jokes', 'tifu',
            'showerthoughts', 'iama', 'all', 'popular', 'random',
            'teenagers', 'askouija', 'polandball', 'askfrance', 'egg_irl'
        }
        
        # Category mappings for better search results
        self.category_keywords = {
            'technology': ['technology', 'tech', 'gadgets', 'programming', 'coding', 'software'],
            'finance': ['finance', 'investing', 'stocks', 'crypto', 'money', 'trading'],
            'fitness': ['fitness', 'gym', 'workout', 'exercise', 'health', 'nutrition'],
            'gaming': ['gaming', 'games', 'pcgaming', 'console', 'esports'],
            'fashion': ['fashion', 'style', 'clothing', 'streetwear', 'sneakers'],
            'food': ['food', 'cooking', 'recipes', 'foodporn', 'baking'],
            'cars': ['cars', 'autos', 'vehicles', 'racing', 'motorcycles'],
            'sports': ['sports', 'football', 'basketball', 'soccer', 'baseball'],
            'music': ['music', 'hiphop', 'rock', 'electronic', 'jazz', 'metal'],
            'art': ['art', 'drawing', 'painting', 'digital', 'illustration'],
            'science': ['science', 'physics', 'chemistry', 'biology', 'space'],
            'travel': ['travel', 'backpacking', 'solo', 'destinations', 'tourism'],
            'photography': ['photography', 'photos', 'cameras', 'photocritique'],
            'movies': ['movies', 'films', 'cinema', 'moviesuggestions'],
            'books': ['books', 'reading', 'literature', 'booksuggestions'],
            'anime': ['anime', 'manga', 'animemes', 'animesuggest'],
            'politics': ['politics', 'political', 'news', 'worldpolitics'],
            'business': ['business', 'entrepreneur', 'startup', 'smallbusiness'],
            'education': ['education', 'learning', 'study', 'college', 'university'],
            'relationships': ['relationships', 'dating', 'advice', 'love'],
            'pets': ['pets', 'dogs', 'cats', 'animals', 'aww'],
            'diy': ['diy', 'crafts', 'woodworking', 'makers', 'howto'],
            'beauty': ['beauty', 'makeup', 'skincare', 'hair', 'cosmetics'],
            'parenting': ['parenting', 'parents', 'mommit', 'daddit', 'babies'],
            'mental_health': ['mentalhealth', 'anxiety', 'depression', 'therapy', 'wellness']
        }
    
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
    
    def detect_category(self, query: str) -> List[str]:
        """Detect if query matches any known categories"""
        query_lower = query.lower()
        matched_categories = []
        
        for category, keywords in self.category_keywords.items():
            for keyword in keywords:
                if keyword in query_lower or query_lower in keyword:
                    matched_categories.append(category)
                    break
        
        return matched_categories
    
    def get_top_subreddits_by_category(self, categories: List[str], limit: int = 50) -> List[Dict[str, Any]]:
        """Get top subreddits for specific categories"""
        subreddits = {}
        
        # Predefined top subreddits for common categories
        category_subs = {
            'technology': ['technology', 'gadgets', 'tech', 'futurology', 'android', 'apple', 'hardware'],
            'finance': ['personalfinance', 'investing', 'stocks', 'cryptocurrency', 'wallstreetbets', 'financialindependence'],
            'fitness': ['fitness', 'gym', 'bodybuilding', 'running', 'weightlifting', 'yoga', 'crossfit'],
            'gaming': ['gaming', 'pcgaming', 'ps5', 'xbox', 'nintendoswitch', 'steam', 'gamingsuggestions'],
            'crypto': ['cryptocurrency', 'bitcoin', 'ethereum', 'cryptomarkets', 'altcoin', 'defi', 'cryptomoonshots'],
            'programming': ['programming', 'learnprogramming', 'webdev', 'javascript', 'python', 'coding'],
            'fashion': ['malefashionadvice', 'femalefashionadvice', 'streetwear', 'sneakers', 'fashionreps'],
            'cars': ['cars', 'autos', 'carporn', 'projectcar', 'whatcarshouldibuy', 'mechanicadvice'],
            'photography': ['photography', 'photocritique', 'itookapicture', 'cameras', 'analogcommunity'],
            'anime': ['anime', 'animemes', 'animesuggest', 'manga', 'animeirl', 'wholesomeanimemes'],
            'food': ['food', 'cooking', 'recipes', 'foodporn', 'mealprepsunday', 'baking', 'eatcheapandhealthy']
        }
        
        for category in categories:
            if category in category_subs:
                for sub_name in category_subs[category]:
                    try:
                        sub = self.reddit.subreddit(sub_name)
                        subreddits[sub_name] = {
                            'name': sub.display_name,
                            'subscribers': sub.subscribers or 0,
                            'relevance': 100,  # High relevance for curated lists
                            'description': sub.public_description[:100] if sub.public_description else ""
                        }
                    except:
                        continue
        
        return list(subreddits.values())
    
    def smart_search_subreddits(self, query: str, limit: int = 50, mode: str = 'search') -> List[Dict[str, Any]]:
        """Smart search that finds truly related subreddits"""
        try:
            all_subreddits = {}
            query_lower = query.lower()
            
            # First, check if this matches any known categories
            categories = self.detect_category(query)
            if categories:
                category_subs = self.get_top_subreddits_by_category(categories, limit)
                for sub in category_subs:
                    all_subreddits[sub['name']] = sub
            
            # Clean up the query for better search
            # Remove common words that lead to bad results
            stop_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
            query_terms = [term for term in query_lower.split() if term not in stop_words]
            clean_query = ' '.join(query_terms)
            
            # Method 1: Search for exact subreddit names first
            exact_searches = [
                query,
                query.replace(' ', ''),  # nospace
                query.replace(' ', '_'),  # underscore
                clean_query,
                clean_query.replace(' ', '')
            ]
            
            for search_term in exact_searches:
                try:
                    # Try to get subreddit directly
                    sub = self.reddit.subreddit(search_term)
                    if sub.subscribers and sub.subscribers > 100:  # Must have some subscribers
                        all_subreddits[sub.display_name] = {
                            'name': sub.display_name,
                            'subscribers': sub.subscribers,
                            'relevance': 100,
                            'description': sub.public_description[:100] if sub.public_description else ""
                        }
                except:
                    pass
            
            # Method 2: Reddit search with strict filtering
            search_limit = min(limit * 3, 300)  # Search more to filter down
            
            for sub in self.reddit.subreddits.search(clean_query, limit=search_limit):
                try:
                    sub_name_lower = sub.display_name.lower()
                    
                    # Skip if already found or generic
                    if sub.display_name in all_subreddits or sub_name_lower in self.generic_subreddits:
                        continue
                    
                    # Skip if subscriber count is too low
                    if not sub.subscribers or sub.subscribers < 1000:
                        continue
                    
                    # Calculate relevance more strictly
                    relevance_score = 0
                    name_matches = 0
                    
                    for term in query_terms:
                        if len(term) > 2:  # Skip very short terms
                            # Exact word match in name (not just substring)
                            if term in sub_name_lower.split('_') or term in re.split(r'(?=[A-Z])', sub.display_name):
                                relevance_score += 20
                                name_matches += 1
                            # Substring match in name
                            elif term in sub_name_lower:
                                relevance_score += 10
                                name_matches += 1
                            # Match in title (first few words matter more)
                            title_words = (sub.title or '').lower().split()[:10]
                            if term in title_words:
                                relevance_score += 5 - title_words.index(term) * 0.5
                    
                    # Require at least partial name match for most queries
                    if name_matches > 0 or relevance_score >= 15:
                        all_subreddits[sub.display_name] = {
                            'name': sub.display_name,
                            'subscribers': sub.subscribers,
                            'relevance': relevance_score,
                            'description': sub.public_description[:100] if sub.public_description else ""
                        }
                    
                except Exception as e:
                    logging.warning(f"Error processing subreddit: {e}")
                    continue
            
            # Convert to list
            results = list(all_subreddits.values())
            
            # Remove duplicates and sort
            seen = set()
            unique_results = []
            for r in results:
                if r['name'] not in seen:
                    seen.add(r['name'])
                    unique_results.append(r)
            
            if mode == 'search':
                # For /search: Sort by subscribers (largest first)
                unique_results.sort(key=lambda x: x['subscribers'], reverse=True)
            else:  # mode == 'niche'
                # For /niche: Find sweet spot - active but not too large
                for sub in unique_results:
                    subs = sub['subscribers']
                    if subs < 10000:
                        sub['niche_score'] = subs / 10000 * 50
                    elif subs < 100000:
                        sub['niche_score'] = 90
                    elif subs < 500000:
                        sub['niche_score'] = 80
                    else:
                        sub['niche_score'] = 60 - min((subs - 500000) / 1500000 * 40, 40)
                    
                    sub['niche_score'] += sub.get('relevance', 0) * 0.5
                
                unique_results.sort(key=lambda x: x.get('niche_score', 0), reverse=True)
            
            return unique_results[:limit]
            
        except Exception as e:
            logging.error(f"Error in smart search: {e}")
            return []
    
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
    days = data.get('days', 7)  # Reduced default days
    
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
        if i > 0:  # Rate limiting between requests
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
    """Search for largest related subreddits"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 30), 50)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # Use smart search in 'search' mode (finds largest communities)
    subreddits = analyzer.smart_search_subreddits(query, limit, mode='search')
    
    # Return just the names for compatibility
    subreddit_names = [sub['name'] for sub in subreddits]
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(subreddit_names),
        'subreddits': subreddit_names,
        'detailed': subreddits  # Also include detailed info
    })

@app.route('/search-and-analyze', methods=['POST'])
def search_and_analyze_endpoint():
    """Search for niche subreddits and analyze them"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 20), 30)
    days = data.get('days', 7)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # Use smart search in 'niche' mode (finds engaged mid-size communities)
    subreddits_data = analyzer.smart_search_subreddits(query, limit, mode='niche')
    
    # Analyze top subreddits
    results = []
    for i, sub_data in enumerate(subreddits_data[:10]):  # Analyze top 10
        if i > 0:
            time.sleep(2)  # Rate limiting
            
        result = analyzer.analyze_subreddit(sub_data['name'], days)
        if result['success']:
            results.append(result)
    
    # Sort by effectiveness
    results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
    
    return jsonify({
        'success': True,
        'query': query,
        'count': len(results),
        'results': results
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
