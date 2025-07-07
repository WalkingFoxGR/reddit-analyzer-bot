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
            'showerthoughts', 'iama', 'all', 'popular', 'random'
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
    
    def find_related_subreddits(self, seed_subreddit: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Find related subreddits using user overlap analysis"""
        try:
            related_subs = defaultdict(int)
            users_analyzed = set()
            
            # Get the seed subreddit
            try:
                subreddit = self.reddit.subreddit(seed_subreddit)
                seed_subscribers = subreddit.subscribers
            except:
                logging.error(f"Could not find subreddit: {seed_subreddit}")
                return []
            
            # Analyze top posts from the last week
            for post in subreddit.top(time_filter='week', limit=25):
                try:
                    # Get the author
                    if post.author and post.author.name not in users_analyzed:
                        users_analyzed.add(post.author.name)
                        
                        # Check their recent posts in other subreddits
                        for user_post in post.author.submissions.new(limit=100):
                            sub_name = user_post.subreddit.display_name.lower()
                            
                            # Skip if it's the same subreddit or a generic one
                            if sub_name != seed_subreddit.lower() and sub_name not in self.generic_subreddits:
                                related_subs[user_post.subreddit.display_name] += 1
                        
                        # Small delay to respect rate limits
                        time.sleep(0.1)
                    
                    # Also check top commenters
                    post.comments.replace_more(limit=0)
                    for comment in post.comments[:10]:  # Top 10 comments
                        if comment.author and comment.author.name not in users_analyzed:
                            users_analyzed.add(comment.author.name)
                            
                            # Check their activity
                            try:
                                for user_comment in comment.author.comments.new(limit=50):
                                    sub_name = user_comment.subreddit.display_name.lower()
                                    if sub_name != seed_subreddit.lower() and sub_name not in self.generic_subreddits:
                                        related_subs[user_comment.subreddit.display_name] += 1
                            except:
                                continue
                            
                            time.sleep(0.1)
                    
                except Exception as e:
                    logging.warning(f"Error analyzing post/user: {e}")
                    continue
                
                # Limit analysis time
                if len(users_analyzed) >= 50:
                    break
            
            # Sort by overlap count and get subreddit info
            sorted_subs = sorted(related_subs.items(), key=lambda x: x[1], reverse=True)[:limit]
            
            results = []
            for sub_name, overlap_count in sorted_subs:
                try:
                    sub = self.reddit.subreddit(sub_name)
                    results.append({
                        'name': sub.display_name,
                        'subscribers': sub.subscribers or 0,
                        'overlap_score': overlap_count,
                        'description': sub.public_description[:100] if sub.public_description else ""
                    })
                except:
                    continue
            
            return results
            
        except Exception as e:
            logging.error(f"Error finding related subreddits: {e}")
            return []
    
    def smart_search_subreddits(self, query: str, limit: int = 50, mode: str = 'search') -> List[Dict[str, Any]]:
        """Smart search that finds truly related subreddits"""
        try:
            all_subreddits = {}
            query_lower = query.lower()
            query_terms = query_lower.split()
            
            # Method 1: Direct Reddit search (but filtered)
            for sub in self.reddit.subreddits.search(query, limit=limit*2):
                try:
                    sub_name_lower = sub.display_name.lower()
                    
                    # Skip generic subreddits
                    if sub_name_lower in self.generic_subreddits:
                        continue
                    
                    # Check if the subreddit is actually related to the query
                    description_lower = (sub.public_description or '').lower()
                    title_lower = (sub.title or '').lower()
                    
                    # Score based on relevance
                    relevance_score = 0
                    for term in query_terms:
                        if term in sub_name_lower:
                            relevance_score += 3
                        if term in title_lower:
                            relevance_score += 2
                        if term in description_lower:
                            relevance_score += 1
                    
                    # Only include if there's some relevance
                    if relevance_score > 0:
                        all_subreddits[sub.display_name] = {
                            'name': sub.display_name,
                            'subscribers': sub.subscribers or 0,
                            'relevance': relevance_score,
                            'description': sub.public_description[:100] if sub.public_description else ""
                        }
                    
                except Exception as e:
                    logging.warning(f"Error processing subreddit: {e}")
                    continue
            
            # Method 2: Search for variations of the query
            variations = [
                query,
                query.replace(' ', ''),  # spaceless
                query.replace(' ', '_'),  # underscored
                f"{query}s",  # plural
                f"the{query}",  # with "the"
                f"{query}community",
                f"{query}discussion"
            ]
            
            for variation in variations:
                try:
                    for sub in self.reddit.subreddits.search(variation, limit=20):
                        sub_name_lower = sub.display_name.lower()
                        
                        if sub_name_lower not in self.generic_subreddits and sub.display_name not in all_subreddits:
                            all_subreddits[sub.display_name] = {
                                'name': sub.display_name,
                                'subscribers': sub.subscribers or 0,
                                'relevance': 1,
                                'description': sub.public_description[:100] if sub.public_description else ""
                            }
                    time.sleep(0.5)
                except:
                    continue
            
            # Method 3: If we found a main subreddit, find related ones
            if all_subreddits:
                # Find the largest relevant subreddit
                largest_sub = max(all_subreddits.values(), key=lambda x: x['subscribers'])
                if largest_sub['subscribers'] > 10000:  # Only if it's established
                    related = self.find_related_subreddits(largest_sub['name'], limit=30)
                    for sub_data in related:
                        if sub_data['name'] not in all_subreddits:
                            all_subreddits[sub_data['name']] = sub_data
            
            # Convert to list and sort
            results = list(all_subreddits.values())
            
            if mode == 'search':
                # For /search: Sort by subscribers (largest first)
                results.sort(key=lambda x: x['subscribers'], reverse=True)
            else:  # mode == 'niche'
                # For /niche: Find sweet spot - active but not too large
                for sub in results:
                    # Calculate niche score (favors 10k-500k subscriber range)
                    subs = sub['subscribers']
                    if subs < 10000:
                        sub['niche_score'] = subs / 10000 * 50  # 0-50 points
                    elif subs < 100000:
                        sub['niche_score'] = 90  # Sweet spot
                    elif subs < 500000:
                        sub['niche_score'] = 80
                    else:
                        sub['niche_score'] = 60 - (min(subs, 2000000) - 500000) / 1500000 * 40
                    
                    # Boost by relevance
                    sub['niche_score'] += sub.get('relevance', 0) * 5
                
                results.sort(key=lambda x: x.get('niche_score', 0), reverse=True)
            
            return results[:limit]
            
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
