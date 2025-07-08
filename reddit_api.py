from flask import Flask, request, jsonify, Response
import praw
from datetime import datetime, timedelta, timezone
import json
import os
from typing import List, Dict, Any, Set, Tuple
import logging
import time
import re
from collections import defaultdict, Counter
import threading
import queue
import pytz

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
        
        # Cache for analysis results
        self.analysis_cache = {}
        self.cache_timeout = 3600  # 1 hour
        
        # Blacklist of generic subreddits
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
    
    def analyze_posting_times(self, subreddit_name: str, days: int = 7) -> Dict[str, Any]:
        """Analyze best posting times for a subreddit"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Data structures for time analysis
            hourly_scores = defaultdict(list)
            hourly_comments = defaultdict(list)
            hourly_posts = defaultdict(int)
            daily_scores = defaultdict(list)
            daily_comments = defaultdict(list)
            
            # Analyze posts from the past week
            date_threshold = datetime.utcnow() - timedelta(days=days)
            posts_analyzed = 0
            
            for post in subreddit.top(time_filter='week', limit=500):
                try:
                    post_time = datetime.utcfromtimestamp(post.created_utc)
                    if post_time < date_threshold:
                        continue
                    
                    # Convert to EST (most Reddit users are US-based)
                    est = pytz.timezone('US/Eastern')
                    post_time_est = pytz.utc.localize(post_time).astimezone(est)
                    
                    hour = post_time_est.hour
                    day = post_time_est.strftime('%A')
                    
                    # Record metrics
                    hourly_scores[hour].append(post.score)
                    hourly_comments[hour].append(post.num_comments)
                    hourly_posts[hour] += 1
                    
                    daily_scores[day].append(post.score)
                    daily_comments[day].append(post.num_comments)
                    
                    posts_analyzed += 1
                    
                    if posts_analyzed % 50 == 0:
                        time.sleep(0.5)  # Rate limiting
                        
                except Exception as e:
                    logging.warning(f"Error analyzing post: {e}")
                    continue
            
            # Calculate averages and find best times
            best_times = []
            
            for hour in range(24):
                if hour in hourly_scores and hourly_scores[hour]:
                    avg_score = sum(hourly_scores[hour]) / len(hourly_scores[hour])
                    avg_comments = sum(hourly_comments[hour]) / len(hourly_comments[hour])
                    post_count = hourly_posts[hour]
                    
                    # Combined engagement score
                    engagement = (avg_score * 0.6 + avg_comments * 10 * 0.4)
                    
                    best_times.append({
                        'hour': hour,
                        'avg_score': round(avg_score, 1),
                        'avg_comments': round(avg_comments, 1),
                        'engagement': round(engagement, 1),
                        'post_count': post_count,
                        'competition': 'Low' if post_count < 10 else 'Medium' if post_count < 20 else 'High'
                    })
            
            # Sort by engagement
            best_times.sort(key=lambda x: x['engagement'], reverse=True)
            
            # Calculate best days
            best_days = []
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            
            for day in day_order:
                if day in daily_scores and daily_scores[day]:
                    avg_score = sum(daily_scores[day]) / len(daily_scores[day])
                    avg_comments = sum(daily_comments[day]) / len(daily_comments[day])
                    
                    best_days.append({
                        'day': day,
                        'avg_score': round(avg_score, 1),
                        'avg_comments': round(avg_comments, 1),
                        'post_count': len(daily_scores[day])
                    })
            
            # Sort days by average score
            best_days.sort(key=lambda x: x['avg_score'], reverse=True)
            
            return {
                'best_hours': best_times[:10],  # Top 10 hours
                'best_days': best_days,
                'timezone': 'EST',
                'posts_analyzed': posts_analyzed
            }
            
        except Exception as e:
            logging.error(f"Error analyzing posting times: {e}")
            return {
                'best_hours': [],
                'best_days': [],
                'error': str(e)
            }
    
    def find_related_subreddits_progressive(self, seed_subreddit: str, max_users: int = 100,
                                          progress_callback=None) -> Dict[str, Any]:
        """Progressive analysis of related subreddits with real-time updates"""
        try:
            related_subs = defaultdict(int)
            user_subreddits = defaultdict(set)
            users_analyzed = set()
            
            # Get the seed subreddit
            try:
                subreddit = self.reddit.subreddit(seed_subreddit)
                seed_info = {
                    'name': subreddit.display_name,
                    'subscribers': subreddit.subscribers
                }
            except:
                return {
                    'status': 'error',
                    'error': f"Subreddit '{seed_subreddit}' not found"
                }
            
            # Progress tracking
            total_users = 0
            users_to_analyze = []
            
            # Collect users from top posts
            for post in subreddit.hot(limit=25):
                try:
                    # Add author
                    if post.author and post.author.name not in users_analyzed:
                        users_to_analyze.append(('author', post.author.name, post.score))
                    
                    # Add top commenters
                    post.comments.replace_more(limit=0)
                    for comment in post.comments[:5]:
                        if comment.author and comment.author.name not in users_analyzed:
                            users_to_analyze.append(('commenter', comment.author.name, comment.score))
                    
                except:
                    continue
            
            # Sort by score (prioritize high-karma users)
            users_to_analyze.sort(key=lambda x: x[2], reverse=True)
            total_users = min(len(users_to_analyze), max_users)
            
            # Analyze users progressively
            for i, (user_type, username, score) in enumerate(users_to_analyze[:total_users]):
                if username in users_analyzed:
                    continue
                    
                users_analyzed.add(username)
                
                try:
                    user = self.reddit.redditor(username)
                    
                    # Analyze user's recent activity
                    user_subs = set()
                    
                    # Check recent submissions
                    for submission in user.submissions.new(limit=50):
                        sub_name = submission.subreddit.display_name
                        if sub_name.lower() != seed_subreddit.lower() and sub_name.lower() not in self.generic_subreddits:
                            user_subs.add(sub_name)
                            related_subs[sub_name] += 2  # Weight submissions higher
                    
                    # Check recent comments
                    for comment in user.comments.new(limit=100):
                        sub_name = comment.subreddit.display_name
                        if sub_name.lower() != seed_subreddit.lower() and sub_name.lower() not in self.generic_subreddits:
                            user_subs.add(sub_name)
                            related_subs[sub_name] += 1
                    
                    user_subreddits[username] = user_subs
                    
                    # Progress update
                    if progress_callback and (i + 1) % 5 == 0:
                        progress_callback({
                            'analyzed_users': i + 1,
                            'total_users': total_users,
                            'found_subreddits': len(related_subs)
                        })
                    
                    # Rate limiting
                    time.sleep(0.2)
                    
                except Exception as e:
                    logging.warning(f"Error analyzing user {username}: {e}")
                    continue
            
            # Get detailed info for top related subreddits
            sorted_subs = sorted(related_subs.items(), key=lambda x: x[1], reverse=True)[:50]
            
            detailed_results = []
            for sub_name, overlap_score in sorted_subs:
                try:
                    sub = self.reddit.subreddit(sub_name)
                    
                    # Calculate user overlap percentage
                    users_in_both = sum(1 for user_subs in user_subreddits.values() if sub_name in user_subs)
                    overlap_percentage = (users_in_both / len(users_analyzed)) * 100 if users_analyzed else 0
                    
                    detailed_results.append({
                        'name': sub.display_name,
                        'subscribers': sub.subscribers or 0,
                        'description': sub.public_description[:200] if sub.public_description else "",
                        'overlap_score': overlap_score,
                        'overlap_percentage': round(overlap_percentage, 1),
                        'users_in_both': users_in_both,
                        'nsfw': sub.over18
                    })
                except:
                    continue
            
            return {
                'status': 'complete',
                'seed_subreddit': seed_info,
                'analyzed_users': len(users_analyzed),
                'total_users': total_users,
                'related_subreddits': detailed_results,
                'analysis_depth': 'deep' if len(users_analyzed) > 50 else 'moderate'
            }
            
        except Exception as e:
            logging.error(f"Error in progressive analysis: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def analyze_subreddit_with_timing(self, subreddit_name: str, days: int = 7) -> Dict[str, Any]:
        """Enhanced subreddit analysis including best posting times"""
        try:
            # Get basic analysis
            basic_analysis = self.analyze_subreddit(subreddit_name, days)
            
            if not basic_analysis['success']:
                return basic_analysis
            
            # Add timing analysis
            timing_analysis = self.analyze_posting_times(subreddit_name, days)
            
            # Combine results
            basic_analysis['posting_times'] = timing_analysis
            
            # Format best times message
            if timing_analysis['best_hours']:
                best_times_msg = "\n\n📅 **Best Posting Times (EST):**\n"
                
                # Top 3 hours
                for i, hour_data in enumerate(timing_analysis['best_hours'][:3]):
                    hour = hour_data['hour']
                    hour_12 = datetime.strptime(str(hour), '%H').strftime('%I %p').lstrip('0')
                    
                    if hour_data['avg_score'] > hour_data['avg_comments'] * 10:
                        icon = "📈"  # High upvotes
                        focus = f"High upvotes (avg {hour_data['avg_score']})"
                    elif hour_data['avg_comments'] > 20:
                        icon = "💬"  # High comments
                        focus = f"High comments (avg {hour_data['avg_comments']})"
                    else:
                        icon = "🏆"  # Balanced
                        focus = "Best overall engagement"
                    
                    best_times_msg += f"{icon} **{hour_12}**: {focus}\n"
                
                # Best days
                if timing_analysis['best_days']:
                    best_times_msg += "\n📊 **Best Days:**\n"
                    top_days = timing_analysis['best_days'][:3]
                    best_times_msg += ", ".join([f"**{d['day']}**" for d in top_days])
                
                basic_analysis['timing_summary'] = best_times_msg
            
            return basic_analysis
            
        except Exception as e:
            logging.error(f"Error in enhanced analysis: {e}")
            return {'success': False, 'error': str(e)}
    
    def analyze_subreddit(self, subreddit_name: str, days: int = 7) -> Dict[str, Any]:
        """Basic subreddit analysis"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            subscribers = subreddit.subscribers
            date_threshold = datetime.utcnow() - timedelta(days=days)
            
            post_count = 0
            total_score = 0
            total_comments = 0
            
            for post in subreddit.new(limit=300):
                post_date = datetime.utcfromtimestamp(post.created_utc)
                if post_date < date_threshold:
                    break
                
                post_count += 1
                total_score += post.score
                total_comments += post.num_comments
                
                if post_count % 50 == 0:
                    time.sleep(0.5)
            
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
                'effectiveness_score': round(effectiveness, 2),
                'days_analyzed': days
            }
        except Exception as e:
            logging.error(f"Error analyzing subreddit {subreddit_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def parse_compare_input(self, input_text: str) -> List[str]:
        """Parse flexible compare input formats"""
        cleaned = re.sub(r'\s+', ' ', input_text.strip())
        subreddits = re.split(r'[,\s]+', cleaned)
        return [sub.strip() for sub in subreddits if sub.strip()]

# Initialize analyzer
analyzer = RedditAnalyzer()

# Store for progressive analysis sessions
analysis_sessions = {}

@app.route('/analyze', methods=['POST'])
def analyze_endpoint():
    """Enhanced analyze endpoint with posting times"""
    data = request.json
    subreddit = data.get('subreddit')
    days = data.get('days', 7)
    
    if not subreddit:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    result = analyzer.analyze_subreddit_with_timing(subreddit, days)
    return jsonify(result)

@app.route('/analyze-multiple', methods=['POST'])
def analyze_multiple_endpoint():
    """Enhanced compare endpoint with posting times"""
    data = request.json
    subreddits_input = data.get('subreddits', [])
    days = data.get('days', 7)
    
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
        
        result = analyzer.analyze_subreddit_with_timing(sub, days)
        if result['success']:
            results.append(result)
    
    results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
    
    return jsonify({
        'success': True,
        'count': len(results),
        'results': results
    })

@app.route('/search', methods=['POST'])
def search_endpoint():
    """Search using user overlap analysis"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 30), 50)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # First, try to find the main subreddit for this query
    try:
        # Try direct match
        main_sub = None
        for variation in [query, query.replace(' ', ''), query.replace(' ', '_')]:
            try:
                sub = analyzer.reddit.subreddit(variation)
                if sub.subscribers and sub.subscribers > 1000:
                    main_sub = sub.display_name
                    break
            except:
                continue
        
        # If found, do user overlap analysis
        if main_sub:
            results = analyzer.find_related_subreddits_progressive(main_sub, max_users=50)
            
            if results['status'] == 'complete' and results['related_subreddits']:
                # Return just names for compatibility, sorted by overlap
                subreddit_names = [sub['name'] for sub in results['related_subreddits'][:limit]]
                
                return jsonify({
                    'success': True,
                    'query': query,
                    'count': len(subreddit_names),
                    'subreddits': subreddit_names,
                    'analysis_method': 'user_overlap',
                    'seed_subreddit': main_sub,
                    'detailed': results['related_subreddits'][:limit]
                })
    except:
        pass
    
    # Fallback to regular search if no main subreddit found
    return jsonify({
        'success': True,
        'query': query,
        'count': 0,
        'subreddits': [],
        'message': f"No main subreddit found for '{query}'. Try a more specific term."
    })

@app.route('/search-and-analyze', methods=['POST'])
def search_and_analyze_endpoint():
    """Progressive search and analysis for niche communities"""
    data = request.json
    query = data.get('query')
    limit = min(data.get('limit', 20), 30)
    days = data.get('days', 7)
    session_id = data.get('session_id', str(time.time()))
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # Create or get session
    if session_id not in analysis_sessions:
        analysis_sessions[session_id] = {
            'status': 'starting',
            'results': [],
            'analyzed': 0,
            'total': 0
        }
    
    def analyze_in_background():
        # Find main subreddit
        main_sub = None
        for variation in [query, query.replace(' ', ''), query.replace(' ', '_')]:
            try:
                sub = analyzer.reddit.subreddit(variation)
                if sub.subscribers and sub.subscribers > 1000:
                    main_sub = sub.display_name
                    break
            except:
                continue
        
        if not main_sub:
            analysis_sessions[session_id]['status'] = 'error'
            analysis_sessions[session_id]['error'] = 'No main subreddit found'
            return
        
        # Progress callback
        def update_progress(progress):
            analysis_sessions[session_id].update({
                'status': 'analyzing',
                'analyzed': progress['analyzed_users'],
                'total': progress['total_users'],
                'found': progress['found_subreddits']
            })
        
        # Find related subreddits
        related = analyzer.find_related_subreddits_progressive(
            main_sub, 
            max_users=100,
            progress_callback=update_progress
        )
        
        if related['status'] == 'complete':
            # Analyze top subreddits
            results = []
            for i, sub_data in enumerate(related['related_subreddits'][:10]):
                if i > 0:
                    time.sleep(2)
                
                analysis = analyzer.analyze_subreddit(sub_data['name'], days)
                if analysis['success']:
                    analysis['overlap_percentage'] = sub_data['overlap_percentage']
                    analysis['users_in_common'] = sub_data['users_in_both']
                    results.append(analysis)
                
                analysis_sessions[session_id]['analyzed_subreddits'] = i + 1
            
            results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
            
            analysis_sessions[session_id].update({
                'status': 'complete',
                'results': results,
                'seed_subreddit': main_sub,
                'total_related': len(related['related_subreddits'])
            })
        else:
            analysis_sessions[session_id]['status'] = 'error'
            analysis_sessions[session_id]['error'] = related.get('error', 'Unknown error')
    
    # Start background analysis
    if analysis_sessions[session_id]['status'] == 'starting':
        thread = threading.Thread(target=analyze_in_background)
        thread.start()
    
    # Return current session state
    session = analysis_sessions[session_id]
    
    # Clean up old sessions
    if session['status'] == 'complete':
        # Remove session after returning results
        result = {
            'success': True,
            'query': query,
            'status': session['status'],
            'count': len(session.get('results', [])),
            'results': session.get('results', []),
            'seed_subreddit': session.get('seed_subreddit'),
            'session_id': session_id
        }
        del analysis_sessions[session_id]
        return jsonify(result)
    else:
        return jsonify({
            'success': True,
            'query': query,
            'status': session['status'],
            'analyzed_users': session.get('analyzed', 0),
            'total_users': session.get('total', 0),
            'found_subreddits': session.get('found', 0),
            'analyzed_subreddits': session.get('analyzed_subreddits', 0),
            'session_id': session_id,
            'message': 'Analysis in progress. Poll this endpoint with the same session_id for updates.'
        })

# Health check endpoints
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'reddit-analyzer'})

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({'message': 'pong', 'timestamp': datetime.utcnow().isoformat()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
