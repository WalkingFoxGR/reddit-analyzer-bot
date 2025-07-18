from flask import Flask, request, jsonify, Response
import praw
from prawcore.exceptions import RequestException, ResponseException
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
from pyairtable import Api
import statistics

# Add this function BEFORE the Flask app initialization
def safe_reddit_call(func, max_retries=3):
    """Wrapper to retry Reddit API calls"""
    for i in range(max_retries):
        try:
            return func()
        except (RequestException, ResponseException) as e:
            if i < max_retries - 1:
                wait_time = 2 ** i  # Exponential backoff: 1, 2, 4 seconds
                logging.warning(f"Reddit API error, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                raise

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration from environment variables
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', 'your_client_id')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', 'your_client_secret')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'RedditAnalyzer/1.0')

class RedditAnalyzer:
    def __init__(self):
        self.airtable = None
        if os.getenv('AIRTABLE_API_KEY') and os.getenv('AIRTABLE_BASE_ID'):
            self.airtable = Api(os.getenv('AIRTABLE_API_KEY'))
            self.karma_table = self.airtable.table(os.getenv('AIRTABLE_BASE_ID'), 'Karma Requirements')
        
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            ratelimit_seconds=300,
            timeout=30  # Added timeout
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
            subreddit = safe_reddit_call(lambda: self.reddit.subreddit(subreddit_name))
            
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
    
    def analyze_karma_requirements(self, subreddit_name: str) -> Dict[str, Any]:
        """Analyze karma requirements with caching"""
        
        # Check Airtable cache first
        if self.airtable:
            try:
                records = self.karma_table.all(formula=f"{{Subreddit}}='{subreddit_name}'")
                if records:
                    record = records[0]['fields']
                    last_updated = datetime.fromisoformat(record.get('Last_Updated', '2000-01-01'))
                    if (datetime.utcnow() - last_updated).days < 30:  # 30-day cache
                        return {
                            'success': True,
                            'from_cache': True,
                            'post_karma_min': record.get('Post_Karma_Min', 0),
                            'comment_karma_min': record.get('Comment_Karma_Min', 0),
                            'account_age_days': record.get('Account_Age_Days', 0),
                            'confidence': record.get('Confidence', 'Unknown'),
                            'requires_verification': record.get('Requires_Verification', False)
                        }
            except Exception as e:
                logging.warning(f"Airtable cache read failed: {e}")
        
        # Analyze if not cached
        try:
            subreddit = safe_reddit_call(lambda: self.reddit.subreddit(subreddit_name))
            
            min_post_karma = float('inf')
            min_comment_karma = float('inf')
            min_account_age = float('inf')
            
            users_analyzed = set()
            verified_users = 0
            total_users = 0
            
            # Check recent posts
            for post in subreddit.new(limit=50):
                try:
                    if not post.author or post.author.name in users_analyzed:
                        continue
                    
                    users_analyzed.add(post.author.name)
                    user = safe_reddit_call(lambda: self.reddit.redditor(post.author.name))
                    
                    # Track minimums
                    if user.link_karma < min_post_karma:
                        min_post_karma = user.link_karma
                    if user.comment_karma < min_comment_karma:
                        min_comment_karma = user.comment_karma
                        
                    account_age = (datetime.utcnow() - datetime.utcfromtimestamp(user.created_utc)).days
                    if account_age < min_account_age:
                        min_account_age = account_age
                    
                    # Check for verified status (if they have special flair)
                    if hasattr(post, 'author_flair_text') and post.author_flair_text:
                        if 'verified' in str(post.author_flair_text).lower():
                            verified_users += 1
                    total_users += 1
                    
                    if len(users_analyzed) % 10 == 0:
                        time.sleep(0.3)
                        
                except Exception as e:
                    continue
            
            # Check recent comments for comment karma requirements
            comment_users = set()
            for comment in subreddit.comments(limit=30):
                try:
                    if not comment.author or comment.author.name in comment_users:
                        continue
                        
                    comment_users.add(comment.author.name)
                    user = safe_reddit_call(lambda: self.reddit.redditor(comment.author.name))
                    
                    if user.comment_karma < min_comment_karma:
                        min_comment_karma = user.comment_karma
                        
                except Exception:
                    continue
            
            # Calculate confidence
            if min_post_karma > 100 or min_comment_karma > 100:
                confidence = 'High'
            elif min_post_karma > 10 or min_comment_karma > 10:
                confidence = 'Medium'
            else:
                confidence = 'Low'
            
            # Check verification requirement
            requires_verification = (verified_users / total_users > 0.5) if total_users > 0 else False
            
            result = {
                'success': True,
                'from_cache': False,
                'post_karma_min': max(0, min_post_karma - 1) if min_post_karma != float('inf') else 0,
                'comment_karma_min': max(0, min_comment_karma - 1) if min_comment_karma != float('inf') else 0,
                'account_age_days': max(0, min_account_age - 1) if min_account_age != float('inf') else 0,
                'confidence': confidence,
                'requires_verification': requires_verification,
                'users_analyzed': len(users_analyzed)
            }
            
            # Cache in Airtable
            if self.airtable and not result['from_cache']:
                try:
                    # Check if record exists
                    existing = self.karma_table.all(formula=f"{{Subreddit}}='{subreddit_name}'")
                    
                    record_data = {
                        'Subreddit': subreddit_name,
                        'Post_Karma_Min': result['post_karma_min'],
                        'Comment_Karma_Min': result['comment_karma_min'],
                        'Account_Age_Days': result['account_age_days'],
                        'Confidence': result['confidence'],
                        'Requires_Verification': result['requires_verification'],
                        'Last_Updated': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                     }
                    
                    if existing:
                        self.karma_table.update(existing[0]['id'], record_data)
                    else:
                        self.karma_table.create(record_data)
                        
                except Exception as e:
                    logging.warning(f"Airtable cache write failed: {e}")
            
            return result
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def detect_fake_upvotes(self, subreddit_name: str) -> Dict[str, Any]:
        """Detect potential fake upvote patterns"""
        try:
            subreddit = safe_reddit_call(lambda: self.reddit.subreddit(subreddit_name))
            
            # Collect post data
            post_scores = []
            user_post_scores = defaultdict(list)
            suspicious_posts = []
            
            for post in subreddit.new(limit=200):
                try:
                    score = post.score
                    post_scores.append(score)
                    
                    if post.author:
                        user_post_scores[post.author.name].append({
                            'score': score,
                            'title': post.title[:100],
                            'created': post.created_utc,
                            'url': f"https://reddit.com{post.permalink}"
                        })
                        
                except Exception:
                    continue
            
            # Calculate subreddit average
            if len(post_scores) > 10:
                median_score = statistics.median(post_scores)
                mean_score = statistics.mean(post_scores)
                
                # Check for suspicious patterns
                for username, posts in user_post_scores.items():
                    if len(posts) >= 2:
                        user_scores = [p['score'] for p in posts]
                        user_median = statistics.median(user_scores)
                        
                        # Check for suspicious spikes
                        for post in posts:
                            if post['score'] > max(median_score * 5, 100):
                                if post['score'] > user_median * 10:
                                    suspicious_posts.append({
                                        'author': username,
                                        'score': post['score'],
                                        'expected_range': f"{int(median_score * 0.5)}-{int(median_score * 2)}",
                                        'spike_ratio': round(post['score'] / median_score, 1),
                                        'title': post['title'],
                                        'url': post['url']
                                    })
            
            has_fake_upvotes = len(suspicious_posts) > 0
            
            # Update Airtable if exists
            if self.airtable and has_fake_upvotes:
                try:
                    existing = self.karma_table.all(formula=f"{{Subreddit}}='{subreddit_name}'")
                    if existing:
                        self.karma_table.update(existing[0]['id'], {
                            'Has_Fake_Upvotes': has_fake_upvotes,
                            'Analysis_Data': json.dumps(suspicious_posts[:5])  # Store top 5
                        })
                except Exception:
                    pass
            
            return {
                'success': True,
                'has_suspicious_activity': has_fake_upvotes,
                'median_score': median_score if 'median_score' in locals() else 0,
                'suspicious_posts': suspicious_posts[:3],  # Return top 3
                'confidence': 'High' if len(suspicious_posts) > 5 else 'Medium' if len(suspicious_posts) > 0 else 'Low'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def find_related_subreddits_progressive(self, seed_subreddit: str, max_users: int = 100,
                                          progress_callback=None) -> Dict[str, Any]:
        """Progressive analysis of related subreddits with real-time updates"""
        try:
            related_subs = defaultdict(int)
            user_subreddits = defaultdict(set)
            users_analyzed = set()
            
            # Get the seed subreddit
            try:
                subreddit = safe_reddit_call(lambda: self.reddit.subreddit(seed_subreddit))
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
                    user = safe_reddit_call(lambda: self.reddit.redditor(username))
                    
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
                    sub = safe_reddit_call(lambda: self.reddit.subreddit(sub_name))
                    
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
        """Enhanced subreddit analysis including best posting times AND restrictions"""
        try:
            # Get basic analysis
            basic_analysis = self.analyze_subreddit(subreddit_name, days)
            
            if not basic_analysis['success']:
                return basic_analysis
            
            # Add timing analysis
            timing_analysis = self.analyze_posting_times(subreddit_name, days)
            basic_analysis['posting_times'] = timing_analysis
            
            # ADD RESTRICTIONS ANALYSIS
            karma_req = self.analyze_karma_requirements(subreddit_name)
            fake_upvotes = self.detect_fake_upvotes(subreddit_name)
            
            basic_analysis['restrictions'] = {
                'karma_requirements': karma_req,
                'fake_upvote_detection': fake_upvotes
            }
            
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
        """Enhanced subreddit analysis with top post information"""
        try:
            subreddit = safe_reddit_call(lambda: self.reddit.subreddit(subreddit_name))
            subscribers = subreddit.subscribers
            date_threshold = datetime.utcnow() - timedelta(days=days)
            
            post_count = 0
            total_score = 0
            total_comments = 0
            
            # Track top post
            top_post = None
            top_post_score = 0
            
            # Analyze new posts for averages
            for post in subreddit.new(limit=300):
                post_date = datetime.utcfromtimestamp(post.created_utc)
                if post_date < date_threshold:
                    break
                
                post_count += 1
                total_score += post.score
                total_comments += post.num_comments
                
                # Track highest scoring post
                if post.score > top_post_score:
                    top_post_score = post.score
                    top_post = {
                        'title': post.title,
                        'score': post.score,
                        'author': post.author.name if post.author else '[deleted]',
                        'comments': post.num_comments,
                        'url': f"https://reddit.com{post.permalink}",
                        'created_utc': datetime.utcfromtimestamp(post.created_utc).isoformat(),
                        'upvote_ratio': post.upvote_ratio,
                        'flair': post.link_flair_text or 'No Flair'
                    }
                
                if post_count % 50 == 0:
                    time.sleep(0.5)
            
            # Also check top posts from the time period to ensure we get the actual top post
            try:
                # Get the top post from the specified time period
                time_filter = 'week' if days <= 7 else 'month' if days <= 30 else 'all'
                
                for post in subreddit.top(time_filter=time_filter, limit=10):
                    post_date = datetime.utcfromtimestamp(post.created_utc)
                    if post_date >= date_threshold and post.score > top_post_score:
                        top_post_score = post.score
                        top_post = {
                            'title': post.title,
                            'score': post.score,
                            'author': post.author.name if post.author else '[deleted]',
                            'comments': post.num_comments,
                            'url': f"https://reddit.com{post.permalink}",
                            'created_utc': datetime.utcfromtimestamp(post.created_utc).isoformat(),
                            'upvote_ratio': post.upvote_ratio,
                            'flair': post.link_flair_text or 'No Flair'
                        }
            except Exception as e:
                logging.warning(f"Error getting top posts: {e}")
            
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
                'days_analyzed': days,
                'top_post': top_post  # Add the top post information
            }
        except Exception as e:
            logging.error(f"Error analyzing subreddit {subreddit_name}: {e}")
            return {'success': False, 'error': str(e)}
    
    def analyze_user(self, username: str, days: int = 30, limit: int = 100) -> Dict[str, Any]:
        """Analyze a Reddit user's posting activity and performance"""
        try:
            user = safe_reddit_call(lambda: self.reddit.redditor(username))
            
            # Get basic user info
            try:
                user_info = {
                    'username': user.name,
                    'total_karma': user.total_karma,
                    'link_karma': user.link_karma,
                    'comment_karma': user.comment_karma,
                    'account_created': datetime.utcfromtimestamp(user.created_utc).isoformat(),
                    'account_age_days': (datetime.utcnow() - datetime.utcfromtimestamp(user.created_utc)).days,
                    'is_verified': user.verified if hasattr(user, 'verified') else False,
                    'has_premium': user.is_gold if hasattr(user, 'is_gold') else False
                }
            except Exception as e:
                logging.warning(f"Error getting user info: {e}")
                user_info = {'username': username, 'error': 'User info not accessible'}
            
            # Analyze recent submissions
            date_threshold = datetime.utcnow() - timedelta(days=days)
            submissions = []
            subreddit_stats = defaultdict(lambda: {'count': 0, 'total_score': 0, 'total_comments': 0})
            
            total_score = 0
            total_comments = 0
            submission_count = 0
            
            # Track top posts
            top_posts = []
            
            try:
                for submission in user.submissions.new(limit=limit):
                    submission_date = datetime.utcfromtimestamp(submission.created_utc)
                    
                    if submission_date < date_threshold:
                        continue
                    
                    submission_data = {
                        'title': submission.title,
                        'subreddit': submission.subreddit.display_name,
                        'score': submission.score,
                        'comments': submission.num_comments,
                        'upvote_ratio': submission.upvote_ratio,
                        'url': f"https://reddit.com{submission.permalink}",
                        'created_utc': submission_date.isoformat(),
                        'flair': submission.link_flair_text or 'No Flair',
                        'is_self': submission.is_self,
                        'is_nsfw': submission.over_18,
                        'is_spoiler': submission.spoiler,
                        'num_crossposts': submission.num_crossposts
                    }
                    
                    submissions.append(submission_data)
                    
                    # Update subreddit statistics
                    subreddit_name = submission.subreddit.display_name
                    subreddit_stats[subreddit_name]['count'] += 1
                    subreddit_stats[subreddit_name]['total_score'] += submission.score
                    subreddit_stats[subreddit_name]['total_comments'] += submission.num_comments
                    
                    # Track totals
                    total_score += submission.score
                    total_comments += submission.num_comments
                    submission_count += 1
                    
                    # Add to top posts if it's good enough
                    top_posts.append(submission_data)
                    
                    # Rate limiting
                    if submission_count % 20 == 0:
                        time.sleep(0.3)
                        
            except Exception as e:
                logging.warning(f"Error analyzing user submissions: {e}")
            
            # Sort submissions by score
            submissions.sort(key=lambda x: x['score'], reverse=True)
            top_posts.sort(key=lambda x: x['score'], reverse=True)
            
            # Calculate averages
            avg_score_per_post = total_score / submission_count if submission_count > 0 else 0
            avg_comments_per_post = total_comments / submission_count if submission_count > 0 else 0
            posts_per_day = submission_count / days
            
            # Process subreddit statistics
            subreddit_performance = []
            for subreddit, stats in subreddit_stats.items():
                if stats['count'] > 0:
                    subreddit_performance.append({
                        'subreddit': subreddit,
                        'post_count': stats['count'],
                        'total_score': stats['total_score'],
                        'total_comments': stats['total_comments'],
                        'avg_score': round(stats['total_score'] / stats['count'], 2),
                        'avg_comments': round(stats['total_comments'] / stats['count'], 2)
                    })
            
            # Sort subreddits by total score
            subreddit_performance.sort(key=lambda x: x['total_score'], reverse=True)
            
            # Analyze comment activity
            comment_count = 0
            comment_score = 0
            try:
                for comment in user.comments.new(limit=100):
                    comment_date = datetime.utcfromtimestamp(comment.created_utc)
                    if comment_date >= date_threshold:
                        comment_count += 1
                        comment_score += comment.score
                        
                        if comment_count % 25 == 0:
                            time.sleep(0.2)
            except Exception as e:
                logging.warning(f"Error analyzing comments: {e}")
            
            avg_comment_score = comment_score / comment_count if comment_count > 0 else 0
            
            return {
                'success': True,
                'username': username,
                'user_info': user_info,
                'analysis_period': {
                    'days': days,
                    'from_date': date_threshold.isoformat(),
                    'to_date': datetime.utcnow().isoformat()
                },
                'submission_stats': {
                    'total_submissions': submission_count,
                    'total_score': total_score,
                    'total_comments': total_comments,
                    'avg_score_per_post': round(avg_score_per_post, 2),
                    'avg_comments_per_post': round(avg_comments_per_post, 2),
                    'posts_per_day': round(posts_per_day, 2)
                },
                'comment_stats': {
                    'total_comments': comment_count,
                    'total_comment_score': comment_score,
                    'avg_comment_score': round(avg_comment_score, 2)
                },
                'top_posts': top_posts[:10],  # Top 10 posts
                'recent_posts': submissions[:20],  # 20 most recent posts
                'subreddit_performance': subreddit_performance[:15],  # Top 15 subreddits
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logging.error(f"Error analyzing user {username}: {e}")
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

def validate_subreddit(subreddit_name: str) -> Dict[str, Any]:
    """Validate if a subreddit exists and is accessible"""
    try:
        subreddit = safe_reddit_call(lambda: analyzer.reddit.subreddit(subreddit_name))
        # Try to access a basic attribute to trigger the API call
        _ = safe_reddit_call(lambda: subreddit.display_name)
        # Check if it's private/banned by trying to access subscribers
        try:
            subscribers = safe_reddit_call(lambda: subreddit.subscribers)
            if subscribers is None:
                return {
                    'valid': False,
                    'error': 'private',
                    'message': f'r/{subreddit_name} is private or restricted'
                }
        except Exception:
            return {
                'valid': False,
                'error': 'forbidden',
                'message': f'r/{subreddit_name} is private, banned, or restricted'
            }
        
        return {
            'valid': True,
            'subscribers': subscribers,
            'display_name': subreddit.display_name
        }
        
    except Exception as e:
        error_str = str(e).lower()
        if 'redirect' in error_str or 'not found' in error_str:
            return {
                'valid': False,
                'error': 'not_found',
                'message': f'r/{subreddit_name} does not exist'
            }
        elif 'forbidden' in error_str or 'private' in error_str:
            return {
                'valid': False,
                'error': 'forbidden',
                'message': f'r/{subreddit_name} is private or restricted'
            }
        else:
            return {
                'valid': False,
                'error': 'unknown',
                'message': f'Error accessing r/{subreddit_name}: {str(e)}'
            }

@app.route('/analyze', methods=['POST'])
def analyze_endpoint():
    """Enhanced analyze endpoint with posting times"""
    data = request.json
    subreddit = data.get('subreddit')
    days = data.get('days', 7)
    
    if not subreddit:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    # Validate subreddit exists
    validation = validate_subreddit(subreddit)
    if not validation['valid']:
        return jsonify({
            'success': False,
            'error': validation['message'],
            'error_type': validation['error']
        }), 400
    
    result = analyzer.analyze_subreddit_with_timing(subreddit, days)
    return jsonify(result)

@app.route('/analyze-user', methods=['POST'])
def analyze_user_endpoint():
    """Analyze a Reddit user's posting activity and performance"""
    data = request.json
    username = data.get('username')
    days = data.get('days', 30)
    limit = data.get('limit', 100)
    
    if not username:
        return jsonify({'success': False, 'error': 'No username provided'}), 400
    
    # Remove u/ prefix if present
    username = username.replace('u/', '').replace('/u/', '')
    
    # Validate user exists
    try:
        user = safe_reddit_call(lambda: analyzer.reddit.redditor(username))
        # Try to access a basic attribute to check if user exists
        _ = safe_reddit_call(lambda: user.name)
        
        # Check if user is suspended/banned
        try:
            _ = safe_reddit_call(lambda: user.created_utc)
        except Exception as e:
            if 'suspended' in str(e).lower() or 'banned' in str(e).lower():
                return jsonify({
                    'success': False,
                    'error': f'User u/{username} is suspended or banned',
                    'error_type': 'suspended'
                }), 400
            else:
                return jsonify({
                    'success': False,
                    'error': f'Cannot access user u/{username}: {str(e)}',
                    'error_type': 'access_denied'
                }), 400
                
    except Exception as e:
        error_str = str(e).lower()
        if 'not found' in error_str or 'redirect' in error_str:
            return jsonify({
                'success': False,
                'error': f'User u/{username} does not exist',
                'error_type': 'not_found'
            }), 400
        else:
            return jsonify({
                'success': False,
                'error': f'Error accessing user u/{username}: {str(e)}',
                'error_type': 'unknown'
            }), 400
    
    result = analyzer.analyze_user(username, days, limit)
    return jsonify(result)

@app.route('/analyze-multiple', methods=['POST'])
def analyze_multiple_endpoint():
    """Enhanced compare endpoint with posting times and validation"""
    data = request.json
    subreddits_input = data.get('subreddits', [])
    days = data.get('days', 7)
    
    if isinstance(subreddits_input, str):
        subreddits = analyzer.parse_compare_input(subreddits_input)
    else:
        subreddits = subreddits_input
    
    if not subreddits:
        return jsonify({'success': False, 'error': 'No subreddits provided'}), 400
    
    # Validate all subreddits first
    invalid_subreddits = []
    valid_subreddits = []
    
    for sub in subreddits:
        validation = validate_subreddit(sub)
        if not validation['valid']:
            invalid_subreddits.append(f"r/{sub}: {validation['message']}")
        else:
            valid_subreddits.append(sub)
    
    # Return error if any subreddits are invalid
    if invalid_subreddits:
        return jsonify({
            'success': False,
            'error': 'Invalid subreddits found',
            'invalid_subreddits': invalid_subreddits,
            'valid_subreddits': valid_subreddits
        }), 400
    
    # Continue with analysis for valid subreddits
    results = []
    for i, sub in enumerate(valid_subreddits):
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
                validation = validate_subreddit(variation)
                if validation['valid'] and validation['subscribers'] > 1000:
                    main_sub = validation['display_name']
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
    """Simplified niche analysis without progress updates"""
    data = request.json
    query = data.get('query')
    days = data.get('days', 7)
    
    if not query:
        return jsonify({'success': False, 'error': 'No search query provided'}), 400
    
    # For immediate response, check if we have a cached result
    cache_key = f"niche_{query}_{days}"
    if cache_key in analyzer.analysis_cache:
        cached_time, cached_result = analyzer.analysis_cache[cache_key]
        if time.time() - cached_time < 3600:  # 1 hour cache
            return jsonify(cached_result)
    
    # Try to find main subreddit synchronously first
    main_sub = None
    for variation in [query, query.replace(' ', ''), query.replace(' ', '_')]:
        try:
            validation = validate_subreddit(variation)
            if validation['valid'] and validation['subscribers'] > 100:
                main_sub = validation['display_name']
                break
        except Exception as e:
            logging.debug(f"Variation {variation} failed: {e}")
            continue
    
    if not main_sub:
        # Try searching for related subreddits
        try:
            search_results = []
            for submission in analyzer.reddit.subreddit('all').search(query, limit=10):
                sub_name = submission.subreddit.display_name
                if sub_name.lower() not in analyzer.generic_subreddits:
                    search_results.append(sub_name)
            
            if search_results:
                main_sub = search_results[0]
            else:
                return jsonify({
                    'success': False,
                    'status': 'error',
                    'error': f'No subreddit found for "{query}". Try a more specific term.',
                    'query': query
                })
        except:
            return jsonify({
                'success': False,
                'status': 'error', 
                'error': f'No subreddit found for "{query}". Try a more specific term.',
                'query': query
            })
    
    # Now do the actual analysis
    try:
        # Find related subreddits with user overlap
        logging.info(f"Starting niche analysis for {main_sub}")
        related = analyzer.find_related_subreddits_progressive(main_sub, max_users=50)
        
        if related['status'] != 'complete':
            return jsonify({
                'success': False,
                'status': 'error',
                'error': related.get('error', 'Analysis failed'),
                'query': query
            })
        
        # Analyze top related subreddits
        results = []
        subreddits_to_analyze = related['related_subreddits'][:10]
        
        for i, sub_data in enumerate(subreddits_to_analyze):
            if i > 0:
                time.sleep(1)  # Rate limiting
            
            try:
                analysis = analyzer.analyze_subreddit(sub_data['name'], days)
                if analysis['success']:
                    # Add overlap data
                    analysis['overlap_percentage'] = sub_data['overlap_percentage']
                    analysis['users_in_common'] = sub_data['users_in_both']
                    results.append(analysis)
            except Exception as e:
                logging.warning(f"Failed to analyze {sub_data['name']}: {e}")
                continue
        
        # Sort by effectiveness
        results.sort(key=lambda x: x.get('effectiveness_score', 0), reverse=True)
        
        # Cache the result
        result = {
            'success': True,
            'query': query,
            'status': 'complete',
            'results': results,
            'seed_subreddit': main_sub
        }
        
        analyzer.analysis_cache[cache_key] = (time.time(), result)
        
        return jsonify(result)
        
    except Exception as e:
        logging.error(f"Error in niche analysis: {e}")
        return jsonify({
            'success': False,
            'status': 'error',
            'error': str(e),
            'query': query
        })

@app.route('/scrape', methods=['POST'])
def scrape_posts():
    """Scrape post titles from a subreddit"""
    data = request.json
    subreddit_name = data.get('subreddit')
    limit = min(data.get('limit', 10), 50)  # Max 50 posts
    sort_type = data.get('sort', 'hot').lower()
    time_filter = data.get('time_filter', 'week').lower()
    
    if not subreddit_name:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    # Validate subreddit exists
    validation = validate_subreddit(subreddit_name)
    if not validation['valid']:
        return jsonify({
            'success': False,
            'error': validation['message'],
            'error_type': validation['error']
        }), 400
    
    # Validate parameters
    valid_sorts = ['hot', 'top', 'new', 'rising']
    valid_time_filters = ['hour', 'day', 'week', 'month', 'year', 'all']
    
    if sort_type not in valid_sorts:
        return jsonify({'success': False, 'error': f'Invalid sort type. Use: {", ".join(valid_sorts)}'}), 400
    
    if sort_type == 'top' and time_filter not in valid_time_filters:
        return jsonify({'success': False, 'error': f'Invalid time filter. Use: {", ".join(valid_time_filters)}'}), 400
    
    try:
        subreddit = safe_reddit_call(lambda: analyzer.reddit.subreddit(subreddit_name))
        posts = []
        
        # Get posts based on sort type
        if sort_type == 'hot':
            post_generator = subreddit.hot(limit=limit)
        elif sort_type == 'new':
            post_generator = subreddit.new(limit=limit)
        elif sort_type == 'rising':
            post_generator = subreddit.rising(limit=limit)
        elif sort_type == 'top':
            post_generator = subreddit.top(time_filter=time_filter, limit=limit)
        
        # Extract post data
        for i, post in enumerate(post_generator):
            try:
                # Format post date
                post_date = datetime.utcfromtimestamp(post.created_utc)
                
                posts.append({
                    'rank': i + 1,
                    'title': post.title,
                    'score': post.score,
                    'comments': post.num_comments,
                    'author': post.author.name if post.author else '[deleted]',
                    'url': f"https://reddit.com{post.permalink}",
                    'created_utc': post_date.isoformat(),
                    'subreddit': post.subreddit.display_name,
                    'flair': post.link_flair_text or 'None',
                    'is_self': post.is_self,
                    'upvote_ratio': post.upvote_ratio
                })
                
                # Rate limiting
                if i % 10 == 0 and i > 0:
                    time.sleep(0.5)
                    
            except Exception as e:
                logging.warning(f"Error processing post {i}: {e}")
                continue
        
        return jsonify({
            'success': True,
            'subreddit': subreddit_name,
            'sort_type': sort_type,
            'time_filter': time_filter if sort_type == 'top' else None,
            'limit': limit,
            'count': len(posts),
            'posts': posts,
            'subscribers': subreddit.subscribers,
            'scraped_at': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logging.error(f"Error scraping subreddit {subreddit_name}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/rules', methods=['POST'])
def get_subreddit_rules():
    """Get subreddit rules and submission guidelines"""
    data = request.json
    subreddit_name = data.get('subreddit')
    
    if not subreddit_name:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    validation = validate_subreddit(subreddit_name)
    if not validation['valid']:
        return jsonify({
            'success': False,
            'error': validation['message'],
            'error_type': validation['error']
        }), 400
    
    try:
        subreddit = safe_reddit_call(lambda: analyzer.reddit.subreddit(subreddit_name))
        rules = []
        
        for rule in subreddit.rules:
            rules.append({
                'title': rule.short_name,
                'description': rule.description,
                'kind': rule.kind,
                'priority': rule.priority
            })
        
        return jsonify({
            'success': True,
            'subreddit': subreddit_name,
            'rules': rules,
            'submission_text': subreddit.submit_text or 'No submission guidelines',
            'subscribers': subreddit.subscribers,
            'public_description': subreddit.public_description or 'No description'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/flairs', methods=['POST'])
def analyze_flairs():
    """Analyze flair performance and effectiveness"""
    data = request.json
    subreddit_name = data.get('subreddit')
    
    if not subreddit_name:
        return jsonify({'success': False, 'error': 'No subreddit provided'}), 400
    
    validation = validate_subreddit(subreddit_name)
    if not validation['valid']:
        return jsonify({
            'success': False,
            'error': validation['message'],
            'error_type': validation['error']
        }), 400
    
    try:
        subreddit = safe_reddit_call(lambda: analyzer.reddit.subreddit(subreddit_name))
        flair_stats = {}
        
        for post in subreddit.hot(limit=100):
            flair = post.link_flair_text or 'No Flair'
            if flair not in flair_stats:
                flair_stats[flair] = {
                    'count': 0,
                    'total_score': 0,
                    'total_comments': 0
                }
            
            flair_stats[flair]['count'] += 1
            flair_stats[flair]['total_score'] += post.score
            flair_stats[flair]['total_comments'] += post.num_comments
        
        # Calculate averages
        flair_analysis = []
        for flair, stats in flair_stats.items():
            if stats['count'] > 0:
                flair_analysis.append({
                    'flair': flair,
                    'post_count': stats['count'],
                    'avg_score': round(stats['total_score'] / stats['count'], 2),
                    'avg_comments': round(stats['total_comments'] / stats['count'], 2)
                })
        
        # Sort by average score
        flair_analysis.sort(key=lambda x: x['avg_score'], reverse=True)
        
        return jsonify({
            'success': True,
            'subreddit': subreddit_name,
            'flair_analysis': flair_analysis,
            'subscribers': subreddit.subscribers,
            'public_description': subreddit.public_description or 'No description',
            'analyzed_posts': sum(stats['count'] for stats in flair_stats.values())
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Helper method to clean up old cache entries periodically
def cleanup_cache():
    """Remove expired cache entries"""
    current_time = time.time()
    expired_keys = []
    
    for key, (cached_time, _) in analyzer.analysis_cache.items():
        if current_time - cached_time > 3600:  # 1 hour expiry
            expired_keys.append(key)
    
    for key in expired_keys:
        del analyzer.analysis_cache[key]

# Add this route for health monitoring
@app.route('/cache-status', methods=['GET'])
def cache_status():
    """Check cache status and memory usage"""
    cleanup_cache()
    return jsonify({
        'cache_entries': len(analyzer.analysis_cache),
        'cache_keys': list(analyzer.analysis_cache.keys()),
        'timestamp': datetime.utcnow().isoformat()
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
