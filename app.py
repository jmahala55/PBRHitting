from flask import Flask, render_template, jsonify, request
from google.cloud import bigquery
import os
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import weasyprint
from jinja2 import Template

app = Flask(__name__)

# Load email configuration from file
def load_email_config():
    try:
        with open('email_config.json', 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print("email_config.json not found. Please create it with your email credentials.")
        return None
    except json.JSONDecodeError:
        print("Error parsing email_config.json. Please check the JSON format.")
        return None

# Load email config
email_config = load_email_config()

# Email configuration from file
if email_config:
    EMAIL_HOST = email_config.get('host', 'smtp.gmail.com')
    EMAIL_PORT = email_config.get('port', 587)
    EMAIL_USERNAME = email_config.get('username', '')
    EMAIL_PASSWORD = email_config.get('password', '')
    EMAIL_FROM = email_config.get('from', email_config.get('username', ''))
else:
    # Default values if config file is not available
    EMAIL_HOST = 'smtp.gmail.com'
    EMAIL_PORT = 587
    EMAIL_USERNAME = ''
    EMAIL_PASSWORD = ''
    EMAIL_FROM = ''

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'harvard-baseball-13fab221b2d4.json'

# Initialize BigQuery client
try:
    client = bigquery.Client()
    print("BigQuery client initialized successfully")
except Exception as e:
    print(f"Error initializing BigQuery client: {e}")
    client = None

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('hitting_index.html')

@app.route('/point-of-contact/<hitter_name>/<date>')
def point_of_contact_page(hitter_name, date):
    """Serve the Point of Contact analysis page"""
    return render_template('point_of_contact.html', hitter_name=hitter_name, date=date)

@app.route('/api/dates')
def get_dates():
    """API endpoint to get all available dates from TestTwo"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    try:
        query = """
        SELECT DISTINCT Date
        FROM `V1PBR.TestTwo`
        WHERE Date IS NOT NULL
        ORDER BY Date
        """
        
        result = client.query(query)
        dates = []
        for row in result:
            # Convert date to string format that matches what's stored
            date_val = row.Date
            if hasattr(date_val, 'strftime'):
                # If it's a datetime object, format it
                dates.append(date_val.strftime('%Y-%m-%d'))
            else:
                # If it's already a string, use as-is
                dates.append(str(date_val))
        
        return jsonify({'dates': dates})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hitters')
def get_hitters():
    """API endpoint to get unique hitters for a specific date"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    selected_date = request.args.get('date')
    if not selected_date:
        return jsonify({'error': 'Date parameter is required'}), 400
    
    try:
        query = """
        SELECT DISTINCT Batter
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter IS NOT NULL
        ORDER BY Batter
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        hitters = [row.Batter for row in result]
        
        return jsonify({'hitters': hitters})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hitter-details')
def get_hitter_details():
    """API endpoint to get detailed hitting data for a specific hitter and date"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    selected_date = request.args.get('date')
    hitter_name = request.args.get('hitter')
    
    if not selected_date or not hitter_name:
        return jsonify({'error': 'Date and hitter parameters are required'}), 400
    
    try:
        query = """
        SELECT *
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter = @hitter
        ORDER BY PitchNo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
                bigquery.ScalarQueryParameter("hitter", "STRING", hitter_name),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        
        # Convert to list of dictionaries
        hitting_data = []
        for row in result:
            hitting_data.append(dict(row))
        
        return jsonify({'hitting_data': hitting_data})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/point-of-contact')
def get_point_of_contact():
    """API endpoint to get point of contact data for a specific hitter and date"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    selected_date = request.args.get('date')
    hitter_name = request.args.get('hitter')
    
    if not selected_date or not hitter_name:
        return jsonify({'error': 'Date and hitter parameters are required'}), 400
    
    try:
        query = """
        SELECT 
            PitchNo,
            ContactPositionX,
            ContactPositionY,
            ContactPositionZ,
            ExitSpeed,
            Angle,
            Distance,
            Direction,
            PlayResult
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter = @hitter
        AND ContactPositionX IS NOT NULL
        AND ContactPositionY IS NOT NULL
        AND ContactPositionZ IS NOT NULL
        ORDER BY PitchNo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
                bigquery.ScalarQueryParameter("hitter", "STRING", hitter_name),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        
        # Convert to list of dictionaries
        contact_data = []
        for row in result:
            contact_data.append(dict(row))
        
        # Calculate contact statistics
        contact_stats = calculate_contact_stats(contact_data)
        
        return jsonify({
            'contact_data': contact_data,
            'contact_stats': contact_stats
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def calculate_contact_stats(contact_data):
    """Calculate point of contact statistics"""
    if not contact_data:
        return None
    
    # Extract coordinates
    x_positions = [d['ContactPositionX'] for d in contact_data if d['ContactPositionX'] is not None]
    y_positions = [d['ContactPositionY'] for d in contact_data if d['ContactPositionY'] is not None]
    z_positions = [d['ContactPositionZ'] for d in contact_data if d['ContactPositionZ'] is not None]
    
    if not x_positions:
        return None
    
    # Calculate averages
    avg_x = round(sum(x_positions) / len(x_positions), 2)
    avg_y = round(sum(y_positions) / len(y_positions), 2)
    avg_z = round(sum(z_positions) / len(z_positions), 2)
    
    # Determine primary contact zone based on Y position
    if avg_y > 3:
        primary_zone = "Deep"
    elif avg_y > -3:
        primary_zone = "Optimal"
    else:
        primary_zone = "Early"
    
    # Calculate consistency (standard deviation)
    import statistics
    try:
        consistency_score = round(statistics.stdev(y_positions), 2)
        if consistency_score < 2:
            consistency = "Excellent"
        elif consistency_score < 4:
            consistency = "Good"
        else:
            consistency = "Needs Work"
    except:
        consistency = "N/A"
    
    return {
        'avg_side': f"{avg_x:.1f}\"",
        'avg_depth': f"{avg_y:.1f}\"",
        'avg_height': f"{avg_z:.1f}\"",
        'total_contacts': len(contact_data),
        'primary_zone': primary_zone,
        'consistency': consistency,
        'raw_avg_x': avg_x,
        'raw_avg_y': avg_y,
        'raw_avg_z': avg_z
    }

@app.route('/api/matched-hitters')
def get_matched_hitters():
    """API endpoint to get hitters that have both hitting data and email info (Type = 'Hitting')"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    selected_date = request.args.get('date')
    if not selected_date:
        return jsonify({'error': 'Date parameter is required'}), 400
    
    try:
        # Get hitters for the selected date
        hitters_query = """
        SELECT DISTINCT Batter
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter IS NOT NULL
        ORDER BY Batter
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
            ]
        )
        
        hitters_result = client.query(hitters_query, job_config=job_config)
        hitters_from_test = [row.Batter for row in hitters_result]
        
        # Get hitter info from Info table (only Type = 'Hitting')
        hitters_info_query = """
        SELECT Event, Prospect, Email, Type, Comp
        FROM `V1PBRInfo.Info`
        WHERE Type = 'Hitting'
        AND Prospect IS NOT NULL
        ORDER BY Prospect
        """
        
        hitters_info_result = client.query(hitters_info_query)
        matched_hitters = []
        
        for row in hitters_info_result:
            if row.Prospect in hitters_from_test:
                matched_hitters.append({
                    'name': row.Prospect,
                    'email': row.Email,
                    'type': row.Type,
                    'event': row.Event,
                    'comp': row.Comp or 'D1'
                })
        
        return jsonify({'hitters': matched_hitters})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """API endpoint to get general dataset statistics for hitting"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    try:
        # Get total record count from TestTwo
        count_query = "SELECT COUNT(*) as total FROM `V1PBR.TestTwo`"
        count_result = client.query(count_query)
        total_records = list(count_result)[0].total
        
        # Get date range from TestTwo
        date_range_query = """
        SELECT 
            MIN(CAST(Date AS STRING)) as earliest_date,
            MAX(CAST(Date AS STRING)) as latest_date,
            COUNT(DISTINCT CAST(Date AS STRING)) as unique_dates,
            COUNT(DISTINCT Batter) as unique_hitters
        FROM `V1PBR.TestTwo`
        WHERE Date IS NOT NULL
        """
        
        date_result = client.query(date_range_query)
        date_info = list(date_result)[0]
        
        # Get all hitters from TestTwo table
        test_hitters_query = """
        SELECT DISTINCT Batter
        FROM `V1PBR.TestTwo`
        WHERE Batter IS NOT NULL
        ORDER BY Batter
        """
        
        test_result = client.query(test_hitters_query)
        test_hitters = set([row.Batter for row in test_result])
        
        # Get hitting prospects from Info table (Type = 'Hitting')
        info_hitters_query = """
        SELECT Event, Prospect, Email, Type
        FROM `V1PBRInfo.Info`
        WHERE Type = 'Hitting'
        ORDER BY Prospect
        """
        
        info_result = client.query(info_hitters_query)
        info_hitters = []
        info_hitter_names = set()
        
        for row in info_result:
            info_hitters.append({
                'name': row.Prospect,
                'email': row.Email,
                'type': row.Type,
                'event': row.Event
            })
            info_hitter_names.add(row.Prospect)
        
        # Find matches and mismatches
        matched_names = test_hitters.intersection(info_hitter_names)
        test_only = test_hitters - info_hitter_names  # In TestTwo but not in Info
        info_only = info_hitter_names - test_hitters  # In Info but not in TestTwo
        
        # Get email info for matched hitters
        matched_with_email = 0
        matched_without_email = 0
        
        for hitter in info_hitters:
            if hitter['name'] in matched_names:
                if hitter['email']:
                    matched_with_email += 1
                else:
                    matched_without_email += 1
        
        return jsonify({
            'total_records': total_records,
            'earliest_date': date_info.earliest_date,
            'latest_date': date_info.latest_date,
            'unique_dates': date_info.unique_dates,
            'unique_hitters': date_info.unique_hitters,
            'matching_stats': {
                'total_in_info': len(info_hitter_names),
                'total_in_test': len(test_hitters),
                'matched_names': len(matched_names),
                'matched_with_email': matched_with_email,
                'matched_without_email': matched_without_email,
                'in_test_only': len(test_only),
                'in_info_only': len(info_only),
                'test_only_names': list(test_only),
                'info_only_names': list(info_only),
                'matched_names_list': list(matched_names)
            }
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_hitter_competition_level(hitter_name):
    """Get the competition level for a specific hitter from the Info table"""
    try:
        query = """
        SELECT Comp
        FROM `V1PBRInfo.Info`
        WHERE Prospect = @hitter_name
        AND Type = 'Hitting'
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hitter_name", "STRING", hitter_name),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        row = list(result)
        
        if row and row[0].Comp:
            return row[0].Comp
        else:
            return 'D1'  # Default to D1 if no competition level found
            
    except Exception as e:
        print(f"Error getting competition level for {hitter_name}: {str(e)}")
        return 'D1'  # Default to D1 on error

def get_college_hitting_averages(comparison_level='D1'):
    """Get college baseball hitting averages for comparison - fixed version"""
    try:
        # Determine the WHERE clause based on comparison level
        if comparison_level == 'SEC':
            level_filter = "League = 'SEC'"
        elif comparison_level in ['D1', 'D2', 'D3']:
            level_filter = f"Level = '{comparison_level}'"
        else:
            level_filter = "Level = 'D1'"  # Default to D1
        
        print(f"Querying college hitting averages for: {comparison_level} with filter: {level_filter}")
        
        # Fixed query without ambiguous column references
        query = f"""
        SELECT 
            AVG(t.ExitSpeed) as avg_exit_velo,
            MAX(t.ExitSpeed) as max_exit_velo,
            APPROX_QUANTILES(t.ExitSpeed, 100)[OFFSET(90)] as percentile_90_exit_velo,
            AVG(CASE 
                WHEN t.ExitSpeed >= 95 AND t.Angle IS NOT NULL AND t.Angle >= 8 AND t.Angle <= 32 
                THEN 1 ELSE 0 
            END) * 100 as barrel_rate,
            AVG(CASE 
                WHEN t.ExitSpeed >= 95 
                THEN 1 ELSE 0 
            END) * 100 as hardhit_rate,
            COUNT(*) as total_batted_balls
        FROM `NCAABaseball.2025Final` t
        WHERE {level_filter}
        AND t.ExitSpeed IS NOT NULL
        AND t.ExitSpeed > 0
        """
        
        result = client.query(query)
        row = list(result)[0] if result else None
        
        print(f"Query result: {row}")
        
        if row and row.total_batted_balls > 0:
            college_data = {
                'avg_exit_velo': float(row.avg_exit_velo) if row.avg_exit_velo else None,
                'max_exit_velo': float(row.max_exit_velo) if row.max_exit_velo else None,
                'percentile_90_exit_velo': float(row.percentile_90_exit_velo) if row.percentile_90_exit_velo else None,
                'barrel_rate': float(row.barrel_rate) if row.barrel_rate else None,
                'hardhit_rate': float(row.hardhit_rate) if row.hardhit_rate else None,
                'total_batted_balls': int(row.total_batted_balls)
            }
            print(f"Returning college data: {college_data}")
            return college_data
        else:
            print(f"No data found for {comparison_level}")
            return None
        
    except Exception as e:
        print(f"Error getting college hitting averages for {comparison_level}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def calculate_hitting_comparison(player_value, college_average):
    """Calculate if player value is better than college average"""
    if player_value is None or college_average is None:
        return None
    
    difference = player_value - college_average
    
    # For hitting metrics, higher is generally better
    better = difference > 0
    
    return {
        'difference': difference,
        'better': better,
        'absolute_diff': abs(difference)
    }

def calculate_hitting_summary(hitting_data, hitter_name=None):
    """Calculate hitting summary statistics with college comparisons"""
    if not hitting_data:
        return None
    
    # Filter to only balls with exit velocity
    balls_with_ev = [h for h in hitting_data if h.get('ExitSpeed')]
    
    if not balls_with_ev:
        return {
            'avg_exit_velo': 0,
            'percentile_90_ev': 0,
            'max_exit_velo': 0,
            'barrel_rate': 0,
            'hardhit_rate': 0
        }
    
    exit_velocities = [h.get('ExitSpeed', 0) for h in balls_with_ev]
    
    # Calculate basic stats
    avg_exit_velo = sum(exit_velocities) / len(exit_velocities)
    max_exit_velo = max(exit_velocities)
    
    # Calculate 90th percentile
    sorted_velocities = sorted(exit_velocities)
    percentile_90_index = int(0.9 * len(sorted_velocities))
    percentile_90_ev = sorted_velocities[percentile_90_index] if sorted_velocities else 0
    
    # Calculate Barrel Rate (95+ mph EV with launch angle 8-32 degrees)
    barrels = 0
    hard_hits = 0
    
    for hit in balls_with_ev:
        exit_speed = hit.get('ExitSpeed', 0)
        launch_angle = hit.get('Angle')
        
        # Hard Hit: 95+ mph
        if exit_speed >= 95:
            hard_hits += 1
            
            # Barrel: 95+ mph AND launch angle between 8-32 degrees
            if launch_angle is not None and 8 <= launch_angle <= 32:
                barrels += 1
    
    # Calculate percentages
    total_balls_with_ev = len(balls_with_ev)
    barrel_rate = (barrels / total_balls_with_ev * 100) if total_balls_with_ev > 0 else 0
    hardhit_rate = (hard_hits / total_balls_with_ev * 100) if total_balls_with_ev > 0 else 0
    
    # Get college comparison data
    comparison_level = None
    college_averages = None
    if hitter_name:
        comparison_level = get_hitter_competition_level(hitter_name)
        college_averages = get_college_hitting_averages(comparison_level)
    
    # Calculate comparisons
    avg_exit_velo_comp = None
    percentile_90_ev_comp = None
    max_exit_velo_comp = None
    barrel_rate_comp = None
    hardhit_rate_comp = None
    
    if college_averages:
        avg_exit_velo_comp = calculate_hitting_comparison(avg_exit_velo, college_averages['avg_exit_velo'])
        percentile_90_ev_comp = calculate_hitting_comparison(percentile_90_ev, college_averages['percentile_90_exit_velo'])
        max_exit_velo_comp = calculate_hitting_comparison(max_exit_velo, college_averages['max_exit_velo'])
        barrel_rate_comp = calculate_hitting_comparison(barrel_rate, college_averages['barrel_rate'])
        hardhit_rate_comp = calculate_hitting_comparison(hardhit_rate, college_averages['hardhit_rate'])
    
    return {
        'avg_exit_velo': round(avg_exit_velo, 1),
        'percentile_90_ev': round(percentile_90_ev, 1),
        'max_exit_velo': round(max_exit_velo, 1),
        'barrel_rate': round(barrel_rate, 1),
        'hardhit_rate': round(hardhit_rate, 1),
        # College comparison data
        'comparison_level': comparison_level,
        'college_avg_exit_velo': round(college_averages['avg_exit_velo'], 1) if college_averages and college_averages['avg_exit_velo'] else None,
        'college_percentile_90_ev': round(college_averages['percentile_90_exit_velo'], 1) if college_averages and college_averages['percentile_90_exit_velo'] else None,
        'college_max_exit_velo': round(college_averages['max_exit_velo'], 1) if college_averages and college_averages['max_exit_velo'] else None,
        'college_barrel_rate': round(college_averages['barrel_rate'], 1) if college_averages and college_averages['barrel_rate'] else None,
        'college_hardhit_rate': round(college_averages['hardhit_rate'], 1) if college_averages and college_averages['hardhit_rate'] else None,
        # Comparison indicators
        'avg_exit_velo_comp': avg_exit_velo_comp,
        'percentile_90_ev_comp': percentile_90_ev_comp,
        'max_exit_velo_comp': max_exit_velo_comp,
        'barrel_rate_comp': barrel_rate_comp,
        'hardhit_rate_comp': hardhit_rate_comp
    }

@app.route('/api/hitter-summary')
def get_hitter_summary():
    """API endpoint to get hitter summary"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    selected_date = request.args.get('date')
    hitter_name = request.args.get('hitter')
    
    if not selected_date or not hitter_name:
        return jsonify({'error': 'Date and hitter parameters are required'}), 400
    
    try:
        # Get hitter's detailed data
        query = """
        SELECT *
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter = @hitter
        ORDER BY PitchNo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
                bigquery.ScalarQueryParameter("hitter", "STRING", hitter_name),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        hitting_data = [dict(row) for row in result]
        
        if not hitting_data:
            return jsonify({'error': 'No hitting data found'}), 404
        
        # Calculate summary statistics WITH COMPARISONS
        summary_stats = calculate_hitting_summary(hitting_data, hitter_name)
        
        return jsonify({
            'hitting_data': hitting_data,
            'summary_stats': summary_stats
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def generate_contact_points_html(contact_data):
    """Generate HTML for contact points that will be injected into the template"""
    if not contact_data:
        return "", ""
    
    # Find min/max values for scaling (keep existing for side view)
    x_values = [d['ContactPositionX'] for d in contact_data if d['ContactPositionX'] is not None]
    y_values = [d['ContactPositionY'] for d in contact_data if d['ContactPositionY'] is not None]
    z_values = [d['ContactPositionZ'] for d in contact_data if d['ContactPositionZ'] is not None]
    
    if not x_values:
        return "", ""
    
    x_min, x_max = min(x_values), max(x_values)
    y_min, y_max = min(y_values), max(y_values)
    z_min, z_max = min(z_values), max(z_values)
    
    # Calculate ranges with minimum range of 2 to avoid division by zero
    x_range = max(x_max - x_min, 2)
    y_range = max(y_max - y_min, 2)
    z_range = max(z_max - z_min, 2)
    
    # Add padding (20% of range)
    x_padding = x_range * 0.2
    y_padding = y_range * 0.2
    z_padding = z_range * 0.2
    
    # Helper function to determine contact type
    def get_contact_type(contact):
        angle = contact.get('Angle')
        if angle is None:
            return 'foul'
        if angle < 10:
            return 'ground-ball'
        elif 10 <= angle <= 25:
            return 'line-drive'
        else:
            return 'fly-ball'
    
    # Generate side view HTML (Y vs Z) - Keep existing functionality
    side_view_html = ""
    for i, contact in enumerate(contact_data):
        y_pos = contact.get('ContactPositionY')
        z_pos = contact.get('ContactPositionZ')
        
        if y_pos is not None and z_pos is not None:
            # Scale positions to plot container (15-85% to leave margins)
            x_percent = ((y_pos - (y_min - y_padding)) / (y_range + 2 * y_padding)) * 70 + 15
            y_percent = 85 - ((z_pos - (z_min - z_padding)) / (z_range + 2 * z_padding)) * 70  # Invert Y
            
            # Clamp to visible area
            x_percent = max(5, min(95, x_percent))
            y_percent = max(5, min(95, y_percent))
            
            contact_type = get_contact_type(contact)
            
            side_view_html += f'''
            <div class="contact-point {contact_type}" 
                 style="left: {x_percent:.1f}%; top: {y_percent:.1f}%;" 
                 title="Point {i+1}: Y={y_pos:.2f}, Z={z_pos:.2f}">
            </div>'''
    
    # Generate overhead view HTML - FLIPPED: Z is horizontal, X is vertical
    overhead_view_html = ""
    
    for i, contact in enumerate(contact_data):
        x_pos = contact.get('ContactPositionX')
        z_pos = contact.get('ContactPositionZ')
        
        if x_pos is not None and z_pos is not None:
            # Convert to inches - FLIPPED: Z is horizontal, X is vertical
            z_inches = z_pos * 12  # Z position in inches (horizontal axis)
            x_inches = x_pos * 12  # X position in inches (vertical axis)
            
            # Horizontal axis (Z): Scale from -18" to +18"
            x_percent = ((z_inches + 18) / 36) * 80 + 10
            
            # Vertical axis (X): Scale from -18" to +18" (inverted so higher X is at top)
            y_percent = 10 + ((18 - x_inches) / 36) * 80
            
            # Clamp to visible area
            x_percent = max(5, min(95, x_percent))
            y_percent = max(5, min(95, y_percent))
            
            contact_type = get_contact_type(contact)
            
            # Enhanced tooltip with Z and X coordinates (flipped order)
            exit_speed = contact.get('ExitSpeed', 'N/A')
            angle = contact.get('Angle', 'N/A')
            distance = contact.get('Distance', 'N/A')
            
            tooltip = f"Point {i+1}: Z={z_inches:.1f}\", X={x_inches:.1f}\" | EV: {exit_speed} mph | LA: {angle}° | Dist: {distance} ft"
            
            overhead_view_html += f'''
            <div class="contact-point {contact_type}" 
                 style="left: {x_percent:.1f}%; top: {y_percent:.1f}%;" 
                 title="{tooltip}">
            </div>
            <div class="contact-point-label" 
                 style="left: {min(90, x_percent + 2):.1f}%; top: {max(5, y_percent - 2):.1f}%;">
                ({z_inches:.1f}", {x_inches:.1f}")
            </div>'''
    
    return side_view_html, overhead_view_html

def generate_hitter_pdf(hitter_name, hitting_data, date):
    """Generate a PDF report for the hitter using WeasyPrint"""
    try:
        # Calculate summary stats
        if not hitting_data:
            print(f"No hitting data for {hitter_name}")
            return None
            
        # Format hitter name (convert "Smith, Jack" to "Jack Smith")
        if ', ' in hitter_name:
            last_name, first_name = hitter_name.split(', ', 1)
            formatted_name = f"{first_name} {last_name}"
        else:
            formatted_name = hitter_name
        
        # Filter to only include batted balls with exit velocity
        batted_balls = [hit for hit in hitting_data if hit.get('ExitSpeed')]
        
        # Calculate summary statistics WITH COMPARISONS (pass hitter_name)
        summary_stats = calculate_hitting_summary(hitting_data, hitter_name)
        
        # Get point of contact data - filter for records with valid contact positions
        contact_data = []
        for hit in hitting_data:
            # Check if contact position fields exist and are not None/null
            x_pos = hit.get('ContactPositionX')
            y_pos = hit.get('ContactPositionY') 
            z_pos = hit.get('ContactPositionZ')
            
            # More flexible checking - also check for 0 values which might be valid
            if (x_pos is not None and y_pos is not None and z_pos is not None and
                x_pos != '' and y_pos != '' and z_pos != ''):
                contact_data.append({
                    'PitchNo': hit.get('PitchNo'),
                    'ContactPositionX': float(x_pos) if x_pos is not None else None,
                    'ContactPositionY': float(y_pos) if y_pos is not None else None,
                    'ContactPositionZ': float(z_pos) if z_pos is not None else None,
                    'ExitSpeed': hit.get('ExitSpeed'),
                    'Angle': hit.get('Angle'),
                    'Distance': hit.get('Distance'),
                    'Direction': hit.get('Direction'),
                    'PlayResult': hit.get('PlayResult')
                })
        
        # Calculate contact statistics
        contact_stats = calculate_contact_stats(contact_data) if contact_data else None
        
        # Generate contact points HTML for server-side rendering
        side_view_points, overhead_view_points = generate_contact_points_html(contact_data)
        
        print(f"Generating PDF for {formatted_name} with {len(batted_balls)} batted balls and {len(contact_data)} contact points")
        print(f"Generated {len(side_view_points.split('contact-point')) - 1} side view points")
        print(f"Generated {len(overhead_view_points.split('contact-point')) - 1} overhead view points")
        
        # Read HTML template
        try:
            with open('hitter_report.html', 'r', encoding='utf-8') as file:
                html_template = file.read()
        except FileNotFoundError:
            print("Error: hitter_report.html not found. Make sure it's in the same directory as app.py")
            return None
        
        # Custom filter to convert data to JSON for JavaScript (still needed for fallback)
        def tojsonfilter(obj):
            import json
            return json.dumps(obj)
        
        # Render template with data using Jinja2
        from jinja2 import Environment
        env = Environment()
        env.filters['tojsonfilter'] = tojsonfilter
        template = env.from_string(html_template)
        
        rendered_html = template.render(
            hitter_name=formatted_name,
            date=date,
            summary_stats=summary_stats,
            hitting_data=batted_balls,  # Pass filtered data instead of all data
            contact_data=contact_data,  # Add contact position data (for JavaScript fallback)
            contact_stats=contact_stats,  # Add contact statistics
            side_view_points_html=side_view_points,  # Pre-generated HTML points
            overhead_view_points_html=overhead_view_points  # Pre-generated HTML points
        )
        
        # Generate PDF using WeasyPrint with proper base_url for static files
        try:
            # Get the absolute path to the current directory so WeasyPrint can find static files
            base_url = f"file://{os.path.abspath('.')}/"
            print(f"Using base_url: {base_url}")
            
            # Check if static files exist
            static_dir = os.path.join(os.getcwd(), 'static')
            if not os.path.exists(static_dir):
                print(f"Warning: Static directory not found at {static_dir}")
                os.makedirs(static_dir, exist_ok=True)
                print(f"Created static directory at {static_dir}")
            
            html_doc = weasyprint.HTML(string=rendered_html, base_url=base_url)
            pdf_bytes = html_doc.write_pdf()
            print(f"PDF generated successfully for {formatted_name} with contact analysis")
            return pdf_bytes
        except Exception as e:
            print(f"WeasyPrint error: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
        
    except Exception as e:
        print(f"Error generating PDF for {hitter_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def send_hitter_email(hitter_name, email, hitting_data, date):
    """Send email to hitter with PDF attachment"""
    try:
        # Check if email config is available
        if not EMAIL_USERNAME or not EMAIL_PASSWORD:
            print("Email configuration not available. Please check email_config.json")
            return False
        
        # Generate PDF
        pdf_data = generate_hitter_pdf(hitter_name, hitting_data, date)
        if not pdf_data:
            print(f"Failed to generate PDF for {hitter_name}")
            return False
        
        # Format hitter name for display
        if ', ' in hitter_name:
            last_name, first_name = hitter_name.split(', ', 1)
            display_name = f"{first_name} {last_name}"
        else:
            display_name = hitter_name
        
        # Calculate basic stats for email body
        total_abs = len(hitting_data) if hitting_data else 0
        summary = calculate_hitting_summary(hitting_data, hitter_name)
        
        # Create email content
        subject = f"Your Hitting Performance Report - {date}"
        
        body = f"""Hi {display_name},

Your hitting performance report for {date} is attached as a PDF.

Report Summary:
- Total At-Bats: {total_abs}
- Average Exit Velocity: {summary['avg_exit_velo']} mph
- Max Exit Velocity: {summary['max_exit_velo']} mph
- 90th Percentile EV: {summary['percentile_90_ev']} mph
- Barrel Rate: {summary['barrel_rate']}%
- Hard Hit Rate: {summary['hardhit_rate']}%

Keep up the great work!

Best regards,
Coaching Staff
"""
        
        # Create email message
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = EMAIL_FROM
        msg['To'] = email
        
        # Add body
        msg.attach(MIMEText(body, 'plain'))
        
        # Add PDF attachment
        pdf_attachment = MIMEBase('application', 'octet-stream')
        pdf_attachment.set_payload(pdf_data)
        encoders.encode_base64(pdf_attachment)
        
        # Create filename (use display name for filename)
        safe_name = display_name.replace(" ", "_").replace(",", "")
        filename = f"{safe_name}_Hitting_Report_{date}.pdf"
        
        pdf_attachment.add_header(
            'Content-Disposition',
            f'attachment; filename="{filename}"'
        )
        msg.attach(pdf_attachment)
        
        # Send email
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        print(f"Email with PDF sent successfully to {display_name} at {email}")
        return True
        
    except Exception as e:
        print(f"Failed to send email to {hitter_name} at {email}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

@app.route('/api/send-emails', methods=['POST'])
def send_emails():
    """API endpoint to send emails to hitters with their data"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    try:
        data = request.get_json()
        selected_date = data.get('date')
        
        if not selected_date:
            return jsonify({'error': 'Date is required'}), 400
        
        # Get hitters for the selected date
        hitters_query = """
        SELECT DISTINCT Batter
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter IS NOT NULL
        ORDER BY Batter
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
            ]
        )
        
        hitters_result = client.query(hitters_query, job_config=job_config)
        hitters_from_test = [row.Batter for row in hitters_result]
        
        # Get hitting prospects from Info table (Type = 'Hitting')
        prospects_query = """
        SELECT Event, Prospect, Email, Type, Comp
        FROM `V1PBRInfo.Info`
        WHERE Type = 'Hitting'
        ORDER BY Prospect
        """
        
        prospects_result = client.query(prospects_query)
        sent_emails = []
        failed_emails = []
        
        for row in prospects_result:
            if row.Prospect in hitters_from_test and row.Email:
                # Get hitter's detailed data
                hitter_data_query = """
                SELECT *
                FROM `V1PBR.TestTwo`
                WHERE CAST(Date AS STRING) = @date
                AND Batter = @hitter
                ORDER BY PitchNo
                """
                
                hitter_job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("date", "STRING", selected_date),
                        bigquery.ScalarQueryParameter("hitter", "STRING", row.Prospect),
                    ]
                )
                
                hitter_result = client.query(hitter_data_query, job_config=hitter_job_config)
                hitting_data = [dict(r) for r in hitter_result]
                
                # Try to send email
                email_success = send_hitter_email(row.Prospect, row.Email, hitting_data, selected_date)
                
                if email_success:
                    sent_emails.append({
                        'hitter': row.Prospect,
                        'email': row.Email,
                        'type': row.Type,
                        'event': row.Event,
                        'at_bats': len(hitting_data)
                    })
                else:
                    failed_emails.append({
                        'hitter': row.Prospect,
                        'email': row.Email,
                        'error': 'Email sending failed'
                    })
        
        return jsonify({
            'success': True,
            'summary': {
                'emails_sent_successfully': len(sent_emails),
                'emails_failed': len(failed_emails)
            },
            'sent_emails': sent_emails,
            'failed_emails': failed_emails
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/send-individual-email', methods=['POST'])
def send_individual_email():
    """API endpoint to send email to a specific hitter"""
    if not client:
        return jsonify({'error': 'BigQuery client not initialized'}), 500
    
    try:
        data = request.get_json()
        selected_date = data.get('date')
        hitter_name = data.get('hitter_name')
        hitter_email = data.get('hitter_email')
        
        if not selected_date or not hitter_name or not hitter_email:
            return jsonify({'error': 'Date, hitter name, and email are required'}), 400
        
        # Get hitter's detailed data
        hitter_data_query = """
        SELECT *
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter = @hitter
        ORDER BY PitchNo
        """
        
        hitter_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", selected_date),
                bigquery.ScalarQueryParameter("hitter", "STRING", hitter_name),
            ]
        )
        
        hitter_result = client.query(hitter_data_query, job_config=hitter_job_config)
        hitting_data = [dict(row) for row in hitter_result]
        
        if not hitting_data:
            return jsonify({'error': f'No hitting data found for {hitter_name} on {selected_date}'}), 400
        
        # Send email
        email_success = send_hitter_email(hitter_name, hitter_email, hitting_data, selected_date)
        
        if email_success:
            return jsonify({
                'success': True,
                'message': f'Email sent successfully to {hitter_name} at {hitter_email}',
                'hitter_name': hitter_name,
                'email': hitter_email,
                'at_bats': len(hitting_data),
                'date': selected_date
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Failed to send email to {hitter_name} at {hitter_email}'
            })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    import os
    if not os.path.exists('templates'):
        os.makedirs('templates')
        print("Created templates directory")
    
    print("Starting Flask server for hitting analytics...")
    print("Make sure harvard-baseball-13fab221b2d4.json is in the same directory")
    print("Make sure templates/hitting_index.html exists")
    print("Make sure hitter_report.html exists")
    print("Make sure static/pbr.png exists")
    app.run(debug=True, host='0.0.0.0', port=5001)