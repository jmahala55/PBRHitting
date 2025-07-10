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
    """Get college baseball hitting averages for comparison - fixed version with proper max velo calculation"""
    try:
        # Determine the WHERE clause based on comparison level
        if comparison_level == 'SEC':
            level_filter = "League = 'SEC'"
        elif comparison_level in ['D1', 'D2', 'D3']:
            level_filter = f"Level = '{comparison_level}'"
        else:
            level_filter = "Level = 'D1'"  # Default to D1
        
        print(f"Querying college hitting averages for: {comparison_level} with filter: {level_filter}")
        
        # Fixed query with proper max exit velo calculation grouped by batter
        query = f"""
        WITH batter_max_velos AS (
            SELECT 
                Batter,
                MAX(CASE WHEN ExitSpeed <= 120 THEN ExitSpeed ELSE NULL END) as max_exit_velo
            FROM `NCAABaseball.2025Final` t
            WHERE {level_filter}
            AND t.ExitSpeed IS NOT NULL
            AND t.ExitSpeed > 0
            GROUP BY Batter
        ),
        all_batted_balls AS (
            SELECT 
                t.ExitSpeed,
                t.Angle
            FROM `NCAABaseball.2025Final` t
            WHERE {level_filter}
            AND t.ExitSpeed IS NOT NULL
            AND t.ExitSpeed > 0
            AND t.ExitSpeed <= 120  -- Filter out unrealistic values
        )
        SELECT 
            AVG(abb.ExitSpeed) as avg_exit_velo,
            AVG(bmv.max_exit_velo) as max_exit_velo,
            APPROX_QUANTILES(abb.ExitSpeed, 100)[OFFSET(90)] as percentile_90_exit_velo,
            AVG(CASE 
                WHEN abb.ExitSpeed >= 95 AND abb.Angle IS NOT NULL AND abb.Angle >= 8 AND abb.Angle <= 32 
                THEN 1 ELSE 0 
            END) * 100 as barrel_rate,
            AVG(CASE 
                WHEN abb.ExitSpeed >= 95 
                THEN 1 ELSE 0 
            END) * 100 as hardhit_rate,
            COUNT(abb.ExitSpeed) as total_batted_balls,
            COUNT(DISTINCT bmv.Batter) as total_batters
        FROM all_batted_balls abb
        CROSS JOIN batter_max_velos bmv
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
                'total_batted_balls': int(row.total_batted_balls),
                'total_batters': int(row.total_batters) if hasattr(row, 'total_batters') else None
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
    
    # Filter for valid contact data
    valid_contacts = []
    for contact in contact_data:
        y_pos = contact.get('ContactPositionY')  # Height (up/down) in inches
        z_pos = contact.get('ContactPositionZ')  # Depth (front/back) in inches
        if y_pos is not None and z_pos is not None:
            valid_contacts.append(contact)
    
    if not valid_contacts:
        return "", ""
    
    # Extract Y and Z values and convert from feet to inches
    y_values = [d['ContactPositionY'] * 12 for d in valid_contacts]  # Height values in inches
    z_values = [d['ContactPositionZ'] * 12 for d in valid_contacts]  # Depth values in inches
    
    # DEBUG: Print the actual Z values to see what we're working with
    print(f"DEBUG: ContactPositionZ values (feet): {[d['ContactPositionZ'] for d in valid_contacts]}")
    print(f"DEBUG: ContactPositionZ values (inches): {z_values}")
    print(f"DEBUG: ContactPositionY values (inches): {y_values}")
    
    # Use actual data range with some padding for Y (height)
    y_min = min(y_values) - 3  # Add 3 inches padding below
    y_max = max(y_values) + 3  # Add 3 inches padding above
    
    # Helper function to determine contact type
    def get_contact_type(contact):
        angle = contact.get('Angle')
        if angle is None:
            return 'unknown'
        if angle < 8:
            return 'ground-ball'
        elif 8 <= angle <= 32:
            exit_speed = contact.get('ExitSpeed', 0)
            if exit_speed >= 95:
                return 'barrel'
            return 'line-drive'
        else:
            return 'fly-ball'
    
    # Generate side view SVG elements using your number line coordinates
    side_view_html = ""
    
    for i, contact in enumerate(valid_contacts):
        y_pos = contact['ContactPositionY'] * 12  # Convert feet to inches
        z_pos = contact['ContactPositionZ'] * 12  # Convert feet to inches
        
        # Map Z (depth) to SVG X coordinate using YOUR NUMBER LINE formula:
        # x = 15 + (25 - z_value) / 42 * 350
        svg_x = 15 + (25 - z_pos) / 42 * 350
        
        # DEBUG: Print each calculation
        print(f"DEBUG: Contact {i+1}: Z={z_pos:.1f}in -> SVG X={svg_x:.1f}")
        
        # Map Y (height) to SVG Y coordinate within the strike zone area
        y_range_data = y_max - y_min
        if y_range_data > 0:
            y_normalized = (y_pos - y_min) / y_range_data
            # Map to strike zone height area (y=125 to y=275)
            svg_y = 275 - (y_normalized * 150)  # 150 is strike zone height
        else:
            svg_y = 200  # Middle if all Y values are the same
        
        # Clamp Y to reasonable bounds but allow X to extend beyond zone
        svg_y = max(110, min(285, svg_y))
        # Don't clamp svg_x - let it show contact outside the zone
        
        # Get contact type and styling
        contact_type = get_contact_type(contact)
        
        # Color mapping - same colors regardless of zone
        color_map = {
            'ground-ball': '#34a853',
            'line-drive': '#191970', 
            'barrel': '#dc2626',
            'fly-ball': '#4285f4',
            'unknown': '#666666'
        }
        point_color = color_map.get(contact_type, '#666666')
        
        # Size based on exit velocity
        exit_speed = contact.get('ExitSpeed', 0)
        radius = 6 if exit_speed >= 100 else 5 if exit_speed >= 95 else 4 if exit_speed >= 90 else 3
        
        # Determine if contact is inside or outside the strike zone
        # Zone boundaries: Z=0 (front) at x=223, Z=-17 (back) at x=365
        is_in_zone = (0 >= z_pos >= -17)
        
        # Consistent styling for all contact points
        stroke_width = 1.5
        opacity = 0.85
        
        # Create tooltip
        angle = contact.get('Angle', 'N/A')
        distance = contact.get('Distance', 'N/A')
        zone_status = "IN ZONE" if is_in_zone else "OUT OF ZONE"
        
        tooltip = f"Contact {i+1}: Z={z_pos:.1f}in (depth), Y={y_pos:.1f}in (height) | {zone_status} | EV: {exit_speed} mph | LA: {angle}° | Dist: {distance} ft"
        
        # Generate SVG circle
        side_view_html += f'''
            <circle cx="{svg_x:.1f}" cy="{svg_y:.1f}" r="{radius}" 
                    fill="{point_color}" stroke="white" stroke-width="{stroke_width}" 
                    opacity="{opacity}" class="contact-point-svg">
                <title>{tooltip}</title>
            </circle>
        '''
    
    # Keep original overhead view (unchanged)
    overhead_view_html = ""
    x_values = [d['ContactPositionX'] for d in contact_data if d['ContactPositionX'] is not None]
    
    if x_values:
        for i, contact in enumerate(contact_data):
            x_pos = contact.get('ContactPositionX')
            z_pos = contact.get('ContactPositionZ')
            
            if x_pos is not None and z_pos is not None:
                # Convert feet to inches
                x_inches = x_pos * 12
                z_inches = z_pos * 12
                
                # Map to percentage coordinates for overhead view
                x_percent = ((x_inches + 18) / 36) * 80 + 10
                z_percent = ((z_inches + 17) / 34) * 80 + 10
                
                # Clamp to visible area
                x_percent = max(5, min(95, x_percent))
                z_percent = max(5, min(95, z_percent))
                
                contact_type = get_contact_type(contact)
                
                y_inches = contact.get('ContactPositionY', 0) * 12  # Convert feet to inches
                tooltip = f"Point {i+1}: X={x_inches:.1f}\" (side), Z={z_inches:.1f}\" (depth), Y={y_inches:.1f}\" (height)"
                
                overhead_view_html += f'''
                <div class="contact-point {contact_type}" 
                     style="left: {x_percent:.1f}%; top: {z_percent:.1f}%;" 
                     title="{tooltip}">
                    <span class="contact-number">{i+1}</span>
                </div>'''
    
    return side_view_html, overhead_view_html

def calculate_spray_chart_stats(hitting_data):
    """Calculate spray chart specific statistics"""
    if not hitting_data:
        return None
    
    # Filter for balls with direction and distance data
    spray_balls = [hit for hit in hitting_data if 
                   hit.get('Direction') is not None and 
                   hit.get('Distance') is not None and 
                   hit.get('Distance') > 0]
    
    if not spray_balls:
        return None
    
    # Calculate directional tendencies
    pull_hits = len([hit for hit in spray_balls if hit.get('Direction', 0) < -5])
    opposite_hits = len([hit for hit in spray_balls if hit.get('Direction', 0) > 5])
    center_hits = len(spray_balls) - pull_hits - opposite_hits
    
    # Calculate ball type distribution
    ground_balls = len([hit for hit in spray_balls if hit.get('Angle', 0) < 10])
    line_drives = len([hit for hit in spray_balls if 10 <= hit.get('Angle', 0) <= 25])
    fly_balls = len([hit for hit in spray_balls if hit.get('Angle', 0) > 25])
    
    # Distance analysis
    distances = [hit.get('Distance') for hit in spray_balls]
    avg_distance = sum(distances) / len(distances) if distances else 0
    max_distance = max(distances) if distances else 0
    long_hits = len([d for d in distances if d >= 300])
    
    return {
        'total_spray_balls': len(spray_balls),
        'pull_percentage': round((pull_hits / len(spray_balls)) * 100, 1) if spray_balls else 0,
        'opposite_percentage': round((opposite_hits / len(spray_balls)) * 100, 1) if spray_balls else 0,
        'center_percentage': round((center_hits / len(spray_balls)) * 100, 1) if spray_balls else 0,
        'ground_ball_count': ground_balls,
        'line_drive_count': line_drives,
        'fly_ball_count': fly_balls,
        'avg_distance': round(avg_distance, 1),
        'max_distance': max_distance,
        'long_hits_300plus': long_hits
    }

def get_spray_chart_data(hitter_name, date):
    """Get spray chart specific data for a hitter and date"""
    if not client:
        return []
    
    try:
        query = """
        SELECT 
            PitchNo,
            ExitSpeed,
            Angle,
            Distance,
            Direction,
            PlayResult
        FROM `V1PBR.TestTwo`
        WHERE CAST(Date AS STRING) = @date
        AND Batter = @hitter
        AND ExitSpeed IS NOT NULL
        AND Direction IS NOT NULL 
        AND Distance IS NOT NULL
        AND Distance > 0
        ORDER BY PitchNo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "STRING", date),
                bigquery.ScalarQueryParameter("hitter", "STRING", hitter_name),
            ]
        )
        
        result = client.query(query, job_config=job_config)
        spray_data = [dict(row) for row in result]
        
        print(f"Spray chart query returned {len(spray_data)} records for {hitter_name}")
        return spray_data
        
    except Exception as e:
        print(f"Error getting spray chart data: {str(e)}")
        return []
    

def calculate_spray_position(direction, distance):
    """Calculate x,y position for spray chart based on direction and distance - CORRECTED"""
    import math
    
    # Normalize direction to field boundaries (-45° to +45°)
    field_direction = max(-45, min(45, direction))
    
    # Convert to radians
    angle_rad = math.radians(field_direction)
    
    # Based on your visual chart, map distances to radius percentages
    # Looking at your image: 100ft ≈ 15%, 200ft ≈ 30%, 300ft ≈ 45%, 400ft ≈ 60%
    if distance <= 100:
        radius_percent = (distance / 100) * 15  # 0-15% for 0-100ft
    elif distance <= 200:
        radius_percent = 15 + ((distance - 100) / 100) * 15  # 15-30% for 100-200ft
    elif distance <= 300:
        radius_percent = 30 + ((distance - 200) / 100) * 15  # 30-45% for 200-300ft
    elif distance <= 400:
        radius_percent = 45 + ((distance - 300) / 100) * 15  # 45-60% for 300-400ft
    else:
        radius_percent = 60 + min((distance - 400) / 100, 1) * 10  # 60-70% for 400ft+
    
    # Convert to actual position
    # Home plate is at center-bottom: x=50%, y=85%
    home_plate_x = 50
    home_plate_y = 85
    
    # Calculate offset from home plate
    x_offset = math.sin(angle_rad) * radius_percent
    y_offset = math.cos(angle_rad) * radius_percent
    
    # Apply offset to home plate position
    x_percent = home_plate_x + x_offset
    y_percent = home_plate_y - y_offset  # Subtract because y increases downward
    
    # Clamp to visible area
    x_percent = max(5, min(95, x_percent))
    y_percent = max(5, min(95, y_percent))
    
    # Debug output
    print(f"Distance: {distance}ft, Direction: {direction}° -> Radius: {radius_percent:.1f}% -> Position: ({x_percent:.1f}%, {y_percent:.1f}%)")
    
    return x_percent, y_percent

def generate_spray_chart_html(spray_chart_data):
    """Generate HTML for spray chart balls with corrected positioning"""
    if not spray_chart_data:
        return "", {}
    
    spray_balls_html = ""
    
    # Initialize counters
    pull_count = 0
    opposite_count = 0
    ground_balls = 0
    line_drives = 0
    fly_balls = 0
    total_distance = 0
    valid_distance_count = 0
    max_distance = 0
    long_hits = 0
    
    for i, hit in enumerate(spray_chart_data):
        direction = hit.get('Direction')
        distance = hit.get('Distance')
        angle = hit.get('Angle')
        
        if (direction is not None and distance is not None and distance > 0):
            # Calculate position on spray chart using fixed function
            x, y = calculate_spray_position(direction, distance)
            
            # Determine ball type and color based on launch angle
            ball_color = '#666'  # Default
            ball_type = 'foul'
            
            if angle is not None:
                if angle < 10:
                    ball_type = 'ground-ball'
                    ball_color = '#34a853'
                    ground_balls += 1
                elif 10 <= angle <= 25:
                    ball_type = 'line-drive'
                    ball_color = '#191970'
                    line_drives += 1
                else:
                    ball_type = 'fly-ball'
                    ball_color = '#4285f4'
                    fly_balls += 1
            
            # Debug info for verification
            print(f"Ball {i+1}: {distance}ft at {direction}° -> {x:.1f}%, {y:.1f}%")
            
            # Generate HTML for this ball
            spray_balls_html += f'''
            <div style="position: absolute; 
                        width: 12px; 
                        height: 12px; 
                        border-radius: 50%; 
                        background: {ball_color}; 
                        left: {x:.1f}%; 
                        top: {y:.1f}%; 
                        z-index: 5; 
                        border: 1px solid rgba(255,255,255,0.7); 
                        box-shadow: 0 2px 4px rgba(0,0,0,0.3);" 
                 title="Ball {i+1}: {distance}ft, {direction}°, {angle}° LA">
            </div>'''
            
            # Update statistics
            if direction < -5:
                pull_count += 1
            elif direction > 5:
                opposite_count += 1
            
            total_distance += distance
            valid_distance_count += 1
            max_distance = max(max_distance, distance)
            if distance >= 300:
                long_hits += 1
    
    # Calculate percentages
    total_directional = pull_count + opposite_count + (len(spray_chart_data) - pull_count - opposite_count)
    pull_percentage = round((pull_count / total_directional) * 100) if total_directional > 0 else 0
    opposite_percentage = round((opposite_count / total_directional) * 100) if total_directional > 0 else 0
    avg_distance = round(total_distance / valid_distance_count) if valid_distance_count > 0 else 0
    
    # Return both HTML and statistics
    spray_stats = {
        'pull_percentage': pull_percentage,
        'opposite_percentage': opposite_percentage,
        'ground_balls': ground_balls,
        'line_drives': line_drives,
        'fly_balls': fly_balls,
        'avg_distance': avg_distance,
        'max_distance': max_distance,
        'long_hits': long_hits
    }
    
    return spray_balls_html, spray_stats

# Test function to validate positioning against known distances
def test_spray_positions():
    """Test function to verify spray chart positioning"""
    test_distances = [100, 200, 300, 400]
    test_directions = [-30, 0, 30]  # Pull, center, opposite
    
    print("=== SPRAY CHART POSITION TESTING ===")
    for distance in test_distances:
        for direction in test_directions:
            x, y = calculate_spray_position(direction, distance)
            dir_name = "Pull" if direction < 0 else "Opposite" if direction > 0 else "Center"
            print(f"{distance}ft {dir_name}: ({x:.1f}%, {y:.1f}%)")
        print()  # Empty line between distance groups


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

        # Get spray chart specific data
        spray_chart_data = get_spray_chart_data(hitter_name, date)

        # NEW: Generate spray chart HTML and stats server-side
        spray_balls_html, spray_chart_stats = generate_spray_chart_html(spray_chart_data)
        
        print(f"Generating PDF for {formatted_name} with {len(batted_balls)} batted balls and {len(contact_data)} contact points")
        print(f"Generated {len(side_view_points.split('contact-point')) - 1} side view points")
        print(f"Generated {len(overhead_view_points.split('contact-point')) - 1} overhead view points")
        print(f"Generated {len(spray_balls_html.split('<div')) - 1} spray chart balls")

        print(f"\n=== DEBUGGING HITTING DATA FOR {hitter_name} ===")
        print(f"Total records: {len(hitting_data)}")
        
        if hitting_data:
            # Check what fields are available
            first_record = hitting_data[0]
            print(f"Available fields: {list(first_record.keys())}")
            
            # Check for spray chart specific fields
            spray_fields = ['Direction', 'Distance', 'Angle', 'ExitSpeed']
            for field in spray_fields:
                if field in first_record:
                    # Count non-null values
                    non_null_count = len([h for h in hitting_data if h.get(field) is not None])
                    print(f"{field}: {non_null_count}/{len(hitting_data)} non-null values")
                    
                    # Show sample values
                    sample_values = [h.get(field) for h in hitting_data[:3] if h.get(field) is not None]
                    print(f"  Sample values: {sample_values}")
                else:
                    print(f"{field}: FIELD NOT FOUND")
            
            # Check spray chart viability
            spray_viable = [hit for hit in hitting_data if 
                           hit.get('Direction') is not None and 
                           hit.get('Distance') is not None and 
                           hit.get('Distance', 0) > 0]
            print(f"Records viable for spray chart: {len(spray_viable)}/{len(hitting_data)}")
        
        print("=== END DEBUGGING ===\n")

        # ADD NEW SPRAY CHART DEBUGGING
        print(f"\n=== SPRAY CHART DEBUG ===")
        print(f"spray_chart_data length: {len(spray_chart_data) if spray_chart_data else 0}")
        if spray_chart_data:
            print(f"First spray chart record: {spray_chart_data[0]}")
            print(f"spray_chart_data sample fields: {list(spray_chart_data[0].keys()) if spray_chart_data else 'None'}")
            
            # Check specific fields
            for i, record in enumerate(spray_chart_data[:3]):
                direction = record.get('Direction')
                distance = record.get('Distance') 
                angle = record.get('Angle')
                print(f"Record {i+1}: Direction={direction}, Distance={distance}, Angle={angle}")

        print(f"hitting_data (all records) length: {len(hitting_data) if hitting_data else 0}")
        print(f"batted_balls length: {len(batted_balls) if batted_balls else 0}")
        if batted_balls:
            print(f"First batted ball record keys: {list(batted_balls[0].keys()) if batted_balls else 'None'}")
            # Check if batted_balls has spray chart fields
            first_batted = batted_balls[0]
            print(f"First batted ball Direction: {first_batted.get('Direction')}")
            print(f"First batted ball Distance: {first_batted.get('Distance')}")
            print(f"First batted ball Angle: {first_batted.get('Angle')}")
        
        # Print spray chart stats
        print(f"Generated spray chart stats: {spray_chart_stats}")
        print("=== END SPRAY CHART DEBUG ===\n")
        
        # Read HTML template
        try:
            with open('hitter_report.html', 'r', encoding='utf-8') as file:
                html_template = file.read()
        except FileNotFoundError:
            print("Error: hitter_report.html not found. Make sure it's in the same directory as app.py")
            return None
        
        # Custom filter to convert data to JSON for JavaScript - FIXED VERSION
        def tojsonfilter(obj):
            import json
            from datetime import date, datetime
            from decimal import Decimal
            
            def json_serializer(o):
                """JSON serializer for objects not serializable by default json code"""
                if isinstance(o, (date, datetime)):
                    return o.isoformat()
                elif isinstance(o, Decimal):
                    return float(o)
                elif hasattr(o, '__dict__'):
                    return o.__dict__
                else:
                    return str(o)
            
            return json.dumps(obj, default=json_serializer)
        
        # Render template with data using Jinja2
        from jinja2 import Environment
        env = Environment()
        env.filters['tojsonfilter'] = tojsonfilter
        template = env.from_string(html_template)
        
        # UPDATED TEMPLATE RENDERING - Include spray chart HTML and stats
        rendered_html = template.render(
            hitter_name=formatted_name,
            date=date,
            summary_stats=summary_stats,
            hitting_data=batted_balls,  # Use batted balls for table (avoids null errors)
            spray_chart_data=spray_chart_data,  # Use spray_chart_data for spray chart
            contact_data=contact_data,
            contact_stats=contact_stats,
            spray_stats=spray_chart_stats,  # Use the pre-calculated spray stats
            spray_balls_html=spray_balls_html,  # Add the pre-generated spray chart HTML
            side_view_points_html=side_view_points,
            overhead_view_points_html=overhead_view_points
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
            print(f"PDF generated successfully for {formatted_name} with contact analysis and spray chart")
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