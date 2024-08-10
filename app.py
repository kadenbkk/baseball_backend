from flask import Flask, jsonify
import pybaseball as pb
import pandas as pd
from datetime import datetime, timedelta

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Baseball Stats API!"

@app.route('/recent_games/<string:team>', methods=['GET'])
def recent_games(team):
    try:
        # Fetch schedule and record data for the specified year and team
        current_date = datetime.now()
        year = current_date.year
        data = pb.schedule_and_record(year, team)
        
        if data.empty:
            return jsonify({"error": "No data found for the specified year and team"}), 404
        
        # Debugging: print the first few rows of the data
        print("Data preview:", data.head())

        # Add the year to the 'Date' column and convert it to datetime
        data['Date'] = data['Date'].apply(lambda d: f"{d}, {year}")  # Add year to date
        data['Date'] = pd.to_datetime(data['Date'], format='%A, %b %d, %Y', errors='coerce')
        
        # Debugging: print the first few rows after conversion
        print("Data with converted dates:", data.head())

        # Calculate the date one month ago from today
        one_month_ago = datetime.now() - timedelta(days=15)
        
        # Debugging: print the date range for filtering
        print("Filtering data from:", one_month_ago)

        # Filter data to include only records from the last month
        recent_data = data[(data['Date'] >= one_month_ago) & (data['Date'] < current_date)]

        
        # Debugging: print the filtered data
        print("Filtered data:", recent_data)

        if recent_data.empty:
            return jsonify({"error": "No data found for the last month"}), 404

        # Process the data as needed (you can add specific processing here)
        processed_data = recent_data.to_dict(orient='records')
        
        return jsonify(processed_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/id/<string:firstName>/<string:lastName>', methods=['GET'])
def get_player_id(firstName, lastName):
    try:
        player_info = pb.playerid_lookup(lastName, firstName)
        if not player_info.empty:
            player_id = int(player_info.iloc[0]['key_mlbam'])
            return jsonify({"player_id": player_id}), 200
        else:
            return jsonify({"error": "Player not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/stats/pitcher/by_arsenal/<int:player_id>', methods=['GET'])
def check_player_stats(player_id):
    try:
        # Fetch pitcher data for the year 2024
        data = pb.statcast_pitcher_arsenal_stats(2024)
        
        if data.empty:
            return jsonify({"error": "No data found for the specified year"}), 404
        
        # Filter the data to only include the specified player_id
        player_data = data[data['player_id'] == player_id]
        
        if player_data.empty:
            return jsonify({"error": "No data found for the specified player_id"}), 404
        
        # Process the data as needed (you can add specific processing here)
        processed_data = player_data.to_dict(orient='records')
        
        return jsonify(processed_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400



@app.route('/stats/pitcher/by_count/<int:player_id>', methods=['GET'])
def get_player_stats(player_id):
    try:
        # Fetch pitcher data for the specified date range
        player_stats = pb.statcast_pitcher('2024-03-28', '2024-08-09', player_id)
        
        # Ensure 'balls', 'strikes', 'pitch_type', and 'type' columns are in the DataFrame
        required_columns = ['balls', 'strikes', 'pitch_type', 'type', 'events']
        if not all(col in player_stats.columns for col in required_columns):
            return jsonify({"error": "Required columns not found in the data."}), 400
        
        # Create a new column to represent pitch count scenarios
        player_stats['count_scenario'] = player_stats.apply(lambda row: f"{row['balls']}-{row['strikes']}", axis=1)
        
        # Group by count scenario and pitch type, and get the count of each combination
        count_pitch_distribution = player_stats.groupby(['count_scenario', 'pitch_type']).size().unstack(fill_value=0)
        count_result = player_stats.groupby(['count_scenario', 'pitch_type', 'type']).size().unstack(fill_value=0)
        
        unique_events = player_stats.groupby(['count_scenario', 'pitch_type'])['events'].unique()

        count_result_ball = count_result.get('B', pd.Series(0, index=count_result.index, dtype=int))
        count_result_strike = count_result.get('S', pd.Series(0, index=count_result.index, dtype=int))
        count_result_in_play = count_result.get('X', pd.Series(0, index=count_result.index, dtype=int))
        
        # Calculate total counts and avoid division by zero
        total_counts = count_result_ball + count_result_strike + count_result_in_play
        total_counts = total_counts.replace(0, 1)  # Replace 0 to avoid division by zero
        
        # Calculate percentages
        count_ball_percentage = (count_result_ball / total_counts) * 100
        count_strike_percentage = (count_result_strike / total_counts) * 100
        count_in_play_percentage = (count_result_in_play / total_counts) * 100

        # Calculate pitch counts and percentages for each pitch count scenario
        pitch_count_totals = count_pitch_distribution.sum(axis=1)
        pitch_percentages = count_pitch_distribution.div(pitch_count_totals, axis=0) * 100
        
        stats = {}
        for count_scenario in count_pitch_distribution.index:
            stats[count_scenario] = {}
            for pitch_type in count_pitch_distribution.columns:
                count = count_pitch_distribution.loc[count_scenario, pitch_type]
                count = int(count)
                
                # Calculate percentages for the current count_scenario and pitch_type
                ball_percent = count_ball_percentage.get(count_scenario, pd.Series(0)).get(pitch_type, 0)
                strike_percent = count_strike_percentage.get(count_scenario, pd.Series(0)).get(pitch_type, 0)
                in_play_percent = count_in_play_percentage.get(count_scenario, pd.Series(0)).get(pitch_type, 0)
                total_pitch_percentage = pitch_percentages.loc[count_scenario, pitch_type] if not pitch_percentages.empty else 0
                events_list = unique_events.get((count_scenario, pitch_type), [])
                stats[count_scenario][pitch_type] = {
                    'unique_events': list(events_list),
                    'total_pitch_count': count,
                    'total_pitch_percentage': round(total_pitch_percentage,2),
                    'ball_percentage': round(float(ball_percent), 2),
                    'strike_percentage': round(float(strike_percent), 2),
                    'in_play_percentage': round(float(in_play_percent), 2)
                }

        return jsonify(stats), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
