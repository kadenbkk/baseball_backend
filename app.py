from flask import Flask, jsonify
import pybaseball as pb
import pandas as pd
from datetime import datetime, timedelta
from pybaseball import statcast_pitcher_spin

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Baseball Stats API!"


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

@app.route('/all/pitchers', methods=['GET'])
def get_team_ids():
    try:
        # Fetch the team pitching data
        team_data = pb.team_pitching_bref('MIA', 2024)

        # Convert the data to a list of dictionaries
        raw_data = team_data.to_dict(orient='records')

        # Filter out players with "40-man" or "60-day IL" in their names
        filtered_data = [
            player for player in raw_data
            if not ("40-man" in player['Name'] or "60-day IL" in player['Name'] or "DFA" in player['Name'])
        ]

        # Extract pitcher names from the filtered data
        pitcher_names = [player['Name'] for player in filtered_data]

        # Prepare the response
        response = {
            "data": filtered_data,
            "pitcher_names": pitcher_names
        }

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/stats/pitcher/by_arsenal/<int:player_id>', methods=['GET'])
def check_player_stats(player_id):
    try:
        # Get the current date and the opening day date for the season
        current_date = datetime.today().strftime('%Y-%m-%d')
        opening_day_date = '2024-03-28'
        
        # Fetch the original arsenal data for the year 2024
        arsenal_data = pb.statcast_pitcher_arsenal_stats(2024)
        
        if arsenal_data.empty:
            return jsonify({"error": "No arsenal data found for the specified year"}), 404
        
        # Filter the arsenal data to only include the specified player_id
        player_arsenal_data = arsenal_data[arsenal_data['player_id'] == player_id]
        
        if player_arsenal_data.empty:
            return jsonify({"error": "No arsenal data found for the specified player_id"}), 404
        
        # Fetch pitch-by-pitch data for the specified date range
        player_stats = pb.statcast_pitcher(opening_day_date, current_date, player_id)
        
        if player_stats.empty:
            return jsonify({"error": "No pitch data found for the specified date range"}), 404
        
        
        # Ensure the necessary columns are present in both datasets
        required_pitch_stats_columns = ['pitch_type', 'release_speed', 'pfx_x', 'pfx_z']
        
        if not all(col in player_stats.columns for col in required_pitch_stats_columns):
            return jsonify({"error": "Required columns not found in the pitch data."}), 400
        
        
        # Calculate the average velocity for each pitch type
        pitch_velocities = player_stats.groupby('pitch_type')['release_speed'].mean().reset_index()
        pitch_velocities = pitch_velocities.rename(columns={'release_speed': 'average_velocity'})
        
        # Calculate average pitch movement (pfx_x and pfx_z) for each pitch type
        pitch_movement_x = player_stats.groupby('pitch_type')['pfx_x'].mean().reset_index()
        pitch_movement_x = pitch_movement_x.rename(columns={'pfx_x': 'average_horizontal_movement'})
        
        pitch_movement_z = player_stats.groupby('pitch_type')['pfx_z'].mean().reset_index()
        pitch_movement_z = pitch_movement_z.rename(columns={'pfx_z': 'average_vertical_movement'})
        

        
        # Convert the processed data to dictionary format
        pitch_velocities_dict = pitch_velocities.to_dict(orient='records')
        pitch_movement_x_dict = pitch_movement_x.to_dict(orient='records')
        pitch_movement_z_dict = pitch_movement_z.to_dict(orient='records')
        
        # Combine the original arsenal data with the calculated statistics
        response_data = {
            "original_arsenal": player_arsenal_data.to_dict(orient='records'),
            "pitch_velocities": pitch_velocities_dict,
            "pitch_movement_x": pitch_movement_x_dict,
            "pitch_movement_z": pitch_movement_z_dict,
        }
        
        return jsonify(response_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/stats/pitcher/recent/<int:player_id>', methods=['GET'])
def get_recent_pitcher_stats(player_id):
    try:
        # Calculate the current date and the date two weeks ago
        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(weeks=2)).strftime('%Y-%m-%d')

        # Fetch player stats using the calculated date range
        player_stats = pb.statcast_pitcher(start_date, end_date, player_id)
        
        # Sort by game_date or game_pk and select the last three unique game_pk values
        last_three_game_pks = player_stats.sort_values(by='game_pk', ascending=False)['game_pk'].unique()[:3]
        
        # Filter the DataFrame to include only the rows with these game_pk values
        last_three_games = player_stats[player_stats['game_pk'].isin(last_three_game_pks)]
        
        grouped_data = last_three_games.groupby('game_pk')

        result = {game_pk: group.to_dict(orient='records') for game_pk, group in grouped_data}
        
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/stats/pitcher/by_count/<int:player_id>', methods=['GET'])
def get_pitcher_stats(player_id):
    try:
        current_date = datetime.today().strftime('%Y-%m-%d')
        opening_day_date = '2024-03-28'  
        # Fetch pitcher data for the specified date range
        player_stats = pb.statcast_pitcher(opening_day_date, current_date, player_id)
        
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
