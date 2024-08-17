from flask import Flask, jsonify, request
from flask_cors import CORS
import pybaseball as pb
import pandas as pd
from datetime import datetime, timedelta
from pybaseball import statcast_pitcher_spin
from pybaseball import cache
import numpy as np

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # This will enable CORS for all routes
cache.enable()


@app.route('/')
def home():
    return "Welcome to the Baseball Stats API!"


@app.route('/get/name', methods=['GET'])
def get_player_names():
    try:
        # Get the 'ids' parameter from the request and split it into a list of strings
        ids_str = request.args.get('ids', '')
        if not ids_str:
            return jsonify({"error": "No IDs provided"}), 400

        # Convert the list of IDs from strings to integers
        ids = [int(id_str)
               for id_str in ids_str.split(',') if id_str.isdigit()]

        # Use playerid_reverse_lookup with 'mlbam' key_type
        player_info = pb.playerid_reverse_lookup(ids, key_type='mlbam')

        if not player_info.empty:
            # Create a dictionary of player names
            player_names = {}
            for index, row in player_info.iterrows():
                first_name = row['name_first'].capitalize()
                last_name = row['name_last'].capitalize()
                player_name = f"{first_name} {last_name}"
                player_names[row['key_mlbam']] = player_name

            return jsonify({"player_names": player_names}), 200
        else:
            return jsonify({"error": "Players not found"}), 404
    except Exception as e:
        # Return an error message if something goes wrong
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


@app.route('/all/pitchers', methods=['GET'])
def get_team_ids():
    try:
        # Fetch the team pitching data
        year = datetime.today().year

        team_data = pb.team_pitching_bref('MIA', year)

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

        # Fetch pitch-by-pitch data for the specified date range
        player_stats = pb.statcast_pitcher(opening_day_date, current_date, player_id)
        print("player stat length: ", player_stats.shape[0])
        if player_stats.empty:
            return jsonify({"error": "No pitch data found for the specified date range"}), 404

        # Ensure the necessary columns are present in the dataset
        required_columns = [
            'pitch_type', 'release_speed', 'pfx_x', 'pfx_z', 'balls', 'strikes', 'type', 'events',
            'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'bb_type',
            'launch_speed', 'spin_axis', 'release_spin_rate', 'description'
        ]

        if not all(col in player_stats.columns for col in required_columns):
            return jsonify({"error": "Required columns not found in the data."}), 400

        # Calculate the hypotenuse of pfx_x and pfx_z
        player_stats['pfx_hypotenuse'] = np.sqrt(player_stats['pfx_x']**2 + player_stats['pfx_z']**2)
        
        # Group by pitch type and calculate stats
        def calculate_stats(group):
            total_pitches = len(group)
            balls_in_play = group[group['description'] == 'hit_into_play'].shape[0]
            total_at_bats = group[group['events'].notna()].shape[0]
            print("total at bats" , total_at_bats)
            # Batting Average (BA) = Hits / Balls in Play
            ba = group[group['events'].isin(['single', 'double', 'triple', 'home_run'])].shape[0] / balls_in_play if balls_in_play > 0 else 0

            # Whiff% = Swings and Misses / Total Pitches
            whiff = group[(group['description'] == 'swinging_strike')].shape[0] / total_pitches if total_pitches > 0 else 0

            # Slugging Percentage (SLG) = Total Bases / Total At Bats
            slug = (
                group['events'].apply(lambda x: 1 if x == 'single' else (2 if x == 'double' else (3 if x == 'triple' else 4 if x=='home_run' else 0)))
            ).sum() / total_at_bats if total_at_bats > 0 else 0

            # HardHit% = Balls hit with launch_speed > 95 mph / Balls in Play
            hardhit = group[(group['launch_speed'] > 95) & (group['description'] == 'hit_into_play')].shape[0] / balls_in_play if balls_in_play > 0 else 0

            # Average Spin Rate
            avg_spin_rate = group['release_spin_rate'].mean()
            avg_velo_rate = group['release_speed'].mean()

            # Average Spin Axis
            avg_spin_axis = group['spin_axis'].mean()

            # Average Hypotenuse
            avg_pfx_hypotenuse = group['pfx_hypotenuse'].mean()

            return pd.Series({
                'Total Pitches': total_pitches,
                'Avg Velocity': avg_velo_rate,
                'BA': ba,
                'Whiff%': whiff,
                'SLG': slug,
                'HardHit%': hardhit,
                'Avg Spin Rate': avg_spin_rate,
                'Avg Spin Axis': avg_spin_axis,
                'Avg PFX Hypotenuse': avg_pfx_hypotenuse
            })

        # Group by 'pitch_type' and apply the calculate_stats function
        stats_by_pitch_type = player_stats.groupby('pitch_type').apply(calculate_stats).reset_index()
        stats_by_pitch_type_dict = stats_by_pitch_type.to_dict(orient='records')

        # Combine the calculated statistics with the pitch arsenal data
        response_data = {
            "data": stats_by_pitch_type_dict,
        }

        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
@app.route('/stats/pitcher/recent/<int:player_id>', methods=['GET'])
def get_recent_pitcher_stats(player_id):
    try:
        # Calculate the current date and the date two weeks ago
        end_date = datetime.today().strftime('%Y-%m-%d')
        # start_date = (datetime.today() - timedelta(weeks=3)
        #               ).strftime('%Y-%m-%d')
        start_date = '2024-03-28'
        # Fetch player stats using the calculated date range
        player_stats = pb.statcast_pitcher(start_date, end_date, player_id)

        # Sort by game_date or game_pk and select the last three unique game_pk values
        last_three_game_pks = player_stats.sort_values(
            by='game_pk', ascending=False)['game_pk'].unique()

        # Filter the DataFrame to include only the rows with these game_pk values
        last_three_games = player_stats[player_stats['game_pk'].isin(
            last_three_game_pks)]

        # Initialize the result dictionary
        result = {}

        # Iterate over the filtered DataFrame grouped by game_pk
        for game_pk, group in last_three_games.groupby('game_pk'):
            # Get the first row to extract game_date and determine the opponent
            first_row = group.iloc[0]
            game_date = first_row['game_date']
            home_team = first_row['home_team']
            away_team = first_row['away_team']

            # Determine the opponent based on the pitcher's team
            opponent = away_team if home_team == 'MIA' else home_team

            # Include the extracted data in the result dictionary
            result[game_pk] = {
                "game_date": game_date,
                "opponent": opponent,
                # Original game details
                "details": group.to_dict(orient='records')
            }

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/stats/pitcher/by_count/<int:player_id>', methods=['GET'])
def get_pitcher_stats(player_id):
    try:
        current_date = datetime.today().strftime('%Y-%m-%d')
        opening_day_date = '2024-03-28'
        # Fetch pitcher data for the specified date range
        player_stats = pb.statcast_pitcher(
            opening_day_date, current_date, player_id)

        # Ensure 'balls', 'strikes', 'pitch_type', and 'type' columns are in the DataFrame
        required_columns = ['balls', 'strikes', 'pitch_type', 'type', 'events']
        if not all(col in player_stats.columns for col in required_columns):
            return jsonify({"error": "Required columns not found in the data."}), 400

        # Create a new column to represent pitch count scenarios
        player_stats['count_scenario'] = player_stats.apply(
            lambda row: f"{row['balls']}-{row['strikes']}", axis=1)

        # Group by count scenario and pitch type, and get the count of each combination
        count_pitch_distribution = player_stats.groupby(
            ['count_scenario', 'pitch_type']).size().unstack(fill_value=0)
        count_result = player_stats.groupby(
            ['count_scenario', 'pitch_type', 'type']).size().unstack(fill_value=0)

        count_result_ball = count_result.get(
            'B', pd.Series(0, index=count_result.index, dtype=int))
        count_result_strike = count_result.get(
            'S', pd.Series(0, index=count_result.index, dtype=int))
        count_result_in_play = count_result.get(
            'X', pd.Series(0, index=count_result.index, dtype=int))

        # Calculate total counts and avoid division by zero
        total_counts = count_result_ball + count_result_strike + count_result_in_play
        # Replace 0 to avoid division by zero
        total_counts = total_counts.replace(0, 1)

        # Calculate percentages
        count_ball_percentage = (count_result_ball / total_counts) * 100
        count_strike_percentage = (count_result_strike / total_counts) * 100
        count_in_play_percentage = (count_result_in_play / total_counts) * 100

        # Calculate pitch counts and percentages for each pitch count scenario
        pitch_count_totals = count_pitch_distribution.sum(axis=1)
        pitch_percentages = count_pitch_distribution.div(
            pitch_count_totals, axis=0) * 100

        stats = {}
        for count_scenario in count_pitch_distribution.index:
            stats[count_scenario] = {}
            for pitch_type in count_pitch_distribution.columns:
                count = count_pitch_distribution.loc[count_scenario, pitch_type]
                count = int(count)

                # Calculate percentages for the current count_scenario and pitch_type
                ball_percent = count_ball_percentage.get(
                    count_scenario, pd.Series(0)).get(pitch_type, 0)
                strike_percent = count_strike_percentage.get(
                    count_scenario, pd.Series(0)).get(pitch_type, 0)
                in_play_percent = count_in_play_percentage.get(
                    count_scenario, pd.Series(0)).get(pitch_type, 0)
                total_pitch_percentage = pitch_percentages.loc[count_scenario,
                                                               pitch_type] if not pitch_percentages.empty else 0
                stats[count_scenario][pitch_type] = {
                    'total_pitch_count': count,
                    'total_pitch_percentage': round(total_pitch_percentage, 2),
                    'ball_percentage': round(float(ball_percent), 2),
                    'strike_percentage': round(float(strike_percent), 2),
                    'in_play_percentage': round(float(in_play_percent), 2)
                }

        return jsonify(stats), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/stats/pitcher/hit_outcome/<int:player_id>', methods=['GET'])
def get_hit_result(player_id):
    try:
        current_date = datetime.today().strftime('%Y-%m-%d')
        opening_day_date = '2024-03-28'
        
        # Fetch pitcher data for the specified date range
        player_stats = pb.statcast_pitcher(
            opening_day_date, current_date, player_id)
        
        # Filter for hit outcomes
        hit_outcomes = player_stats[
            player_stats['description'].isin(['hit_into_play'])
        ][['launch_angle', 'bb_type', 'launch_speed', 'launch_speed_angle', 'hc_x', 'hc_y', 'hit_distance_sc', 'events']]
        
        if hit_outcomes.empty:
            return jsonify({"message": "No hit outcomes found for this player."}), 404
        
        # Calculate summary statistics
        summary = {
            "total_hits": hit_outcomes.shape[0],
            "hits_by_type": hit_outcomes['events'].value_counts().to_dict(),
            "mean_launch_speed": hit_outcomes['launch_speed'].mean(),
            "std_dev_launch_speed": hit_outcomes['launch_speed'].std(),
            "min_launch_speed": hit_outcomes['launch_speed'].min(),
            "max_launch_speed": hit_outcomes['launch_speed'].max(),
            "mean_launch_angle": hit_outcomes['launch_angle'].mean(),
            "std_dev_launch_angle": hit_outcomes['launch_angle'].std(),
            "min_launch_angle": hit_outcomes['launch_angle'].min(),
            "max_launch_angle": hit_outcomes['launch_angle'].max(),
            "mean_hit_distance": hit_outcomes['hit_distance_sc'].mean(),
            "std_dev_hit_distance": hit_outcomes['hit_distance_sc'].std(),
            "min_hit_distance": hit_outcomes['hit_distance_sc'].min(),
            "max_hit_distance": hit_outcomes['hit_distance_sc'].max(),
        }
        
        # Convert hit outcomes DataFrame to a list of dictionaries
        hit_outcomes_list = hit_outcomes.to_dict(orient='records')
        
        # Return both the summary and detailed hit outcomes
        return jsonify({"summary": summary, "hit_outcomes": hit_outcomes_list}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/stats/pitcher/progression/<int:player_id>', methods=['GET'])
def get_progression(player_id):
    try:
        current_date = datetime.today().strftime('%Y-%m-%d')
        opening_day_date = '2024-03-28'
        
        # Fetch pitcher data for the specified date range
        player_stats = pb.statcast_pitcher(
            opening_day_date, current_date, player_id)
        
        # Filter for relevant columns
        relevant_stats = player_stats[['game_date', 'pitch_type', 'release_speed', 'release_spin_rate', 'spin_axis', 
                                       'release_extension', 'pfx_x', 'pfx_z', 'description']]
        
        if relevant_stats.empty:
            return jsonify({"message": "No data found for this player."}), 404
        
        # Calculate the hypotenuse of pfx_x and pfx_z using the Pythagorean theorem
        relevant_stats['pfx_hypotenuse'] = np.sqrt(relevant_stats['pfx_x']**2 + relevant_stats['pfx_z']**2)
        
        # Calculate strike percentage by game
        relevant_stats['is_strike'] = relevant_stats['description'].isin(['called_strike', 'swinging_strike', 'foul', 'foul_tip', 'swinging_strike_blocked'])
        strike_percentage = relevant_stats.groupby('game_date').agg({
            'is_strike': 'mean'
        }).reset_index()
        strike_percentage['strike_percentage'] = strike_percentage['is_strike'] * 100
        strike_percentage = strike_percentage[['game_date', 'strike_percentage']]
        
        # Group by game and pitch type, then calculate mean velocity, spin rate, spin axis, release extension, pfx_x, pfx_z, and pfx_hypotenuse
        grouped_stats = relevant_stats.groupby(['game_date', 'pitch_type']).agg({
            'release_speed': 'mean',
            'release_spin_rate': 'mean',
            'spin_axis': 'mean',
            'release_extension': 'mean',
            'pfx_x': 'mean',
            'pfx_z': 'mean',
            'pfx_hypotenuse': 'mean'  # Mean hypotenuse
        }).reset_index()
        
        # Flatten the MultiIndex columns
        grouped_stats.columns = [
            'game_date', 'pitch_type', 'mean_release_speed', 'mean_release_spin_rate', 
            'mean_spin_axis', 'mean_release_extension', 'mean_pfx_x', 'mean_pfx_z', 'mean_pfx_hypotenuse'
        ]
        
        # Merge strike percentage with the grouped stats
        progression_data = grouped_stats.merge(strike_percentage, on='game_date', how='left')
        
        # Convert the grouped data to a list of dictionaries
        progression_data = progression_data.to_dict(orient='records')
        
        # Return the progression data grouped by game and pitch type
        return jsonify({"progression": progression_data}), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
