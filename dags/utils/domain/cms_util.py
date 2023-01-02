import json
import logging
import pandas as pd
from datetime import datetime

log = logging.getLogger(__name__)

class CMSUtil:

    @staticmethod
    def validate_json_data(json_str):
        """Validates if the input string is a valid JSON or not

        Args:
            json_str (str): Input JSON string

        Returns:
            bool
        """
        try:
            log.info('Validating JSON response')
            json.loads(json_str)
        except ValueError as e:
            log.error('Invalid JSON')
            return False
        log.info('Valid JSON Response')
        return True

    @staticmethod
    def flatten_sport_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for Sports from the JSON

        Args:
            response_data (str): API Json response string

        Returns:
            str_buffer
        """
        if CMSUtil.validate_json_data(response_data):
            
            # Create seperate df for item array
            log.info('Creating Sports datafram for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['items'])

            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt','fields.category', 'fields.intensityLevels', 'fields.images', 'fields.name',
                            'sys.id', 'fields.places', 'fields.visible']]
            req_items_df = req_items_df.dropna()

            # Create seperate df for entry array
            log.info('Creating datafram for entry array')
            entry_df = pd.json_normalize(json.loads(response_data),record_path=['includes', 'Entry'])

            req_entry_df = entry_df[['sys.id', 'fields.name', 'fields.met', 'fields.speed', 'fields.unit',
                            'fields.internalName', 'fields.text', 'fields.url']]

            output_df = pd.DataFrame()
            # Loop over items df and find corresponding img and intensity value in entry df based on id
            log.info('Flattening response JSON with required columns')
            for index, row in req_items_df.iterrows():
                intensity_levels_len = len(row['fields.intensityLevels'])
                images_length = len(row['fields.images'])
                intensity_level_list = []
                media_list = []

                for i in range(0, intensity_levels_len):
                    id = row['fields.intensityLevels'][i]['sys']['id']
                    filtered_df = req_entry_df[req_entry_df.loc[:, 'sys.id'] == id]

                    if filtered_df.empty:
                        intensity_level_list.append({"met": None, "name": None})
                    else:
                        intensity_level_list.append({"met": filtered_df['fields.met'].item(), "name": filtered_df['fields.name'].item()})

                for i in range(0, images_length):
                    id = row['fields.images'][i]['sys']['id']
                    filtered_df = req_entry_df[req_entry_df.loc[:, 'sys.id'] == id]
                    if filtered_df.empty:
                        media_list.append(
                        {"description": None, "location": None})
                    else:
                         media_list.append(
                        {"description": filtered_df['fields.text'].item(), "location": filtered_df['fields.url'].item()})

                output_df = output_df.append({'CATEGORY': row['fields.category'], 'INTENSITYLEVEL': intensity_level_list,
                                        'MEDIA': {"images": media_list}, 'NAME': row['fields.name'],
                                        'PFXWORKOUTID': f"pfx:workouts:sport:{row['sys.id']}",
                                        'PLACES': row['fields.places'], 'SOURCEAPP': None, 'VISIBLE': row['fields.visible'],
                                        "CREATEDAT": datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                        "UPDATEDAT": datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3]},
                                        ignore_index=True)

            output_df['VISIBLE'] = output_df['VISIBLE'].astype(bool)        
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)

            log.info(f'Total records processed = {len(json_data)}')
            
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)
                    
            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

    @staticmethod
    def flatten_exercise_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for Exercise from the JSON

        Args:
           response_data (str): API Json response string

        Returns:
            json_str
        """
        if CMSUtil.validate_json_data(response_data):
            # Create seperate df for item array
            log.info('Creating Exercise datafram for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['items'])
            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt', 'fields.bodyParts', 'fields.equipment', 'fields.difficulty', 'fields.thumbnailUrl',
                         'fields.thumbnailImage.sys.id', 'fields.videoUrl', 'fields.name', 'sys.id',
                         'fields.tips']]

             # Create seperate df for asset array
            log.info('Creating datafram for asset array')
            asset_df = pd.json_normalize(json.loads(response_data), record_path=['includes', 'Asset'])
            req_asset_df = asset_df[['sys.id', 'fields.description']]
            output_df = pd.DataFrame()

            # Loop over items df and find corresponding media value(description) in asset df based on id
            log.info('Flattening response JSON with required columns')
            for index, row in req_items_df.iterrows():
                id = row['fields.thumbnailImage.sys.id']
                filtered_df = req_asset_df[req_asset_df.loc[:, 'sys.id'] == id]

                if filtered_df.empty:
                    media_dict = {'images': [{'description': None,
                                            'location': None}],
                                'video': [{'description': None,
                                            'location': None}]}
                else:
                    media_dict = {'images': [{'description': filtered_df['fields.description'].item(),
                                            'location': row['fields.thumbnailUrl']}],
                                'video': [{'description': 'main',
                                            'location': row['fields.videoUrl']}]}

                output_df = output_df.append({'BODYPARTS': row['fields.bodyParts'], 
                                            'DESCRIPTION': 'Exercise description',
                                            'EQUIPMENT': row['fields.equipment'], 
                                            'EXPERIENCELEVEL': row['fields.difficulty'],
                                            'MEDIA': media_dict, 
                                            'NAME': row['fields.name'],
                                            'PFXEXERCISEID': f"pfx:exercises:{row['sys.id']}", 
                                            "TIPS": row['fields.tips'],
                                            "CREATEDAT": datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                            "UPDATEDAT": datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3]},
                                            ignore_index=True)

             
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
            
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

    @staticmethod
    def flatten_routine_workoutLayout_json(longFormWorkout_data,category_data):
            """
            Flattens the nested JSON and extracts required columns for Routine workoutLayout type from the JSON
            Args:
            data (str): API Json response string
            Returns:
                json_str
            """
            if CMSUtil.validate_json_data(longFormWorkout_data):
                # Create seperate df for item array
                
                log.info('Creating longFormWorkout datafram for items array')
                items_df = pd.json_normalize(json.loads(longFormWorkout_data), record_path=['items'])

                req_items_df = items_df[['sys.id', 'sys.createdAt', 'sys.updatedAt', 'fields.bodyArea', 'fields.digital',
                         'fields.description', 'fields.duration', 'fields.equipment', 'fields.fitnessLevel',
                         'fields.goals', 'fields.thumbnailImages', 'fields.videos', 'fields.metValue', 'fields.name',
                         'fields.trainers', 'fields.calories', 'fields.startDate', 'fields.endDate',
                         'fields.workoutExperiences', 'fields.workoutType']]

                log.info('Creating longFormWorkout datafram for entry array')
                entry_df = pd.json_normalize(json.loads(longFormWorkout_data), record_path=['includes', 'Entry'])

                log.info('Creating longFormWorkout datafram for asset array')
                asset_df = pd.json_normalize(json.loads(longFormWorkout_data), record_path=['includes', 'Asset'])

                log.info('Creating category datafram for items array')
                category_df = pd.json_normalize(json.loads(category_data), record_path=['items'])

                req_category_df = category_df[['sys.id', 'fields.name', 'fields.workouts']]
                flt_req_category_df = req_category_df.explode('fields.workouts')

                flt_req_category_df['fields.workouts'] = [{**workouts, **{'name': name}}
                                                            for workouts, name in
                                                            zip(flt_req_category_df['fields.workouts'],
                                                                flt_req_category_df['fields.name'])]

                flt_req_category_df = pd.DataFrame(flt_req_category_df['fields.workouts'].tolist())
                flt_req_category_df['sys'] = [{**sys, **{'name': name}} for sys, name in zip(flt_req_category_df['sys'], flt_req_category_df['name'])]

                flt_req_category_df = pd.DataFrame(flt_req_category_df['sys'].tolist())

                output_df = pd.DataFrame()

                for index, row in req_items_df.iterrows():

                    category_list = list(set(flt_req_category_df[flt_req_category_df['id'] == row['sys.id']]['name'].values.tolist()))

                    if isinstance(row['fields.thumbnailImages'], list):
                        images_length = len(row['fields.thumbnailImages'])
                    else:
                        images_length = 0

                    if isinstance(row['fields.videos'], list):
                        videos_length = len(row['fields.videos'])
                    else:
                        videos_length = 0

                    images = []
                    videos = []

                    for i in range(0, images_length):
                        img_id = row['fields.thumbnailImages'][i]['sys']['id']
                        filtered_asset_df = asset_df[asset_df.loc[:, 'sys.id'] == img_id]

                        images.append({"description": ''.join(filtered_asset_df['fields.description'].astype(str)),
                            "location": ''.join(filtered_asset_df['fields.file.url'].astype(str))})

                    for i in range(0, videos_length):
                        vdo_id = row['fields.videos'][i]['sys']['id']
                        filtered_media_entry_df = entry_df[entry_df.loc[:, 'sys.id'] == vdo_id]
                        videos.append({"description": ''.join(filtered_media_entry_df['fields.description'].astype(str)),
                            "location": ''.join(filtered_media_entry_df['fields.url'].astype(str))})

                    output_df = output_df.append({'BODYAREA': row['fields.bodyArea'],
                                                  'BODYPARTS': [],
                                                  'CATEGORY': category_list,
                                                  'CATEGORYORDER': 0,
                                                  'CONTENTTYPE': ([row['fields.digital']]
                                                          if pd.notnull(row['fields.digital']) else []),
                                                  'DESCRIPTION': row['fields.description'],
                                                  'DURATIONINSECONDS': row['fields.duration'],
                                                  'EQUIPMENT': row['fields.equipment'],
                                                  'EXERCISEDURATIONINSECONDS': 0,
                                                  'EXPERIENCELEVEL': row['fields.fitnessLevel'],
                                                  'FEATURED': False,
                                                  'FITNESSGOALS': row['fields.goals'],
                                                  'FORMAT': row['fields.workoutType'],
                                                  'MEDIA': {'images': images, 'video': videos},
                                                  'MET': row['fields.metValue'],
                                                  'NAME': row['fields.name'],
                                                  'PFXWORKOUTID': f"pfx:workouts:routine:{row['sys.id']}",
                                                  'POTENTIALCALORIESBURNED': (row['fields.calories']
                                                                  if pd.notnull(row['fields.calories']) else 0),
                                                  'RESTDURATIONINSECONDS': 0,
                                                  'SETS': [],
                                                  'TRAINERS': row['fields.trainers'],
                                                  'VISIBILITY': {'endDate': (datetime.strftime(datetime.strptime(row['fields.endDate'], '%Y-%m-%d'), '%Y-%m-%dT%H:%M:%SZ')
                                                             if pd.notnull(row['fields.endDate']) else ''),
                                                                'startDate':(datetime.strftime(datetime.strptime(row['fields.startDate'], '%Y-%m-%d'), '%Y-%m-%dT%H:%M:%SZ')
                                                             if pd.notnull(row['fields.startDate']) else '')},
                                                  'WORKOUTEXPERIENCES': row['fields.workoutExperiences'],
                                                  "CREATEDAT": datetime.strftime(datetime.strptime(
                                                      row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                                                          '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                  "UPDATEDAT": datetime.strftime(datetime.strptime(
                                                      row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                                                          '%Y-%m-%d %H:%M:%S.%f')[:-3]
                                                  },
                                                 ignore_index=True)
                output_df['FEATURED'] = output_df['FEATURED'].astype(bool)
                output_str = output_df.to_json(orient='records')
                output_json = json.loads(output_str)

                out_json_str = None
                for elem in output_json:
                    if out_json_str is not None:
                        out_json_str = out_json_str + '\n' + json.dumps(elem)
                    else:
                        out_json_str = json.dumps(elem)

                return out_json_str
            else:
                # TODO Confirm if error records needs to be saved in S3.
                log.error('Input JSON response is not a valid JSON')
                return False
    @staticmethod
    def flatten_routine_standardWorkout_json(data):
        """
            Flattens the nested JSON and extracts required columns for Routine StandardWorkout type from the JSON
            Args:
                data (str): API Json response string

            Returns:
                json_str
        """
        if CMSUtil.validate_json_data(data):
            response_data = json.loads(data)
            log.info('Creating dataframe for items array')
            items_df = pd.json_normalize(response_data,
                                record_path=['items'])
        
            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt', 'sys.id', 'fields.name', 'fields.description', 'fields.duration', 'fields.metValue',
                                'fields.bodyArea', 'fields.equipment', 'fields.goals', 'fields.fitnessLevel',
                                'fields.workoutExperiences', 'fields.startDate', 'fields.endDate', 'fields.images',
                                'fields.circuits']]
            
            log.info('Creating dataframe for Asset array')
            asset_df = pd.json_normalize(response_data,
                                    record_path=['includes', 'Asset'])

            log.info('Creating dataframe for entry array')
            entry_df = pd.json_normalize(response_data,
                                    record_path=['includes', 'Entry'])

            media_entry_df = entry_df[entry_df.loc[:, 'sys.contentType.sys.id'] == 'media']

            circuit_entry_df = entry_df[entry_df.loc[:, 'sys.contentType.sys.id'] == 'circuit']

            step_entry_df = entry_df[entry_df.loc[:, 'sys.contentType.sys.id'] == 'step']

            exercise_entry_df = entry_df[entry_df.loc[:, 'sys.contentType.sys.id'] == 'exercise']

            output_df = pd.DataFrame()
                                
            for index, item_row in req_items_df.iterrows():
                # check if fields has all values empty (name is required parameter)
                if isinstance(item_row['fields.name'], float):
                    continue
                if isinstance(item_row['fields.circuits'], list):
                    len_circuits = len(item_row['fields.circuits'])
                else:
                    len_circuits = 0
                total_exercise_duration = 0
                sets = []
                for i in range(0, len_circuits):
                    circuit_id = item_row['fields.circuits'][i]['sys']['id']
                    filtered_circuit_entry_df = circuit_entry_df[circuit_entry_df.loc[:, 'sys.id'] == circuit_id]
                    req_filtered_circuit_entry_df = filtered_circuit_entry_df[['sys.id', 'fields.name', 'fields.repetition',
                                                                            'fields.restDuration', 'fields.steps']]
                    circuit_row = req_filtered_circuit_entry_df.iloc[0]

                    if isinstance(circuit_row['fields.steps'], list):
                        len_steps = len(circuit_row['fields.steps'])
                    else:
                        len_steps = 0

                    steps = []
                    exercise_duration = 0
                    for j in range(0, len_steps):
                        steps_id = circuit_row['fields.steps'][j]['sys']['id']
                        filtered_step_entry_df = step_entry_df[step_entry_df.loc[:, 'sys.id'] == steps_id]
                        req_filtered_step_entry_df = filtered_step_entry_df[['sys.id', 'fields.exerciseName', 'fields.duration',
                                                                            'fields.restDuration', 'fields.exercise.sys.id']]

                        step_row = req_filtered_step_entry_df.iloc[0]
                        exercise_id = step_row['fields.exercise.sys.id']

                        filtered_exercise_entry_df = exercise_entry_df[exercise_entry_df.loc[:, 'sys.id'] == exercise_id]
                        req_filtered_exercise_entry_df = filtered_exercise_entry_df[['sys.id', 'fields.name', 'fields.bodyParts',
                                                                                    'fields.difficulty', 'fields.equipment',
                                                                                    'fields.tips', 'fields.thumbnailUrl',
                                                                                    'fields.thumbnailImage.sys.id',
                                                                                    'fields.videoUrl']]

                        exercise_row = req_filtered_exercise_entry_df.iloc[0]

                        filtered_asset_df = asset_df[asset_df.loc[:, 'sys.id'] == exercise_row['fields.thumbnailImage.sys.id']]
                        images_row = filtered_asset_df.iloc[0]

                        images_list = [{"description": images_row['fields.description'],
                                        "location": f"https:{images_row['fields.file.url']}"}]

                        video_list = [{"description": "main",
                                    "location": exercise_row['fields.videoUrl']}]

                        media_dict = {"images": images_list,
                                    "videos": video_list}

                        steps.append({
                            "bodyParts": exercise_row['fields.bodyParts'],
                            "description": "Exercise description",
                            "equipment": exercise_row['fields.equipment'],
                            "exerciseDurationInSeconds": step_row['fields.duration'],
                            "experienceLevel": exercise_row['fields.difficulty'],
                            "media": media_dict,
                            "name": exercise_row['fields.name'],
                            "pfxExerciseId": f"pfx:exercises:{exercise_row['sys.id']}",
                            "restDurationInSeconds": step_row['fields.restDuration'],
                            "tips": exercise_row['fields.tips']
                        })
                        exercise_duration = exercise_duration + step_row['fields.duration']

                    sets.append({
                        "label": circuit_row['fields.name'],
                        "repetitions": circuit_row['fields.repetition'],
                        "restDurationInSeconds": circuit_row['fields.restDuration'],
                        "steps": steps
                    })
                    total_exercise_duration = total_exercise_duration + (exercise_duration * circuit_row['fields.repetition'])

                if isinstance(item_row['fields.images'], list):
                    len_images = len(item_row['fields.images'])
                else:
                    len_images = 0

                images = []
                for i in range(0, len_images):
                    img_id = item_row['fields.images'][i]['sys']['id']
                    filtered_entry_df = media_entry_df[media_entry_df.loc[:, 'sys.id'] == img_id]
                    images.append({"description": ''.join(filtered_entry_df['fields.description']),
                                "location": ''.join(filtered_entry_df['fields.url'])})

                output_df = output_df.append({
                    "BODYAREA": item_row['fields.bodyArea'],
                    "BODYPARTS": [],
                    "CATEGORY": [],
                    "CATEGORYORDER": 0,
                    "CONTENTTYPE": ["STANDARD"],
                    "DESCRIPTION": item_row['fields.description'],
                    "DURATIONINSECONDS": item_row['fields.duration'],
                    "EQUIPMENT": item_row['fields.equipment'],
                    "EXERCISEDURATIONINSECONDS": total_exercise_duration,
                    "EXPERIENCELEVEL": item_row['fields.fitnessLevel'],
                    "FEATURED": False,
                    "FITNESSGOALS": item_row['fields.goals'],
                    "FORMAT": "STANDARD",
                    "MEDIA": {"images": images, "video": []},
                    "MET": item_row['fields.metValue'],
                    "NAME": item_row['fields.name'],
                    "PFXWORKOUTID": f"pfx:workouts:routine:{item_row['sys.id']}",
                    "POTENTIALCALORIESBURNED": (item_row['fields.metValue'] * 68.0389 * (total_exercise_duration/3600)) + (1 * 68.0389 * ((item_row['fields.duration'] - total_exercise_duration)/3600)),
                    "RESTDURATIONINSECONDS": item_row['fields.duration'] - total_exercise_duration,
                    "SETS": sets,
                    "TRAINERS": [],
                    "VISIBILITY": {'endDate': datetime.strftime(datetime.strptime(item_row['fields.endDate'], '%Y-%m-%d'),
                                                                '%Y-%m-%dT%H:%M:%SZ'),
                                'startDate': datetime.strftime(datetime.strptime(
                                                                item_row['fields.startDate'], '%Y-%m-%d'),
                                                                '%Y-%m-%dT%H:%M:%SZ')},
                    "WORKOUTEXPERIENCES": item_row['fields.workoutExperiences'],
                    "CREATEDAT": datetime.strftime(datetime.strptime(item_row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                    "UPDATEDAT": datetime.strftime(datetime.strptime(item_row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3]
                }, ignore_index=True)

            output_df['FEATURED'] = output_df['FEATURED'].astype(bool)
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
                
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

    @staticmethod
    def flatten_perks_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for Perks from the JSON

        Args:
           response_data (str): API Json response string

        Returns:
            json_str
        """
        if CMSUtil.validate_json_data(response_data):
            # Create seperate df for item array
            log.info('Creating Perks dataframe for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['items'])
            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt', 'sys.id', 'fields.name', 
            'fields.headline', 'fields.description', 'fields.featuredHeadline', 'fields.featuredDescription', 'fields.digitalExperienceList', 'fields.newForXDays',
            'fields.startDate', 'fields.endDate', 'fields.ctaText', 'fields.url', 'fields.legalDisclaimer']]

            # Create seperate df for asset array
            output_df = pd.DataFrame()

            # Loop over items df and find corresponding media value(description) in asset df based on id
            log.info('Flattening response JSON with required columns')
            for index, row in req_items_df.iterrows():
                output_df = output_df.append(
                    {
                        'NAME': row['fields.name'], 
                        'DESCRIPTION': row['fields.description'],
                        'HEADLINE': row['fields.headline'], 
                        'FEATURED_HEADLINE': row['fields.featuredHeadline'],
                        'FEATURED_DESCRIPTION': row['fields.featuredDescription'], 
                        'LEGAL_DISCLAIMER': row['fields.legalDisclaimer'],
                        'CTA_TEXT': row['fields.ctaText'],
                        'URL': row['fields.url'],
                        'START_DATE': row['fields.startDate'],
                        'END_DATE': row['fields.endDate'],
                        'DIGITAL_EXPERIENCE_LIST': row['fields.digitalExperienceList'],
                        'NEW_FOR_X_DAYS': row['fields.newForXDays'],
                        'ID': row['sys.id'],
                        'CREATED_AT': datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                        'UPDATED_AT': datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3]
                    },
                ignore_index=True)
 
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
            
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

                

    def flatten_livestream_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for Livestream from the JSON

        Args:
           response_data (str): API Json response string

        Returns:
            json_str
        """
        if CMSUtil.validate_json_data(response_data):
            # Create seperate df for item array
            log.info('Creating livestream dataframe for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['items'])
            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt', 'sys.id', 'fields.title', 'fields.date', 'fields.contentType',
                                'fields.sourceType', 'fields.source']]

            output_df = pd.DataFrame()                    
            log.info('Flattening response JSON with required columns')

            for index, row in req_items_df.iterrows():
                output_df = output_df.append({'ID': row['sys.id'], 
                                                "CREATEDAT": datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                    "UPDATEDAT": datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                    'CONTENT_TYPE': row['fields.contentType'],
                                                    'DATE': row['fields.date'], 
                                                    'SOURCE': row['fields.source'],
                                                    'SOURCE_TYPE': row['fields.sourceType'], 
                                                    'TITLE': row['fields.title']},
                                                    ignore_index=True)

                    
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
                    
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

    @staticmethod
    def flatten_promotion_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for promotion from the JSON

        Args:
           response_data (str): API Json response string

        Returns:
            json_str
        """
        if CMSUtil.validate_json_data(response_data):
            # Create seperate df for item array
            log.info('Creating promotion dataframe for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['items'])
            req_items_df = items_df[['sys.createdAt', 'sys.updatedAt', 'sys.id', 'fields.endDate', 'fields.maxMonthsFree', 'fields.name',
                                'fields.startDate' ]]

            output_df = pd.DataFrame()                    
            log.info('Flattening response JSON with required columns')

            for index, row in req_items_df.iterrows():
                output_df = output_df.append({'ID': row['sys.id'], 
                                                "CREATEDAT": datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                    "UPDATEDAT": datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                    'END_DATE': row['fields.endDate'],
                                                    'MAX_MONTHS_FREE': row['fields.maxMonthsFree'], 
                                                    'NAME': row['fields.name'],
                                                    'START_DATE': row['fields.startDate']},
                                                    ignore_index=True)

                    
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
                    
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False

    @staticmethod
    def flatten_cms_codes_json(response_data):
        """
        Flattens the nested JSON and extracts required columns for cms_codes from the JSON

        Args:
           response_data (str): API Json response string

        Returns:
            json_str
        """
        if CMSUtil.validate_json_data(response_data):
            # Create seperate df for item array
            log.info('Creating cms_codes dataframe for items array')
            items_df = pd.json_normalize(json.loads(response_data), record_path=['includes', 'Entry'])
            req_items_df = items_df[['sys.id', 'sys.createdAt', 'sys.updatedAt', 'sys.type', 'fields.key', 'fields.value']]

            output_df = pd.DataFrame()                    
            log.info('Flattening response JSON with required columns')

            for index, row in req_items_df.iterrows():
                output_df = output_df.append({'ID': row['sys.id'], 
                                                "CREATED_AT": datetime.strftime(datetime.strptime(row['sys.createdAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                "UPDATED_AT": datetime.strftime(datetime.strptime(row['sys.updatedAt'], '%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%d %H:%M:%S.%f')[:-3],
                                                    'CODE_TYPE': row['sys.type'],
                                                    'CODE': row['fields.key'], 
                                                    'CODE_DESCRIPTION': row['fields.value']},
                                                    ignore_index=True)

                    
            json_str = output_df.to_json(orient='records')   
            json_data = json.loads(json_str)
                    
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            return output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False