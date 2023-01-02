import json
import logging
import pandas as pd
from datetime import datetime

log = logging.getLogger(__name__)

class FBUtil:
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
    def flatten_fb_ad_insight_json(response_data):
        """
        Flattens the nested JSON and extracts required columns from the JSON

        Args:
            response_data (str): API Json response string

        Returns:
            str_buffer
        """
        if FBUtil.validate_json_data(response_data):
            log.info('Creating dataframe for data array')
            df = pd.json_normalize(json.loads(response_data), record_path=['data'])
            df = df[df['insights.data'].notnull()]

            log.info('Flattening insights array')
            df_explode = df.explode('insights.data')
            insights_df = pd.concat([df_explode.drop(['insights.data'], axis=1), df_explode['insights.data'].apply(pd.Series)], axis=1)

            log.info('Flattening actions array')
            insights_df_explode = insights_df.explode('actions')
            adsets_df = pd.concat([insights_df_explode.drop(['actions'], axis=1), insights_df_explode['actions'].apply(pd.Series)], axis=1)

            fb_spends_actual_df = pd.DataFrame()
            fb_joins_df = pd.DataFrame()

            log.info('Creating flat JSON with required columns')
            for index, row in adsets_df.iterrows():

                fb_spends_actual_df = fb_spends_actual_df.append({
                    'CAMPAIGN_ID': row['campaign.id'],
                    'CAMPAIGN_NAME': row['campaign.name'],
                    'LIFETIME_BUDGET': (row['campaign.lifetime_budget'] if pd.notnull(row['campaign.lifetime_budget']) else 0.0),
                    'ADSET_ID': row['id'],
                    'ADSET_NAME': row['name'],
                    'AMOUNT_SPENT': (row['spend'] if pd.notnull(row['spend']) else None),
                    'REACH': (row['reach'] if pd.notnull(row['reach']) else 0),
                    'IMPRESSIONS': (row['impressions'] if pd.notnull(row['impressions']) else 0),
                    'DATETIME': row['date_start'],
                    'CLICKS': row['clicks']
                }, ignore_index=True)

                fb_joins_df = fb_joins_df.append({
                    'CAMPAIGN_ID': row['campaign.id'],
                    'CAMPAIGN_NAME': row['campaign.name'],
                    'ADSET_ID': row['id'],
                    'ADSET_NAME': row['name'],
                    'JOIN': (row['value'] if pd.notnull(row['value']) else None),
                    'DATETIME': row['date_start']
                }, ignore_index=True)

            fb_spends_json_str = fb_spends_actual_df.to_json(orient='records')   
            fb_spends_json_data = json.loads(fb_spends_json_str)

            log.info(f'Total records processed for fb_spends_actual = {len(fb_spends_json_data)}')
            
            fb_spends_output_json_str = None

            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in fb_spends_json_data:
                if fb_spends_output_json_str is not None:
                    fb_spends_output_json_str = fb_spends_output_json_str + '\n' + json.dumps(elem)
                else:
                    fb_spends_output_json_str = json.dumps(elem)
            
            fb_joins_json_str = fb_joins_df.to_json(orient='records')   
            fb_joins_json_data = json.loads(fb_joins_json_str)

            log.info(f'Total records processed for fb_joins_actual = {len(fb_joins_json_data)}')
            
            fb_joins_output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in fb_joins_json_data:
                if fb_joins_output_json_str is not None:
                    fb_joins_output_json_str = fb_joins_output_json_str + '\n' + json.dumps(elem)
                else:
                    fb_joins_output_json_str = json.dumps(elem)

            return fb_spends_output_json_str, fb_joins_output_json_str
        else:
            # TODO Confirm if error records needs to be saved in S3.
            log.error('Input JSON response is not a valid JSON')
            return False
