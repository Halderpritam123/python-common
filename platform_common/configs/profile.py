from ..storage.mongo import MongoDBConnector
from ..config_loader import get_config
from ..dataclasses import ProfilingResults, UserDetails
from typing import List, Dict
def get_profile_information(
    table_info: ProfilingResults, user_details: UserDetails
) -> Dict[str, any]:
    mongo_client = MongoDBConnector.get_instance()
    db = mongo_client[get_config("DB_NAME_DATA_QUALITY")]
    collection = db[get_config("COL_NAME_DATA_QUALITY_PROFILE_RESULTS")]
    matching_filter = {
        "connectorId": table_info.connector_id,
        "databaseName": table_info.database_name,
        "tableName": table_info.table_name,
        "orgId": user_details.org_id,
        "projectId": user_details.project_id,
    }
    if table_info.length_histogram:
        matching_filter["options.lengthHistogram"] = True
    pipeline = [
        {"$match": matching_filter},
        {
            "$sort": {
                "_id": -1,
            }
        },
        {"$limit": 1},
        {
            "$project": {
                "_id": 0,
            }
        },
    ]
    
    profiling_results = list(collection.aggregate(pipeline))
    return profiling_results[0]