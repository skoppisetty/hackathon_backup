import redislite, pickle, csv, appdirs
from os.path import join, exists
from dataingestion.services import user_config, constants
from dataingestion.services.batch import BatchUploadTask
red_con = redislite.Redis('/tmp/redis.db')
from dataingestion.services.celery_tasks import _upload_task,_upload_task_error_handler

import uuid, json
APP_NAME = 'iDigBio Data Ingestion Tool'
APP_AUTHOR = 'iDigBio'
USER_CONFIG_FILENAME = 'user.conf'
data_folder = appdirs.user_data_dir(APP_NAME, APP_AUTHOR)
user_config_path = join(data_folder, USER_CONFIG_FILENAME)
user_config.setup(user_config_path)
iDigbioProvidedByGUID = user_config.get_user_config(
	user_config.IDIGBIOPROVIDEDBYGUID)
print iDigbioProvidedByGUID

values = {'RightsLicense': 'CC0', 'CSVfilePath': '/home/suresh/Downloads/Plants_1/media_records.csv'}
task_id = uuid.uuid4()
print task_id
val = BatchUploadTask()
val.set_id(task_id)
val.set_status(BatchUploadTask.STATUS_WAITING)
values['status'] = pickle.dumps(val)
print json.dumps(values)
red_con.set(task_id,json.dumps(values))
_upload_task.apply_async((task_id,),link_error=_upload_task_error_handler.s())
# print pickle.loads(pickle.dumps(val))._id