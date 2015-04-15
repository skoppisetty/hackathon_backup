from celery import Celery
import threading, json, appdirs
from os.path import join, exists

from dataingestion.services import user_config, constants, model

app = Celery('tasks',backend='redis://localhost:6379/', broker='redis://localhost:6379/')
app.config_from_object('celeryconfig')

import redislite, pickle, csv
red_con = redislite.Redis('/tmp/redis.db')

APP_NAME = 'iDigBio Data Ingestion Tool'
APP_AUTHOR = 'iDigBio'
USER_CONFIG_FILENAME = 'user.conf'
data_folder = appdirs.user_data_dir(APP_NAME, APP_AUTHOR)
user_config_path = join(data_folder, USER_CONFIG_FILENAME)
user_config.setup(user_config_path)
db_file = join(data_folder, APP_AUTHOR + ".ingest.db")
model.setup(db_file)

@app.task
def _upload_task(task_id):
	task = json.loads(red_con.get(task_id))
	CSVfilePath = task['CSVfilePath']
	ongoing_upload_task = pickle.loads(task['status'])
	iDigbioProvidedByGUID = user_config.get_user_config(
	user_config.IDIGBIOPROVIDEDBYGUID)
	RightsLicense = task[user_config.RIGHTS_LICENSE]
	license_set = constants.IMAGE_LICENSES[RightsLicense]
	RightsLicenseStatementUrl = license_set[2]
	RightsLicenseLogoUrl = license_set[3]

	# Insert into the database.
	batch = model.add_batch(
	CSVfilePath, iDigbioProvidedByGUID, RightsLicense,
	RightsLicenseStatementUrl, RightsLicenseLogoUrl)

	model.commit()
	ongoing_upload_task.batch = batch
	batch_id = str(batch.id)
	print batch_id

   with open(CSVfilePath, 'rb') as csvfile:
      csv.register_dialect('mydialect', delimiter=',', quotechar='"',
                           skipinitialspace=True)
      reader = csv.reader(csvfile, 'mydialect')
      headerline = None
      recordCount = 0
      for row in reader: # For each line do the work.
         if not headerline:
				batch.ErrorCode = "CSV File Format Error."
				headerline = row
				batch.ErrorCode = ""
				continue

       	# Validity test for each line in CSV file  
         if len(row) != len(headerline):
            logger.debug("Input CSV File weird. At least one row has different"
              + " number of columns")
            raise InputCSVException("Input CSV File weird. At least one row has"
              + " different number of columns")

         for col in row: 
            if "\"" in col:
            	logger.debug("One of CSV field contains \"(Double Quatation)")
            	raise InputCSVException(
                "One of CSV field contains Double Quatation Mark(\")") 

        	# Get the image record
         image_record = model.add_image(batch, row, headerline)

         if image_record is None:
         	 # Skip this one because it's already uploaded.
         	 # Increment skips count and return.
          	fn = partial(ongoing_upload_task.increment, 'skips')
          	ongoing_upload_task.postprocess_queue.put(fn)
        	else:
          	object_queue.put(image_record)

        	recordCount = recordCount + 1
      batch.RecordCount = recordCount
      model.commit()
    logger.debug('Put all image records into db done.')

    for thread in object_threads:
      thread.start()
    logger.debug(
        '{0} upload worker threads started.'.format(worker_thread_count))

    # Wait until all images are executed.
    #while not object_queue.empty():
    #  sleep(0.01)
    while (not ongoing_upload_task.is_finished()):
      if fatal_server_error: 
        raise ServerException  
      sleep(1)
    
    for thread in object_threads:
      thread.abort = True
      while thread.isAlive():
        thread.join(0.01)

    batch.FailCount = ongoing_upload_task.get_fails()
    batch.SkipCount = ongoing_upload_task.get_skips()

    was_error = _put_errors_from_threads(object_threads)
    if not was_error:
      logger.info("Image upload finishes with no error")
    else:
      logger.error("Image upload finishes with errors.")

@app.task(bind=True)
def _upload_task_error_handler(self, uuid):
    result = self.app.AsyncResult(uuid)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
          uuid, result.result, result.traceback))

@app.task(bind=True)
def _upload_images_error_handler(self, uuid):
    result = self.app.AsyncResult(uuid)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
          uuid, result.result, result.traceback))