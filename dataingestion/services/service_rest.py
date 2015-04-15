#!/usr/bin/env python
#
# Copyright (c) 2013 Liu, Yonggang <myidpt@gmail.com>, University of
# Florida
#
# This software may be used and distributed according to the terms of the
# MIT license: http://www.opensource.org/licenses/mit-license.php

import cherrypy, json, logging, ast
# from dataingestion.services.ingestion_manager import IngestServiceException
from cherrypy import HTTPError
from cherrypy._cpcompat import ntob
from dataingestion.services import user_config, api_client
# import (constants, ingestion_service, csv_generator,
#                                     ingestion_manager, api_client, model,
#                                     result_generator, user_config)

logger = logging.getLogger('iDigBioSvc.service_rest')


class JsonHTTPError(HTTPError):
  def set_response(self):
    cherrypy.response.status = self.status
    cherrypy.response.headers['Content-Type'] = "text/html;charset=utf-8"
    cherrypy.response.headers.pop('Content-Length', None)
    cherrypy.response.body = ntob(self._message)


class Authentication(object):
  exposed = True

  def GET(self):
    """
    Authenticate the user account and return the authentication result.
    """
    logger.debug("Authentication GET.")
    try:
      accountuuid = user_config.get_user_config('accountuuid')
      apikey = user_config.get_user_config('apikey')
    except AttributeError:
      return json.dumps(False)

    try:
      ret = api_client.authenticate(accountuuid, apikey)
      return json.dumps(ret)
    except ClientException as ex:
      cherrypy.log.error(str(ex), __name__)
      error = "Error: " + str(ex)
      print error
      logger.error(error)
      raise JsonHTTPError(503, str(ex))

  def POST(self, accountuuid, apikey):
    """
    Post an authentication information pair <user, password>.
    Raises:
      JsonHTTPError 503: if the service is unavilable.
      JsonHTTPError 409: if the UUID/APIKey combination is incorrect.
    """
    logger.debug("Authentication POST: {0}, {1}".format(accountuuid, apikey))
    try:
      ret = api_client.authenticate(accountuuid, apikey)
    except ClientException as ex:
      cherrypy.log.error(str(ex), __name__)
      error = "Error: " + str(ex)
      print error
      logger.error(error)
      raise JsonHTTPError(503, 'iDigBio Service Currently Unavailable.')

    if ret:
      # Set the attributes.
      user_config.set_user_config('accountuuid', accountuuid)
      user_config.set_user_config('apikey', apikey)
    else:
      error = "Authentication combination incorrect."
      print error
      logger.error(error)
      raise JsonHTTPError(409, error)


class CsvIngestionService(object):
  exposed = True

  def GET(self):
    logger.debug("CsvIngestionService GET.")
    return '<html><body>CSV ingestion Service is running.</body></html>'

  def POST(self, values=None):
    """
    Ingest csv data.
    """
    logger.debug("CsvIngestionService POST.")
    if values is None:
      return self._resume()
    else:
      dic = ast.literal_eval(values) # Parse the string to dictionary.
      return self._upload(dic)

  def _upload(self, values):
    try:
      ingestion_service.start_upload(values)
    except ValueError as ex:
      error = "Error: " + str(ex)
      print error
      logger.error(error)
      raise JsonHTTPError(409, str(ex))

  def _resume(self):
    try:
      ingestion_service.start_upload()
    except ValueError as ex:
      error = "Error: " + str(ex)
      print error
      logger.error(error)
      raise JsonHTTPError(409, str(ex)) 


class DataIngestionService(object):
  """
  The root RESTful web service exposed through CherryPy at /services
  """
  exposed = True

  def __init__(self):
    """
    Each self.{attr} manages the request to URL path /services/{attr}.
    """
    self.auth = Authentication()
    self.ingest = CsvIngestionService()
