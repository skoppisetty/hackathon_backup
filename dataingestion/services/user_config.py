import os
import ConfigParser

#Fields in the configuration file.
CONFIG_SECTION = 'iDigBio'
DISABLE_CHECK = "devmode_disable_startup_service_check"

config = None

def setup(config_file):
  global config
  config = UserConfig(config_file)
  config.reload()

def get_user_config(name):
  """
  Returns:
    The attribute value for the name.
  Except:
    AttributeError: If the name does not exist.
  TODO: check whether the name is in the allowed list.
  """

  global config
  return getattr(config, name)

def set_user_config(name, value):
  global config
  setattr(config, name, value)

def rm_user_config():
  global config
  if os.path.exists(config.config_file):
    os.remove(config.config_file)
  config = UserConfig(config.config_file)

class UserConfig(object):
  """
  The class with the user config values.
  """
  def __init__(self, config_file):
    self.config = ConfigParser.ConfigParser()
    self.config.add_section(CONFIG_SECTION)
    self.config_file = config_file

  def reload(self):
    if os.path.exists(self.config_file):
      self.config.read(self.config_file)

    if not self.config.has_section(CONFIG_SECTION):
      self.config.add_section(CONFIG_SECTION)

  def __getattr__(self, name):
    self.reload()
    try:
      return self.config.get(CONFIG_SECTION, name)
    except:
      raise AttributeError

  def __setattr__(self, name, value):
    if name == 'config' or name == 'config_file':
      self.__dict__[name] = value
      return

    self.config.set(CONFIG_SECTION, name, value)
    with open(self.config_file, 'wb') as f:
      self.config.write(f)

  # Check if the authentication check is disabled.
  def check_disabled(self):
    self.reload()

    if self.config.has_option(CONFIG_SECTION, DISABLE_CHECK):
      return self.config.get(CONFIG_SECTION, DISABLE_CHECK)
    else:
      return False
